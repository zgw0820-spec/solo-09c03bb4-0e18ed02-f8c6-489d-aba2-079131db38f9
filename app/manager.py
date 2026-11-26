"""Core domain logic: sample artifact publishing + dual-volume backup rounds.

Concurrency model
-----------------
Everything mutating in this manager runs on the single asyncio event loop of
the web process.  ``start_backup`` / ``accept_artifact`` are synchronous
critical sections from the loop's point of view, which makes the
"pause acceptance first", "one round at a time" and "settle exactly once"
invariants straightforward.

Round isolation
---------------
Every round owns its result object.  Snapshot HTTP calls are tasks tied to
that round.  When the stop-write window settles a round (success / failure /
timeout), late tasks are *not* awaited by the state machine; instead a
detached watcher archives their response as a "late report" on the OLD round.
Late reports never mutate a settled round and never touch the acceptance
flag, so they cannot interfere with the next round.

Queued publish (reservations)
-----------------------------
While a round has acceptance paused, publish orders may be queued instead of
rejected.  Queued orders are memory-only (no disk writes), can be withdrawn,
and auto-expire 10s after queuing.  When a round settles, acceptance is
resumed in one synchronous step: expire overdue orders -> accept still-valid
orders in queue order (they join ``pending``) -> only then allow new orders.
A round started right afterwards therefore drains those accepted orders to
BOTH volumes before it captures.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import time
import uuid
import zipfile
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import httpx

from .config import Settings

logger = logging.getLogger("artifact-lab.manager")

VOLUME_LABELS = ("index", "blobs")
TERMINAL_ROUND_STATES = ("success", "failed")

RoundState = Literal["draining", "snapshotting", "success", "failed"]


class RoundAlreadyActive(Exception):
    def __init__(self, active_round_id: str):
        self.active_round_id = active_round_id
        super().__init__(f"another backup round is running: {active_round_id}")


class AcceptancePaused(Exception):
    def __init__(self, active_round_id: str | None):
        self.active_round_id = active_round_id
        super().__init__("artifact acceptance is paused for a backup round")


class ReservationNotFound(Exception):
    pass


class NotPaused(Exception):
    pass


class ReservationNotWithdrawable(Exception):
    def __init__(self, reservation_id: str, state: str):
        self.reservation_id = reservation_id
        self.state = state
        super().__init__(f"reservation {reservation_id} is {state}, not withdrawable")


class AgentConfigError(Exception):
    pass


# --------------------------------------------------------------------------- #
# Durable filesystem helpers
# --------------------------------------------------------------------------- #
def durable_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes to a temp file, fsync, atomically rename, fsync dir."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        dfd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #
@dataclass
class Artifact:
    id: str
    seq: int
    name: str
    interval_seconds: float
    state: str = "accepted"  # accepted|blob_writing|interval_wait|index_writing|complete|failed
    created_at_unix: float = field(default_factory=time.time)
    blob_at_unix: float | None = None
    index_at_unix: float | None = None
    blob_file: str | None = None
    index_file: str | None = None
    size_bytes: int = 0
    sha256: str | None = None
    error: str | None = None
    reservation_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {**self.__dict__}


@dataclass
class Reservation:
    """A queued publish order.  Exists only in web-process memory."""

    id: str
    seq: int
    name: str
    interval_seconds: float
    state: str = "queued"  # queued|accepted|withdrawn|expired
    created_at_unix: float = field(default_factory=time.time)
    expires_at_unix: float = 0.0
    finalized_at_unix: float | None = None
    accepted_at_unix: float | None = None
    artifact_id: str | None = None
    note: str | None = None

    def to_dict(self, *, now: float | None = None) -> dict[str, Any]:
        now = now if now is not None else time.time()
        remaining = None
        if self.state == "queued":
            remaining = max(0.0, self.expires_at_unix - now)
        return {
            **self.__dict__,
            "remaining_seconds": remaining,
            "withdrawable": self.state == "queued",
        }


@dataclass
class Round:
    id: str
    seq: int
    state: RoundState = "draining"
    created_at_unix: float = field(default_factory=time.time)
    started_at_unix: float = field(default_factory=time.time)
    finished_at_unix: float | None = None
    drain_started_at_unix: float = field(default_factory=time.time)
    drain_finished_at_unix: float | None = None
    snapshot_started_at_unix: float | None = None
    window_started_unix: float | None = None
    window_deadline_unix: float | None = None
    window_seconds: float | None = None
    inflight_at_pause: list[str] = field(default_factory=list)
    reports: dict[str, dict[str, Any]] = field(default_factory=dict)
    late_reports: list[dict[str, Any]] = field(default_factory=list)
    failure_code: str | None = None
    failure_message: str | None = None
    bundle: dict[str, Any] = field(
        default_factory=lambda: {"state": "pending", "filename": None, "size_bytes": None, "error": None}
    )

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_ROUND_STATES

    def to_dict(self, *, now: float | None = None) -> dict[str, Any]:
        now = now if now is not None else time.time()
        window_remaining = None
        if self.state == "snapshotting" and self.window_deadline_unix is not None:
            window_remaining = max(0.0, self.window_deadline_unix - now)
        drain_seconds = None
        if self.drain_finished_at_unix is not None:
            drain_seconds = self.drain_finished_at_unix - self.drain_started_at_unix
        elif self.state == "draining":
            drain_seconds = now - self.drain_started_at_unix
        total_seconds = None
        if self.finished_at_unix is not None:
            total_seconds = self.finished_at_unix - self.started_at_unix
        return {
            "id": self.id,
            "seq": self.seq,
            "state": self.state,
            "terminal": self.terminal,
            "created_at_unix": self.created_at_unix,
            "started_at_unix": self.started_at_unix,
            "finished_at_unix": self.finished_at_unix,
            "drain_started_at_unix": self.drain_started_at_unix,
            "drain_finished_at_unix": self.drain_finished_at_unix,
            "drain_seconds": drain_seconds,
            "snapshot_started_at_unix": self.snapshot_started_at_unix,
            "window_started_unix": self.window_started_unix,
            "window_deadline_unix": self.window_deadline_unix,
            "window_seconds": self.window_seconds,
            "window_remaining_seconds": window_remaining,
            "total_seconds": total_seconds,
            "inflight_at_pause": self.inflight_at_pause,
            "reports": self.reports,
            "late_reports": self.late_reports,
            "late_report_count": len(self.late_reports),
            "failure_code": self.failure_code,
            "failure_message": self.failure_message,
            "bundle": self.bundle,
            "download_available": (
                self.state == "success" and self.bundle.get("state") == "ready"
            ),
        }


# --------------------------------------------------------------------------- #
# Snapshot agent HTTP client
# --------------------------------------------------------------------------- #
class AgentClient:
    def __init__(
        self,
        label: str,
        base_url: str,
        timeout: float,
        transport: httpx.BaseTransport | httpx.AsyncBaseTransport | None = None,
    ):
        self.label = label
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(timeout, connect=min(3.0, timeout)),
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def snapshot(self, round_id: str) -> dict[str, Any]:
        """Call the snapshot end.  Never raises for an agent-side failure;
        transport problems are converted into an ok=False report."""
        t0 = time.monotonic()
        try:
            resp = await self._client.post("/snapshot", json={"round_id": round_id})
        except httpx.HTTPError as exc:
            return {
                "ok": False,
                "round_id": round_id,
                "error_code": "transport_error",
                "error": f"{type(exc).__name__}: {exc}",
                "client_elapsed_seconds": time.monotonic() - t0,
            }
        elapsed = time.monotonic() - t0
        try:
            data = resp.json()
        except ValueError:
            data = {}
        if resp.status_code >= 400 or data.get("status") not in (None, "succeeded"):
            return {
                "ok": False,
                "round_id": round_id,
                "status_code": resp.status_code,
                "error_code": data.get("error_code", "agent_reported_failure"),
                "error": data.get("error") or f"HTTP {resp.status_code}",
                "agent_delay_seconds": data.get("delay_seconds"),
                "client_elapsed_seconds": elapsed,
            }
        return {
            "ok": True,
            "round_id": round_id,
            "status_code": resp.status_code,
            "volume": data.get("volume", self.label),
            "files": data.get("files"),
            "bytes": data.get("bytes"),
            "copied_bytes": data.get("copied_bytes"),
            "delay_seconds": data.get("delay_seconds"),
            "copy_seconds": data.get("copy_seconds"),
            "elapsed_seconds": data.get("elapsed_seconds", elapsed),
            "snapshot_path": data.get("snapshot_path"),
            "client_elapsed_seconds": elapsed,
        }

    async def get_config(self) -> dict[str, Any]:
        resp = await self._client.get("/admin/config")
        resp.raise_for_status()
        return resp.json()

    async def put_config(self, patch: dict[str, Any]) -> dict[str, Any]:
        resp = await self._client.put("/admin/config", json=patch)
        resp.raise_for_status()
        return resp.json()


# --------------------------------------------------------------------------- #
# Manager
# --------------------------------------------------------------------------- #
class Manager:
    def __init__(
        self,
        settings: Settings,
        transports: dict[str, httpx.AsyncBaseTransport] | None = None,
    ):
        self.settings = settings
        self.write_interval: float = settings.default_write_interval

        timeout = settings.agent_http_timeout_seconds
        self.agents: dict[str, AgentClient] = {
            label: AgentClient(
                label,
                (
                    settings.index_agent_url
                    if label == "index"
                    else settings.blob_agent_url
                ),
                timeout,
                transport=(transports or {}).get(label),
            )
            for label in VOLUME_LABELS
        }

        self.accepting: bool = True
        self.active_round_id: str | None = None

        self.artifacts: dict[str, Artifact] = {}
        self.artifact_order: deque[str] = deque()
        self.pending: set[str] = set()
        self.drain_cond = asyncio.Condition()

        # Queued publish orders live only in this web process.  They never
        # touch disk while queued and never survive a restart (by design).
        self.reservations: dict[str, Reservation] = {}
        self.reservation_order: deque[str] = deque()
        self._reservation_tasks: dict[str, asyncio.Task] = {}

        self.rounds: dict[str, Round] = {}
        self.round_order: deque[str] = deque()

        self.events: deque[dict[str, Any]] = deque(maxlen=settings.history_events)
        self._artifact_seq = 0
        self._reservation_seq = 0
        self._round_seq = 0
        self._bg_tasks: set[asyncio.Task] = set()

    # ----------------------------- events ---------------------------------- #
    def add_event(self, level: str, message: str, **extra: Any) -> None:
        evt = {
            "seq": len(self.events) + 1,
            "at_unix": time.time(),
            "level": level,
            "message": message,
            **extra,
        }
        self.events.append(evt)
        getattr(logger, "info" if level == "info" else level, logger.info)(message)

    # ---------------------------- lifecycle --------------------------------- #
    async def aclose(self) -> None:
        tasks = list(self._bg_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for client in self.agents.values():
            await client.aclose()

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    # ---------------------------- reservations ------------------------------ #
    def queue_publish(
        self, name: str | None = None, interval_seconds: float | None = None
    ) -> Reservation:
        """Queue a publish order instead of rejecting it while paused.

        Synchronous critical section: no await between the liveness/acceptance
        checks and registration, so an expiry sweep or a new round cannot
        interleave.  Nothing is written to either volume here.
        """
        if self.accepting:
            # Nothing to queue for: the caller should publish directly.
            raise NotPaused()
        self._reservation_seq += 1
        seq = self._reservation_seq
        rid = f"rsv_{seq:05d}_{uuid.uuid4().hex[:6]}"
        if interval_seconds is None:
            interval = self.write_interval
        else:
            interval = max(0.0, min(float(interval_seconds), 60.0))
        now = time.time()
        rsv = Reservation(
            id=rid,
            seq=seq,
            name=(name or f"样例制品 {seq}").strip()[:120],
            interval_seconds=interval,
            created_at_unix=now,
            expires_at_unix=now + self.settings.reservation_ttl_seconds,
        )
        self.reservations[rid] = rsv
        self.reservation_order.append(rid)
        self._trim_reservations()
        # Arm the TTL.  Expiry is enforced lazily too (see _expire_reservations),
        # so a missed timer can never publish an overdue order.
        self._reservation_tasks[rid] = self._spawn(self._expire_reservation(rsv))
        self.add_event(
            "warning",
            f"排队发布 {rid}（{rsv.name}）：已暂停接单，先排队不写盘；"
            f"{self.settings.reservation_ttl_seconds:.0f}s 内未被接单将自动作废",
            reservation_id=rid,
        )
        return rsv

    async def _expire_reservation(self, rsv: Reservation) -> None:
        try:
            # Sleep to the wall-clock deadline, tolerating early wakeups.
            while rsv.state == "queued":
                remaining = rsv.expires_at_unix - time.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(remaining)
            if rsv.state == "queued" and time.time() >= rsv.expires_at_unix:
                self._finalize_reservation(
                    rsv,
                    "expired",
                    f"预约 {self.settings.reservation_ttl_seconds:.0f}s 内未被接单，自动作废，不再发布",
                )
            # Sweep any siblings that are also overdue (lazy safety net).
            self._expire_reservations()
        finally:
            # Identity-guarded: only drop our own registration.
            if self._reservation_tasks.get(rsv.id) is asyncio.current_task():
                self._reservation_tasks.pop(rsv.id, None)

    def _expire_reservations(self) -> list[Reservation]:
        """Mark every queued reservation past its deadline as expired."""
        now = time.time()
        expired: list[Reservation] = []
        for rsv in self.reservations.values():
            if rsv.state == "queued" and now >= rsv.expires_at_unix:
                self._finalize_reservation(rsv, "expired", "预约到期仍未接单，自动作废，不再发布")
                expired.append(rsv)
        return expired

    def _cancel_reservation_timer(self, rsv: Reservation) -> None:
        task = self._reservation_tasks.pop(rsv.id, None)
        # Never self-cancel: expiry finalizes its own reservation from within
        # the timer task, which is already about to return.
        try:
            current = asyncio.current_task()
        except RuntimeError:  # no running loop (plain sync caller)
            current = None
        if task is not None and not task.done() and task is not current:
            task.cancel()

    def _finalize_reservation(
        self, rsv: Reservation, state: str, note: str | None = None
    ) -> None:
        if rsv.state != "queued":
            return
        rsv.state = state
        rsv.finalized_at_unix = time.time()
        rsv.note = note
        self._cancel_reservation_timer(rsv)
        if state == "expired":
            self.add_event(
                "error",
                f"排队发布 {rsv.id}（{rsv.name}）：{note}",
                reservation_id=rsv.id,
            )

    def withdraw_reservation(self, reservation_id: str) -> Reservation:
        rsv = self.reservations.get(reservation_id)
        if rsv is None:
            raise ReservationNotFound(reservation_id)
        # Lazy sweep so an overdue reservation reports "expired", not "queued".
        self._expire_reservations()
        if rsv.state != "queued":
            raise ReservationNotWithdrawable(reservation_id, rsv.state)
        self._finalize_reservation(rsv, "withdrawn", "已由值班员撤回，不再发布")
        self.add_event(
            "warning",
            f"排队发布 {rsv.id}（{rsv.name}）：已撤回，不再发布",
            reservation_id=rsv.id,
        )
        return rsv

    def _trim_reservations(self) -> None:
        limit = self.settings.history_reservations
        while len(self.reservation_order) > limit:
            old_id = self.reservation_order.popleft()
            old = self.reservations.get(old_id)
            if old is not None and old.state == "queued":
                self.reservation_order.appendleft(old_id)
                break
            self.reservations.pop(old_id, None)

    def _accept_due_reservations(self) -> list[Artifact]:
        """Accept still-valid queued reservations in queue order.

        Must run synchronously with ``accepting`` flipped to True and exactly
        before new orders are allowed in, so accepted reservations keep their
        ordering and are all in ``pending`` before the next round pauses.
        """
        self._expire_reservations()
        accepted: list[Artifact] = []
        for rid in list(self.reservation_order):
            rsv = self.reservations.get(rid)
            if rsv is None or rsv.state != "queued":
                continue
            if time.time() >= rsv.expires_at_unix:
                continue  # swept above, defensive
            try:
                art = self._accept_artifact_internal(
                    rsv.name, rsv.interval_seconds, reservation_id=rsv.id
                )
            except Exception as exc:  # never leave acceptance stuck on one bad order
                logger.exception("reservation accept failed: %s", rsv.id)
                self._finalize_reservation(
                    rsv, "expired", f"接单失败，按作废处理：{type(exc).__name__}: {exc}"
                )
                continue
            rsv.state = "accepted"
            rsv.accepted_at_unix = time.time()
            rsv.finalized_at_unix = rsv.accepted_at_unix
            rsv.artifact_id = art.id
            rsv.note = "恢复接单时按预约顺序接单，立即发布"
            self._cancel_reservation_timer(rsv)
            accepted.append(art)
            self.add_event(
                "success",
                f"排队发布 {rsv.id}（{rsv.name}）：恢复接单，按预约顺序接单为制品 {art.id}",
                reservation_id=rsv.id,
            )
        return accepted

    # --------------------------- artifacts --------------------------------- #
    def accept_artifact(
        self, name: str | None = None, interval_seconds: float | None = None
    ) -> Artifact:
        """Synchronous critical section: check acceptance + register pending.

        No await may happen between the accepting check and insertion into
        self.pending, otherwise a backup round could observe a false drain.
        """
        if not self.accepting:
            raise AcceptancePaused(self.active_round_id)
        return self._accept_artifact_internal(name, interval_seconds)

    def _accept_artifact_internal(
        self,
        name: str | None = None,
        interval_seconds: float | None = None,
        *,
        reservation_id: str | None = None,
    ) -> Artifact:
        """Register + start publishing one artifact.  Caller owns the
        acceptance decision (accepting must already be True)."""
        self._artifact_seq += 1
        seq = self._artifact_seq
        artifact_id = f"art_{seq:05d}_{uuid.uuid4().hex[:6]}"
        if interval_seconds is None:
            interval = self.write_interval
        else:
            interval = max(0.0, min(float(interval_seconds), 60.0))
        art = Artifact(
            id=artifact_id,
            seq=seq,
            name=(name or f"样例制品 {seq}").strip()[:120],
            interval_seconds=interval,
            reservation_id=reservation_id,
        )
        self.artifacts[artifact_id] = art
        self.artifact_order.append(artifact_id)
        self.pending.add(artifact_id)
        self._trim_artifacts()
        self._spawn(self._write_artifact(artifact_id))
        self.add_event(
            "info",
            f"已接单 {artifact_id}（{art.name}），两次落盘间隔 {interval:.2f}s"
            + (f"，来源预约 {reservation_id}" if reservation_id else ""),
        )
        return art

    def _trim_artifacts(self) -> None:
        limit = self.settings.history_artifacts
        while len(self.artifact_order) > limit:
            old_id = self.artifact_order.popleft()
            if old_id in self.pending:
                self.artifact_order.appendleft(old_id)
                break
            self.artifacts.pop(old_id, None)

    async def _write_artifact(self, artifact_id: str) -> None:
        art = self.artifacts[artifact_id]
        try:
            # First landing: blob/file volume.
            art.state = "blob_writing"
            payload = self._sample_payload(art)
            art.size_bytes = len(payload)
            art.sha256 = hashlib.sha256(payload).hexdigest()
            blob_name = f"{artifact_id}.bin"
            await asyncio.to_thread(
                durable_write_bytes, self.settings.blob_dir / blob_name, payload
            )
            art.blob_file = blob_name
            art.blob_at_unix = time.time()

            # Adjustable gap between the two landings.
            art.state = "interval_wait"
            await asyncio.sleep(art.interval_seconds)

            # Second landing: index volume (commit marker).
            art.state = "index_writing"
            entry = {
                "id": art.id,
                "seq": art.seq,
                "name": art.name,
                "blob_file": blob_name,
                "size_bytes": art.size_bytes,
                "sha256": art.sha256,
                "interval_seconds": art.interval_seconds,
                "accepted_at_unix": art.created_at_unix,
                "blob_at_unix": art.blob_at_unix,
                "index_at_unix": time.time(),
            }
            index_name = f"{artifact_id}.json"
            await asyncio.to_thread(
                durable_write_bytes,
                self.settings.index_dir / index_name,
                json.dumps(entry, ensure_ascii=False, indent=2).encode("utf-8"),
            )
            art.index_file = index_name
            art.index_at_unix = entry["index_at_unix"]
            art.state = "complete"
            self.add_event("info", f"制品 {artifact_id} 已写齐两卷")
        except asyncio.CancelledError:
            art.state = "failed"
            art.error = "server_shutdown"
            raise
        except Exception as exc:  # disk errors must unblock the round as failure
            logger.exception("artifact write failed: %s", artifact_id)
            art.state = "failed"
            art.error = f"{type(exc).__name__}: {exc}"
            self.add_event("error", f"制品 {artifact_id} 落盘失败：{art.error}")
        finally:
            self.pending.discard(artifact_id)
            async with self.drain_cond:
                self.drain_cond.notify_all()

    @staticmethod
    def _sample_payload(art: Artifact) -> bytes:
        body = (
            f"artifact id   : {art.id}\n"
            f"name          : {art.name}\n"
            f"seq           : {art.seq}\n"
            f"created_at    : {art.created_at_unix:.6f}\n"
            f"payload-nonce : {uuid.uuid4().hex}\n"
        ).encode("utf-8")
        # A little deterministic-ish padding so copies have measurable bytes.
        return body + (b"sample-artifact-block\n" * 64)

    # ---------------------------- settings --------------------------------- #
    def update_settings(self, patch: dict[str, Any]) -> dict[str, Any]:
        if "write_interval_seconds" in patch and patch["write_interval_seconds"] is not None:
            self.write_interval = max(0.0, min(float(patch["write_interval_seconds"]), 60.0))
        return self.settings_dict()

    def settings_dict(self) -> dict[str, Any]:
        return {
            "write_interval_seconds": self.write_interval,
            "snapshot_window_seconds": self.settings.snapshot_window_seconds,
            "max_window_seconds": 3.0,
            "drain_timeout_seconds": self.settings.drain_timeout_seconds,
            "agent_http_timeout_seconds": self.settings.agent_http_timeout_seconds,
            "reservation_ttl_seconds": self.settings.reservation_ttl_seconds,
        }

    # ----------------------------- rounds ---------------------------------- #
    def start_backup(self) -> Round:
        """Pause acceptance and start a round.  One round at a time."""
        if self.active_round_id is not None:
            raise RoundAlreadyActive(self.active_round_id)
        self._round_seq += 1
        round_id = f"rnd_{self._round_seq:04d}_{uuid.uuid4().hex[:6]}"
        rnd = Round(id=round_id, seq=self._round_seq)
        self.rounds[round_id] = rnd
        self.round_order.append(round_id)
        while len(self.round_order) > self.settings.history_rounds:
            old = self.round_order.popleft()
            self.rounds.pop(old, None)

        # --- The essential ordering: pause acceptance BEFORE anything else.
        self.accepting = False
        self.active_round_id = round_id
        # Snapshot the in-flight set at the exact pause instant.  With
        # accepting=False it can only shrink afterwards.
        rnd.inflight_at_pause = sorted(self.pending)
        self.add_event(
            "warning",
            f"轮次 {round_id} 开始：已暂停接单（{len(rnd.inflight_at_pause)} 个已接单在途）",
        )
        self._spawn(self._run_backup(rnd))
        return rnd

    def get_round(self, round_id: str) -> Round | None:
        return self.rounds.get(round_id)

    async def _run_backup(self, rnd: Round) -> None:
        loop = asyncio.get_running_loop()
        try:
            # 1) Wait for every order accepted before the pause to land on
            #    BOTH volumes.  New orders cannot appear (accepting=False).
            rnd.state = "draining"
            self.add_event(
                "info",
                f"轮次 {rnd.id}：等待 {len(rnd.inflight_at_pause)} 个已接单制品写齐两卷后再抓取",
                round_id=rnd.id,
            )
            if self.pending:
                try:
                    async with self.drain_cond:
                        await asyncio.wait_for(
                            self.drain_cond.wait_for(lambda: len(self.pending) == 0),
                            timeout=self.settings.drain_timeout_seconds,
                        )
                except asyncio.TimeoutError:
                    stuck = sorted(self.pending)
                    self._settle_failed(
                        rnd,
                        "drain_timeout",
                        f"等待在途制品写齐两卷超时（{self.settings.drain_timeout_seconds:.0f}s），"
                        f"仍在写：{stuck}",
                    )
                    return

            rnd.drain_finished_at_unix = time.time()
            failed_arts = [
                aid
                for aid in rnd.inflight_at_pause
                if aid in self.artifacts and self.artifacts[aid].state == "failed"
            ]
            if failed_arts:
                self._settle_failed(
                    rnd,
                    "artifact_write_failed",
                    f"以下已接单制品未能写齐两卷：{failed_arts}",
                )
                return

            # 2) Volumes are quiescent now.  Open the bounded stop-write
            #    window (hard cap 3s) and call both snapshot ends in parallel.
            rnd.state = "snapshotting"
            rnd.snapshot_started_at_unix = time.time()
            window = self.settings.snapshot_window_seconds
            rnd.window_seconds = window
            rnd.window_started_unix = time.time()
            rnd.window_deadline_unix = rnd.window_started_unix + window
            deadline_mono = loop.time() + window
            self.add_event(
                "warning",
                f"轮次 {rnd.id}：两卷已无在途写入，停写抓取窗口开启，最长 {window:.1f}s",
                round_id=rnd.id,
            )

            tasks: dict[str, asyncio.Task] = {
                label: self._spawn(self.agents[label].snapshot(rnd.id))
                for label in VOLUME_LABELS
            }
            label_of = {task: label for label, task in tasks.items()}
            pending: set[asyncio.Task] = set(tasks.values())

            while pending:
                remaining = deadline_mono - loop.time()
                if remaining <= 0:
                    break
                done, pending = await asyncio.wait(
                    pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
                )
                failure_now: tuple[str, dict[str, Any]] | None = None
                for task in done:
                    label = label_of[task]
                    report = task.result()  # AgentClient.snapshot never raises
                    report["received_at_unix"] = time.time()
                    rnd.reports[label] = report
                    if report.get("ok"):
                        self.add_event(
                            "success",
                            f"轮次 {rnd.id}：{label} 卷按时回报抓取成功"
                            f"（{report.get('files')} 个文件 / {report.get('bytes')} 字节）",
                            round_id=rnd.id,
                            volume=label,
                        )
                    else:
                        failure_now = (label, report)
                        self.add_event(
                            "error",
                            f"轮次 {rnd.id}：{label} 卷回报失败：{report.get('error')}",
                            round_id=rnd.id,
                            volume=label,
                        )
                if failure_now is not None:
                    label, report = failure_now
                    self._settle_failed(
                        rnd,
                        "agent_failure",
                        f"{label} 卷抓取失败：{report.get('error')}",
                    )
                    self._watch_pending_as_late(rnd, pending, label_of)
                    return

                if all(
                    rnd.reports.get(label, {}).get("ok") for label in VOLUME_LABELS
                ):
                    self._settle_success(rnd)
                    return

            # 3) Window elapsed without both timely successes.
            got = [
                label
                for label in VOLUME_LABELS
                if rnd.reports.get(label, {}).get("ok")
            ]
            missing = [label for label in VOLUME_LABELS if label not in got]
            self._settle_failed(
                rnd,
                "capture_timeout",
                f"停写窗口 {window:.1f}s 到期，未按时收齐两卷成功回报"
                f"（已成功：{got or '无'}；缺失/未决：{missing}）",
            )
            self._watch_pending_as_late(rnd, pending, label_of)
        except asyncio.CancelledError:
            # Shutdown path: leave acceptance flag as-is; nothing to resume.
            raise
        except Exception as exc:  # defensive: never leave acceptance paused
            logger.exception("backup round crashed: %s", rnd.id)
            self._settle_failed(rnd, "internal_error", f"{type(exc).__name__}: {exc}")

    def _watch_pending_as_late(
        self,
        rnd: Round,
        pending: set[asyncio.Task],
        label_of: dict[asyncio.Task, str],
    ) -> None:
        for task in list(pending):
            self._spawn(self._collect_late_report(rnd, label_of[task], task))

    async def _collect_late_report(
        self, rnd: Round, label: str, task: asyncio.Task
    ) -> None:
        try:
            report = await task
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # defensive
            report = {
                "ok": False,
                "error_code": "transport_error",
                "error": f"{type(exc).__name__}: {exc}",
            }
        report["received_at_unix"] = time.time()
        # Append-only audit on the OLD round.  There is deliberately NO call
        # to _settle_* / acceptance mutation here.
        rnd.late_reports.append(
            {"label": label, "at_unix": time.time(), "report": report}
        )
        verdict = "成功" if report.get("ok") else "失败"
        self.add_event(
            "warning",
            f"轮次 {rnd.id}：收到 {label} 卷的迟到{verdict}回报，仅归档，"
            f"不改变本轮「{rnd.state}」结果，也不影响下一轮",
            round_id=rnd.id,
            volume=label,
            late=True,
        )

    # --------------------------- settlement -------------------------------- #
    def _resume_acceptance_if_owner(self, rnd: Round) -> None:
        # Identity guard: an old round must never clear a newer active round.
        if self.active_round_id != rnd.id:
            return
        self.active_round_id = None
        # (1) Finalize anything already past its 10s deadline, (2) accept the
        # still-valid reservations in queue order, (3) only then open the gate
        # to brand-new orders.  All synchronous: a new order or an immediate
        # next round can only happen after every accepted reservation is in
        # ``pending``, so a follow-up round will drain them before capturing.
        accepted = self._accept_due_reservations()
        self.accepting = True
        if accepted:
            self.add_event(
                "info",
                f"轮次 {rnd.id} 结束恢复接单：先按预约顺序接下 {len(accepted)} 个仍有效的预约，"
                f"再接新单",
                round_id=rnd.id,
            )

    def _settle_success(self, rnd: Round) -> None:
        if rnd.terminal:
            return
        rnd.state = "success"
        rnd.finished_at_unix = time.time()
        self.add_event(
            "success",
            f"轮次 {rnd.id}：两卷均在 {rnd.window_seconds:.1f}s 停写窗口内回报成功，"
            f"整组快照可下载；立即恢复接单",
            round_id=rnd.id,
        )
        self._resume_acceptance_if_owner(rnd)
        self._spawn(self._build_bundle(rnd))

    def _settle_failed(self, rnd: Round, code: str, message: str) -> None:
        if rnd.terminal:
            return
        rnd.state = "failed"
        rnd.failure_code = code
        rnd.failure_message = message
        rnd.finished_at_unix = time.time()
        self.add_event(
            "error",
            f"轮次 {rnd.id}：备份失败（{code}）：{message}；本轮不提供下载，立即恢复接单",
            round_id=rnd.id,
        )
        self._resume_acceptance_if_owner(rnd)

    # ----------------------------- bundle ----------------------------------- #
    def bundle_path(self, rnd: Round) -> Path:
        return self.settings.bundle_dir / f"{rnd.id}.zip"

    async def _build_bundle(self, rnd: Round) -> None:
        rnd.bundle["state"] = "building"
        try:
            path = await asyncio.to_thread(self._make_bundle_zip, rnd)
            rnd.bundle.update(
                state="ready",
                filename=path.name,
                size_bytes=path.stat().st_size,
                error=None,
            )
            self.add_event(
                "success",
                f"轮次 {rnd.id}：整组下载包已打好（{path.name}，{rnd.bundle['size_bytes']} 字节）",
                round_id=rnd.id,
            )
        except Exception as exc:
            logger.exception("bundle build failed: %s", rnd.id)
            rnd.bundle.update(state="error", error=f"{type(exc).__name__}: {exc}")
            self.add_event("error", f"轮次 {rnd.id}：打包整组下载失败：{exc}", round_id=rnd.id)

    def _make_bundle_zip(self, rnd: Round) -> Path:
        round_root = self.settings.snapshot_dir / rnd.id
        manifest = {
            "kind": "artifact-vault-dual-volume-snapshot-set",
            "round_id": rnd.id,
            "round_seq": rnd.seq,
            "note": "Index and blobs share the same backup round; restore both volumes from this set.",
            "created_at_unix": rnd.created_at_unix,
            "finished_at_unix": rnd.finished_at_unix,
            "window_seconds": rnd.window_seconds,
            "inflight_at_pause": rnd.inflight_at_pause,
            "agent_reports": rnd.reports,
            "volumes": {
                "index": {"mount_in_zip": "index/", "snapshot_dir": str(round_root / "index")},
                "blobs": {"mount_in_zip": "blobs/", "snapshot_dir": str(round_root / "blobs")},
            },
            "artifacts": [
                self.artifacts[aid].to_dict()
                for aid in rnd.inflight_at_pause
                if aid in self.artifacts
            ],
        }
        self.settings.bundle_dir.mkdir(parents=True, exist_ok=True)
        final_path = self.bundle_path(rnd)
        tmp_path = final_path.with_suffix(".zip.tmp")
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
            for label in VOLUME_LABELS:
                src = round_root / label
                if not src.is_dir():
                    raise FileNotFoundError(f"missing snapshot directory for {label}: {src}")
                for path in sorted(src.rglob("*")):
                    if path.is_file():
                        zf.write(path, arcname=str(Path(label) / path.relative_to(src)))
        os.replace(tmp_path, final_path)
        return final_path

    # --------------------------- agent admin ------------------------------- #
    async def agents_overview(self) -> dict[str, Any]:
        async def one(label: str, client: AgentClient) -> tuple[str, dict[str, Any]]:
            try:
                cfg = await client.get_config()
                return label, {"reachable": True, "config": cfg}
            except Exception as exc:
                return label, {"reachable": False, "error": f"{type(exc).__name__}: {exc}"}

        pairs = await asyncio.gather(
            *(one(label, client) for label, client in self.agents.items())
        )
        return {label: info for label, info in pairs}

    async def put_agent_config(self, label: str, patch: dict[str, Any]) -> dict[str, Any]:
        if label not in self.agents:
            raise AgentConfigError(f"unknown volume agent: {label}")
        return await self.agents[label].put_config(patch)

    # --------------------------- serialization ------------------------------ #
    def public_state(self) -> dict[str, Any]:
        now = time.time()
        # Keep the visible status fresh even between timer ticks.
        self._expire_reservations()
        return {
            "server_unix": now,
            "accepting": self.accepting,
            "active_round_id": self.active_round_id,
            "in_flight": len(self.pending),
            "pending_ids": sorted(self.pending),
            "settings": self.settings_dict(),
            "reservations": [
                self.reservations[rid].to_dict(now=now)
                for rid in reversed(self.reservation_order)
                if rid in self.reservations
            ],
            "queued_reservations": sum(
                1 for r in self.reservations.values() if r.state == "queued"
            ),
            "artifacts": [
                self.artifacts[aid].to_dict()
                for aid in reversed(self.artifact_order)
            ],
            "rounds": [
                self.rounds[rid].to_dict(now=now) for rid in reversed(self.round_order)
            ],
            "events": list(self.events)[-80:][::-1],
        }
