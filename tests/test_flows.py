"""End-to-end state-machine tests.

The two snapshot agents run as in-process ASGI apps (with REAL temporary
directories that they really copy) while the web app is also driven through
ASGI.  Timings are shortened but keep the same ordering and the same
deadline/late-report code paths used in production.
"""
from __future__ import annotations

import asyncio
import io
import json
import tempfile
import time
import zipfile
from pathlib import Path

import httpx
from httpx import ASGITransport

from app.agent.snapshot_agent import AgentRuntime, create_agent_app
from app.config import Settings
from app.main import create_app


class Harness:
    def __init__(
        self,
        *,
        window: float = 2.0,
        drain_timeout: float = 10.0,
        index_delay: float = 0.05,
        blob_delay: float = 0.05,
    ):
        self._tmp = tempfile.TemporaryDirectory(prefix="vault-test-")
        root = Path(self._tmp.name)
        self.root = root
        self.index_dir = root / "index"
        self.blob_dir = root / "blobs"
        self.snapshot_dir = root / "snapshots"
        self.bundle_dir = self.snapshot_dir / "bundles"
        for d in (self.index_dir, self.blob_dir, self.snapshot_dir, self.bundle_dir):
            d.mkdir(parents=True, exist_ok=True)

        self.settings = Settings(
            index_dir=self.index_dir,
            blob_dir=self.blob_dir,
            snapshot_dir=self.snapshot_dir,
            bundle_dir=self.bundle_dir,
            index_agent_url="http://snap-index",
            blob_agent_url="http://snap-blob",
            default_write_interval=0.2,
            drain_timeout_seconds=drain_timeout,
            snapshot_window_seconds=window,
            agent_http_timeout_seconds=10.0,
        )
        self.rt_index = AgentRuntime(
            label="index",
            source_dir=self.index_dir,
            snapshot_root=self.snapshot_dir,
            delay_seconds=index_delay,
        )
        self.rt_blob = AgentRuntime(
            label="blobs",
            source_dir=self.blob_dir,
            snapshot_root=self.snapshot_dir,
            delay_seconds=blob_delay,
        )
        self.agent_apps = {
            "index": create_agent_app(self.rt_index),
            "blobs": create_agent_app(self.rt_blob),
        }
        self.web = create_app(
            self.settings,
            transports={
                "index": ASGITransport(app=self.agent_apps["index"]),
                "blobs": ASGITransport(app=self.agent_apps["blobs"]),
            },
        )
        self.client = httpx.AsyncClient(
            transport=ASGITransport(app=self.web), base_url="http://web"
        )

    async def aclose(self) -> None:
        await self.client.aclose()
        await self.web.state.manager.aclose()
        self._tmp.cleanup()


async def state(h: Harness) -> dict:
    return (await h.client.get("/api/state")).json()


def get_round(s: dict, rid: str) -> dict:
    return next(r for r in s["rounds"] if r["id"] == rid)


async def wait_for(h: Harness, pred, timeout: float = 8.0, interval: float = 0.03):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await state(h)
        if pred(last):
            return last
        await asyncio.sleep(interval)
    raise AssertionError(
        "timed out waiting for condition; last state="
        + json.dumps(last, ensure_ascii=False, default=str)[:4000]
    )


async def wait_terminal(h: Harness, rid: str, timeout: float = 8.0) -> dict:
    s = await wait_for(h, lambda st: get_round(st, rid)["terminal"], timeout=timeout)
    return get_round(s, rid)


def scenario(fn=None, **harness_kwargs):
    """Run an async scenario inside asyncio with a fresh Harness.

    Usable as ``@scenario`` or ``@scenario(window=0.8, index_delay=1.4)``.
    """

    def decorate(f):
        def wrapper():
            async def main():
                h = Harness(**harness_kwargs)
                try:
                    await f(h)
                finally:
                    await h.aclose()

            asyncio.run(main())

        wrapper.__name__ = f.__name__
        return wrapper

    if fn is not None:
        return decorate(fn)
    return decorate


# --------------------------------------------------------------------------- #
@scenario
async def test_page_served(h: Harness):
    r = await h.client.get("/")
    assert r.status_code == 200
    assert "双卷" in r.text and "备份" in r.text
    assert (await h.client.get("/health")).json()["status"] == "ok"


@scenario
async def test_artifact_two_landings_with_adjustable_interval(h: Harness):
    t0 = time.time()
    r = await h.client.post("/api/artifacts", json={"interval_seconds": 0.35})
    assert r.status_code == 201, r.text
    aid = r.json()["artifact"]["id"]

    s = await wait_for(h, lambda st: st["artifacts"][0]["state"] == "complete")
    art = next(a for a in s["artifacts"] if a["id"] == aid)
    assert art["state"] == "complete", art
    assert art["blob_at_unix"] and art["index_at_unix"]
    # The gap between the two real landings respects the adjustable interval.
    gap = art["index_at_unix"] - art["blob_at_unix"]
    assert gap >= 0.30, gap
    assert art["index_at_unix"] >= art["blob_at_unix"]

    assert (h.blob_dir / f"{aid}.bin").is_file()
    index_file = h.index_dir / f"{aid}.json"
    assert index_file.is_file()
    entry = json.loads(index_file.read_text(encoding="utf-8"))
    assert entry["id"] == aid and entry["sha256"] == art["sha256"]
    assert time.time() - t0 >= 0.3


@scenario
async def test_backup_waits_for_inflight_and_offers_group_download(h: Harness):
    # Long-enough interval so the backup must visibly drain first.
    r = await h.client.post(
        "/api/artifacts", json={"name": "在途制品", "interval_seconds": 0.8}
    )
    aid = r.json()["artifact"]["id"]

    r = await h.client.post("/api/backup/start")
    assert r.status_code == 201, r.text
    rid = r.json()["round"]["id"]

    # One round at a time.
    r2 = await h.client.post("/api/backup/start")
    assert r2.status_code == 409
    assert r2.json()["error_code"] == "round_already_active"

    # Acceptance is paused: new orders are rejected immediately.
    r3 = await h.client.post("/api/artifacts", json={"name": "备份期间"})
    assert r3.status_code == 409
    assert r3.json()["error_code"] == "acceptance_paused"

    rnd = await wait_terminal(h, rid)
    assert rnd["state"] == "success", rnd
    assert rnd["drain_seconds"] >= 0.7, rnd["drain_seconds"]
    assert set(rnd["reports"]) == {"index", "blobs"}
    assert all(rep["ok"] for rep in rnd["reports"].values())

    # Acceptance restored.
    s = await state(h)
    assert s["accepting"] is True and s["active_round_id"] is None
    r4 = await h.client.post("/api/artifacts", json={"interval_seconds": 0})
    assert r4.status_code == 201

    # Real same-round snapshot directories on both volumes.
    idx_report = h.snapshot_dir / rid / "index" / "_snapshot_report.json"
    blob_report = h.snapshot_dir / rid / "blobs" / "_snapshot_report.json"
    assert idx_report.is_file() and blob_report.is_file()
    assert json.loads(idx_report.read_text())["round_id"] == rid
    assert json.loads(blob_report.read_text())["round_id"] == rid

    # Whole-group zip becomes downloadable and ties both volumes to the round.
    s = await wait_for(h, lambda st: get_round(st, rid)["download_available"])
    rdl = await h.client.get(f"/api/rounds/{rid}/download")
    assert rdl.status_code == 200
    zf = zipfile.ZipFile(io.BytesIO(rdl.content))
    names = zf.namelist()
    assert "manifest.json" in names
    assert any(n.startswith("index/") and n.endswith(".json") for n in names)
    assert any(n.startswith("blobs/") and n.endswith(".bin") for n in names)
    manifest = json.loads(zf.read("manifest.json"))
    assert manifest["round_id"] == rid
    assert aid in manifest["inflight_at_pause"]


@scenario(window=0.8, index_delay=1.4, blob_delay=0.05)
async def test_capture_timeout_late_report_cannot_change_round_or_next(h: Harness):
    t0 = time.monotonic()
    r = await h.client.post("/api/backup/start")
    rid1 = r.json()["round"]["id"]

    rnd1 = await wait_terminal(h, rid1)
    elapsed = time.monotonic() - t0
    assert rnd1["state"] == "failed"
    assert rnd1["failure_code"] == "capture_timeout"
    assert 0.7 <= elapsed < 1.3, elapsed  # settled at the 0.8s window, not 1.4s
    assert rnd1["download_available"] is False

    # Immediate resume: a second round can start right away.
    s = await state(h)
    assert s["accepting"] is True
    rdl = await h.client.get(f"/api/rounds/{rid1}/download")
    assert rdl.status_code == 409

    # Heal the slow agent; launch round 2 before the round-1 call returns.
    h.rt_index.delay_seconds = 0.05
    r = await h.client.post("/api/backup/start")
    assert r.status_code == 201
    rid2 = r.json()["round"]["id"]
    rnd2 = await wait_terminal(h, rid2)

    s = await wait_for(h, lambda st: get_round(st, rid2)["download_available"])
    rnd2 = get_round(s, rid2)
    assert rnd2["state"] == "success"
    assert all(rep["ok"] for rep in rnd2["reports"].values())

    # Wait for the genuine late response of round 1's slow index call.
    s = await wait_for(
        h, lambda st: get_round(st, rid1)["late_report_count"] >= 1, timeout=5
    )
    r1_after = get_round(s, rid1)
    r2_after = get_round(s, rid2)

    # The original verdict is untouched...
    assert r1_after["state"] == "failed"
    assert r1_after["failure_code"] == "capture_timeout"
    late = r1_after["late_reports"]
    assert late[0]["label"] == "index"
    assert late[0]["report"]["ok"] is True  # it actually finished, but too late
    # ...and the next round is completely clean.
    assert r2_after["state"] == "success"
    assert r2_after["late_report_count"] == 0
    assert set(r2_after["reports"]) == {"index", "blobs"}

    # Round 2 bundle is independently downloadable.
    assert (await h.client.get(f"/api/rounds/{rid2}/download")).status_code == 200


@scenario(window=2.0, index_delay=0.9, blob_delay=0.25)
async def test_agent_failure_settles_immediately_other_report_goes_late(h: Harness):
    h.rt_blob.fail_once = True
    t0 = time.monotonic()
    rid = (await h.client.post("/api/backup/start")).json()["round"]["id"]

    rnd = await wait_terminal(h, rid)
    settle_elapsed = time.monotonic() - t0
    assert rnd["state"] == "failed"
    assert rnd["failure_code"] == "agent_failure"
    assert settle_elapsed < 0.8, settle_elapsed  # did not wait for slow index/window
    assert (await state(h))["accepting"] is True
    assert (await h.client.get(f"/api/rounds/{rid}/download")).status_code == 409

    # The slow-but-successful index report arrives later and is archived only.
    s = await wait_for(h, lambda st: get_round(st, rid)["late_report_count"] >= 1)
    rnd = get_round(s, rid)
    assert rnd["state"] == "failed"
    labels = {x["label"] for x in rnd["late_reports"]}
    assert "index" in labels
    assert all(x["report"]["ok"] for x in rnd["late_reports"] if x["label"] == "index")


@scenario
async def test_concurrent_backup_starts_exactly_one_round(h: Harness):
    results = await asyncio.gather(
        h.client.post("/api/backup/start"),
        h.client.post("/api/backup/start"),
    )
    codes = sorted(r.status_code for r in results)
    assert codes == [201, 409], codes
    rid = next(r.json()["round"]["id"] for r in results if r.status_code == 201)
    rnd = await wait_terminal(h, rid)
    assert rnd["state"] == "success"


@scenario
async def test_settings_update_and_unsafe_round_id_rejected(h: Harness):
    r = await h.client.post("/api/settings", json={"write_interval_seconds": 2.5})
    assert r.status_code == 200 and r.json()["write_interval_seconds"] == 2.5

    agent = httpx.AsyncClient(
        transport=ASGITransport(app=h.agent_apps["index"]), base_url="http://agent"
    )
    try:
        r = await agent.post("/snapshot", json={"round_id": "../etc-passwd"})
        assert r.status_code == 422
    finally:
        await agent.aclose()
