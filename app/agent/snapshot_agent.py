"""Local snapshot end ("快照端").

One instance runs per volume (index / blobs).  It copies the *actually
mounted* source directory into ``$SNAPSHOT_ROOT/<round_id>/<volume_label>``
and reports a structured result.

Fault / latency controls are available both through environment defaults and
at runtime via ``GET/PUT /admin/config`` so the backup page (and tests) can
dial latency past the web side's 3s stop window or arm failures.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logger = logging.getLogger("snapshot-agent")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

ROUND_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class SnapshotRequest(BaseModel):
    round_id: str = Field(min_length=1, max_length=64)


class ConfigPatch(BaseModel):
    delay_seconds: float | None = Field(default=None, ge=0.0, le=30.0)
    fail_mode: Literal["none", "always_error"] | None = None
    # true arms a single failing snapshot; it flips back to false after use.
    fail_once: bool | None = None


class AgentRuntime:
    """Runtime paths + mutable fault-injection settings for one volume."""

    def __init__(
        self,
        *,
        label: str,
        source_dir: Path | str,
        snapshot_root: Path | str,
        delay_seconds: float = 0.3,
        fail_mode: Literal["none", "always_error"] = "none",
        fail_once: bool = False,
    ):
        self.label = label
        self.source_dir = Path(source_dir).resolve()
        self.snapshot_root = Path(snapshot_root).resolve()
        self.delay_seconds = max(0.0, min(float(delay_seconds), 30.0))
        self.fail_mode = fail_mode
        self.fail_once = fail_once

    def config(self) -> dict:
        return {
            "volume": self.label,
            "source_dir": str(self.source_dir),
            "snapshot_root": str(self.snapshot_root),
            "delay_seconds": self.delay_seconds,
            "fail_mode": self.fail_mode,
            "fail_once_armed": self.fail_once,
        }

    def validate_round_id(self, round_id: str) -> None:
        if not ROUND_ID_RE.fullmatch(round_id) or ".." in round_id:
            raise HTTPException(status_code=422, detail=f"unsafe round_id: {round_id!r}")

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def copy_volume(self, round_id: str) -> dict:
        """Blocking: really copy every file from the mounted source volume."""
        if not self.source_dir.exists():
            raise FileNotFoundError(f"source dir is not mounted: {self.source_dir}")

        target = self.snapshot_root / round_id / self.label
        if target.exists():
            shutil.rmtree(target)
        target.parent.mkdir(parents=True, exist_ok=True)

        t0 = time.monotonic()
        shutil.copytree(self.source_dir, target)
        for path in target.rglob("*"):
            if path.is_file():
                with open(path, "rb") as fh:
                    os.fsync(fh.fileno())
        self._fsync_dir(target)
        copy_seconds = time.monotonic() - t0

        files_meta = []
        total_bytes = 0
        for path in sorted(target.rglob("*")):
            if not path.is_file() or path.name == "_snapshot_report.json":
                continue
            h = hashlib.sha256()
            size = 0
            with open(path, "rb") as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                    h.update(chunk)
                    size += len(chunk)
            rel = path.relative_to(target).as_posix()
            total_bytes += size
            files_meta.append({"path": rel, "size_bytes": size, "sha256": h.hexdigest()})

        report = {
            "status": "succeeded",
            "round_id": round_id,
            "volume": self.label,
            "copied_from": str(self.source_dir),
            "snapshot_path": str(target),
            "files": len(files_meta),
            "bytes": total_bytes,
            "copy_seconds": copy_seconds,
            "copied_at_unix": time.time(),
            "file_manifest": files_meta,
        }
        # Stamp the snapshot itself with its round id: this is what makes the
        # two volume copies provably belong to the same backup round.
        (target / "_snapshot_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return report


def create_agent_app(rt: AgentRuntime) -> FastAPI:
    app = FastAPI(title=f"snapshot-agent-{rt.label}")

    @app.get("/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "volume": rt.label,
            "source_mounted": rt.source_dir.is_dir(),
        }

    @app.get("/admin/config")
    async def get_config() -> dict:
        return rt.config()

    @app.put("/admin/config")
    async def put_config(patch: ConfigPatch) -> dict:
        if patch.delay_seconds is not None:
            rt.delay_seconds = patch.delay_seconds
        if patch.fail_mode is not None:
            rt.fail_mode = patch.fail_mode
        if patch.fail_once is not None:
            rt.fail_once = patch.fail_once
        logger.info("[%s] config updated: %s", rt.label, rt.config())
        return rt.config()

    @app.post("/snapshot")
    async def create_snapshot(req: SnapshotRequest):
        rt.validate_round_id(req.round_id)
        delay = rt.delay_seconds
        logger.info("[%s] snapshot request round=%s delay=%.3f", rt.label, req.round_id, delay)

        # Latency injection (covers both success and failure paths). The delay
        # value is captured here, so changing config does not rescue a call
        # that is already waiting — exactly what produces late reports.
        if delay > 0:
            await asyncio.sleep(delay)

        one_shot = rt.fail_once
        if one_shot:
            rt.fail_once = False
        if rt.fail_mode == "always_error" or one_shot:
            body = {
                "status": "failed",
                "round_id": req.round_id,
                "volume": rt.label,
                "error_code": "injected_failure",
                "error": (
                    "injected one-shot snapshot failure"
                    if one_shot and rt.fail_mode != "always_error"
                    else "injected persistent snapshot failure"
                ),
                "delay_seconds": delay,
                "at_unix": time.time(),
            }
            logger.warning("[%s] injected failure round=%s one_shot=%s", rt.label, req.round_id, one_shot)
            return JSONResponse(status_code=503, content=body)

        try:
            report = await asyncio.to_thread(rt.copy_volume, req.round_id)
        except Exception as exc:
            logger.exception("[%s] snapshot copy failed", rt.label)
            return JSONResponse(
                status_code=500,
                content={
                    "status": "failed",
                    "round_id": req.round_id,
                    "volume": rt.label,
                    "error_code": "copy_error",
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
        report["delay_seconds"] = delay
        report["elapsed_seconds"] = delay + float(report.get("copy_seconds") or 0.0)
        logger.info(
            "[%s] snapshot ok round=%s files=%s bytes=%s",
            rt.label, req.round_id, report["files"], report["bytes"],
        )
        return report

    return app


def _runtime_from_env() -> AgentRuntime:
    return AgentRuntime(
        label=os.getenv("VOLUME_LABEL", "volume"),
        source_dir=os.getenv("SOURCE_DIR", "/source"),
        snapshot_root=os.getenv("SNAPSHOT_ROOT", "/snapshots"),
        delay_seconds=float(os.getenv("AGENT_DELAY_SECONDS", "0.3")),
        fail_mode="always_error" if os.getenv("AGENT_FAIL_MODE", "none") == "always_error" else "none",
        fail_once=os.getenv("AGENT_FAIL_ONCE", "0") == "1",
    )


# uvicorn app.agent.snapshot_agent:app
app = create_agent_app(_runtime_from_env())
