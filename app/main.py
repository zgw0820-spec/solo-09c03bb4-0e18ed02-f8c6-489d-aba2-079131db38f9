"""FastAPI backup page + JSON API for the artifact vault."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from .config import Settings
from .manager import (
    AcceptancePaused,
    AgentConfigError,
    Manager,
    NotPaused,
    ReservationNotFound,
    ReservationNotWithdrawable,
    RoundAlreadyActive,
)

logger = logging.getLogger("artifact-lab.web")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

STATIC_INDEX = Path(__file__).parent / "static" / "index.html"


class ArtifactRequest(BaseModel):
    name: str | None = Field(default=None, max_length=120)
    interval_seconds: float | None = Field(default=None, ge=0.0, le=60.0)


class SettingsRequest(BaseModel):
    write_interval_seconds: float | None = Field(default=None, ge=0.0, le=60.0)


class AgentConfigRequest(BaseModel):
    delay_seconds: float | None = Field(default=None, ge=0.0, le=30.0)
    fail_mode: str | None = Field(default=None, pattern="^(none|always_error)$")
    fail_once: bool | None = None


def create_app(
    settings: Settings | None = None,
    transports: dict[str, Any] | None = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    settings.ensure_dirs()
    manager = Manager(settings, transports=transports)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        await manager.aclose()

    app = FastAPI(title="制品库双卷备份台", version="1.0.0", lifespan=lifespan)
    app.state.manager = manager
    app.state.settings = settings

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        return HTMLResponse(STATIC_INDEX.read_text(encoding="utf-8"))

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/state")
    async def get_state() -> dict[str, Any]:
        return manager.public_state()

    @app.post("/api/settings")
    async def post_settings(req: SettingsRequest) -> dict[str, Any]:
        return manager.update_settings(req.model_dump(exclude_none=True))

    @app.post("/api/artifacts", status_code=201)
    async def create_artifact(req: ArtifactRequest) -> dict[str, Any]:
        try:
            art = manager.accept_artifact(req.name, req.interval_seconds)
        except AcceptancePaused as exc:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": "acceptance_paused",
                    "error": "备份轮次进行中，已暂停接单，请等本轮结束",
                    "active_round_id": exc.active_round_id,
                },
            )
        return {"ok": True, "artifact": art.to_dict()}

    @app.post("/api/backup/start", status_code=201)
    async def start_backup() -> dict[str, Any]:
        try:
            rnd = manager.start_backup()
        except RoundAlreadyActive as exc:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": "round_already_active",
                    "error": "一次只能跑一轮双卷备份",
                    "active_round_id": exc.active_round_id,
                },
            )
        return {"ok": True, "round": rnd.to_dict()}

    @app.post("/api/publish-queue", status_code=201)
    async def queue_publish(req: ArtifactRequest) -> dict[str, Any]:
        """Queue a publish order while acceptance is paused (e.g. mid-backup).

        The queue lives in this web process only; nothing is written to the
        volumes while queued, and a queued order can be withdrawn.
        """
        try:
            rsv = manager.queue_publish(req.name, req.interval_seconds)
        except NotPaused:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": "not_paused",
                    "error": "当前正在接单，无需排队，请直接发布",
                },
            )
        return {"ok": True, "reservation": rsv.to_dict()}

    @app.post("/api/publish-queue/{reservation_id}/withdraw")
    async def withdraw_publish(reservation_id: str) -> dict[str, Any]:
        try:
            rsv = manager.withdraw_reservation(reservation_id)
        except ReservationNotFound:
            return JSONResponse(
                status_code=404,
                content={"ok": False, "error_code": "reservation_not_found", "error": "预约不存在"},
            )
        except ReservationNotWithdrawable as exc:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": "reservation_not_queued",
                    "error": f"该预约已经是 {exc.state}，不能再撤回",
                    "state": exc.state,
                },
            )
        return {"ok": True, "reservation": rsv.to_dict()}

    @app.get("/api/rounds/{round_id}")
    async def get_round(round_id: str) -> dict[str, Any]:
        rnd = manager.get_round(round_id)
        if rnd is None:
            return JSONResponse(status_code=404, content={"ok": False, "error": "round not found"})
        return {"ok": True, "round": rnd.to_dict()}

    @app.get("/api/rounds/{round_id}/download")
    async def download_round(round_id: str):
        rnd = manager.get_round(round_id)
        if rnd is None:
            return JSONResponse(status_code=404, content={"ok": False, "error": "round not found"})
        if rnd.state != "success":
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": "bundle_not_available",
                    "error": f"本轮状态为 {rnd.state}，不提供整组下载",
                    "failure_code": rnd.failure_code,
                    "failure_message": rnd.failure_message,
                },
            )
        path = manager.bundle_path(rnd)
        if rnd.bundle.get("state") != "ready" or not path.is_file():
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": "bundle_not_ready",
                    "error": "两卷快照虽已成功，整组压缩包尚未就绪，请稍候重试",
                    "bundle": rnd.bundle,
                },
            )
        return FileResponse(
            path,
            media_type="application/zip",
            filename=f"artifact-vault-{rnd.id}-dual-volume.zip",
        )

    @app.get("/api/agents")
    async def agents_overview() -> dict[str, Any]:
        return {"agents": await manager.agents_overview()}

    @app.put("/api/agents/{label}/config")
    async def put_agent_config(label: str, req: AgentConfigRequest) -> dict[str, Any]:
        try:
            return await manager.put_agent_config(label, req.model_dump(exclude_none=True))
        except AgentConfigError as exc:
            return JSONResponse(status_code=404, content={"ok": False, "error": str(exc)})
        except Exception as exc:  # proxy errors
            return JSONResponse(
                status_code=502,
                content={"ok": False, "error": f"无法写入快照端配置：{type(exc).__name__}: {exc}"},
            )

    return app


# uvicorn app.main:app
app = create_app()
