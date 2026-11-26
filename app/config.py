"""Configuration for the backup web service.

All paths / endpoints are environment driven so the same image runs under
Docker Compose (service DNS names + mounted volumes) and locally
(see scripts/run-local.sh).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# Hard safety cap from the requirement: the quiescent stop-write capture
# window must never exceed 3 seconds.  Operators may configure a smaller
# window (e.g. for fast tests), never a larger one.
MAX_STOP_WINDOW_SECONDS = 3.0


@dataclass(frozen=True)
class Settings:
    index_dir: Path
    blob_dir: Path
    snapshot_dir: Path
    bundle_dir: Path
    index_agent_url: str
    blob_agent_url: str
    # Interval used for newly accepted artifacts when the caller does not
    # override it per request.
    default_write_interval: float
    # How long to wait for already-accepted artifacts to land on BOTH
    # volumes before giving up on a round.
    drain_timeout_seconds: float
    # Max time both agents get to report snapshot success after drain.
    snapshot_window_seconds: float
    # Per HTTP call to a snapshot agent; deliberately longer than the
    # capture window so late responses can still be observed and archived.
    agent_http_timeout_seconds: float
    # A queued publish reservation that is not accepted by a backup round
    # within this many seconds auto-expires and is never published.  The
    # product rule is 10 seconds; overridable only to make tests/demos fast.
    reservation_ttl_seconds: float = 10.0
    history_rounds: int = 50
    history_artifacts: int = 100
    history_reservations: int = 100
    history_events: int = 200

    @staticmethod
    def from_env() -> "Settings":
        snapshot_dir = Path(os.getenv("SNAPSHOT_DIR", "./data/snapshots")).resolve()
        bundle_dir = Path(os.getenv("BUNDLE_DIR", str(snapshot_dir / "bundles"))).resolve()
        window = _float("SNAPSHOT_WINDOW_SECONDS", MAX_STOP_WINDOW_SECONDS)
        window = max(0.1, min(window, MAX_STOP_WINDOW_SECONDS))
        return Settings(
            index_dir=Path(os.getenv("INDEX_DIR", "./data/index")).resolve(),
            blob_dir=Path(os.getenv("BLOB_DIR", "./data/blobs")).resolve(),
            snapshot_dir=snapshot_dir,
            bundle_dir=bundle_dir,
            index_agent_url=os.getenv("SNAP_INDEX_URL", "http://127.0.0.1:8001").rstrip("/"),
            blob_agent_url=os.getenv("SNAP_BLOB_URL", "http://127.0.0.1:8002").rstrip("/"),
            default_write_interval=max(0.0, _float("WRITE_INTERVAL_SECONDS", 1.2)),
            drain_timeout_seconds=max(1.0, _float("DRAIN_TIMEOUT_SECONDS", 60.0)),
            snapshot_window_seconds=window,
            agent_http_timeout_seconds=max(
                window + 5.0, _float("AGENT_HTTP_TIMEOUT_SECONDS", 20.0)
            ),
            reservation_ttl_seconds=max(0.1, _float("RESERVATION_TTL_SECONDS", 10.0)),
        )

    def ensure_dirs(self) -> None:
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.blob_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.bundle_dir.mkdir(parents=True, exist_ok=True)
