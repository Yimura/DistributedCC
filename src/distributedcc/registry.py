"""Discovery and validation of live local Claude Code sessions."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

TESTED_CLAUDE_CODE_VERSION = "2.1.296"
SUPPORTED_PEER_PROTOCOL = 1


@dataclass(frozen=True, slots=True)
class LocalSession:
    name: str
    session_id: str
    pid: int
    proc_start: str
    socket_path: Path
    permission_mode: Literal["bypass", "prompting"]
    status: str


def infer_permission_mode(command: list[str]) -> Literal["bypass", "prompting"]:
    if "--dangerously-skip-permissions" in command:
        return "bypass"
    return "prompting"


def discover_live_sessions(
    sessions_dir: Path,
    *,
    proc_root: Path = Path("/proc"),
) -> dict[str, LocalSession]:
    sessions: dict[str, LocalSession] = {}
    for path in sessions_dir.glob("*.json"):
        record = _load_record(path)
        session = _validated_session(record, proc_root)
        if session is None or session.name in sessions:
            continue
        sessions[session.name] = session
    return sessions


def _load_record(path: Path) -> dict[str, object] | None:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _validated_session(
    record: dict[str, object] | None,
    proc_root: Path,
) -> LocalSession | None:
    if record is None:
        return None
    version = record.get("version")
    if not isinstance(version, str) or not version:
        return None
    if version != TESTED_CLAUDE_CODE_VERSION:
        _warn_unverified_version(version)
    if record.get("peerProtocol") != SUPPORTED_PEER_PROTOCOL:
        return None

    pid = record.get("pid")
    proc_start = record.get("procStart")
    if not isinstance(pid, int) or pid <= 0 or not isinstance(proc_start, str):
        return None
    if _read_start_token(proc_root, pid) != proc_start:
        return None

    name = record.get("name")
    session_id = record.get("sessionId")
    socket_path = record.get("messagingSocketPath")
    status = record.get("status", "unknown")
    if not all(isinstance(value, str) and value for value in (name, session_id, socket_path)):
        return None
    if not isinstance(status, str):
        return None

    command = _read_command(proc_root, pid)
    if command is None:
        return None
    return LocalSession(
        name=name,
        session_id=session_id,
        pid=pid,
        proc_start=proc_start,
        socket_path=Path(socket_path),
        permission_mode=infer_permission_mode(command),
        status=status,
    )


@lru_cache(maxsize=32)
def _warn_unverified_version(version: str) -> None:
    logging.warning(
        "unverified Claude Code version %s; tested version is %s",
        version,
        TESTED_CLAUDE_CODE_VERSION,
    )


def _read_start_token(proc_root: Path, pid: int) -> str | None:
    try:
        stat = (proc_root / str(pid) / "stat").read_text()
        tail = stat.rsplit(") ", maxsplit=1)[1].split()
    except (OSError, IndexError):
        return None
    return tail[19] if len(tail) > 19 else None


def _read_command(proc_root: Path, pid: int) -> list[str] | None:
    try:
        raw = (proc_root / str(pid) / "cmdline").read_bytes()
    except OSError:
        return None
    return [part.decode(errors="replace") for part in raw.split(b"\0") if part]
