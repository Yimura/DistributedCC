import json
from pathlib import Path

from distributedcc.registry import discover_live_sessions, infer_permission_mode


def write_process(proc_root: Path, pid: int, start_token: str, command: list[str]) -> None:
    process_dir = proc_root / str(pid)
    process_dir.mkdir(parents=True)
    stat_fields = ["0"] * 52
    stat_fields[0] = str(pid)
    stat_fields[1] = "(claude)"
    stat_fields[21] = start_token
    (process_dir / "stat").write_text(" ".join(stat_fields))
    (process_dir / "cmdline").write_bytes(b"\0".join(part.encode() for part in command) + b"\0")


def write_session(sessions_dir: Path, pid: int, **overrides: object) -> None:
    record = {
        "pid": pid,
        "sessionId": "session-123",
        "cwd": "/workspace",
        "startedAt": 1000,
        "procStart": "4242",
        "version": "2.1.294",
        "peerProtocol": 1,
        "peerFeatures": ["notify_idle"],
        "kind": "interactive",
        "entrypoint": "cli",
        "messagingSocketPath": f"/run/user/1000/cc-socks/{pid}.sock",
        "name": "yrrp-release-manager",
        "status": "idle",
        "updatedAt": 1000,
    }
    record.update(overrides)
    sessions_dir.mkdir(parents=True, exist_ok=True)
    (sessions_dir / f"{pid}.json").write_text(json.dumps(record))


def test_discovers_live_supported_session(tmp_path: Path) -> None:
    sessions_dir = tmp_path / "sessions"
    proc_root = tmp_path / "proc"
    write_session(sessions_dir, 123)
    write_process(proc_root, 123, "4242", ["claude", "--dangerously-skip-permissions"])

    sessions = discover_live_sessions(sessions_dir, proc_root=proc_root)

    assert list(sessions) == ["yrrp-release-manager"]
    assert sessions["yrrp-release-manager"].permission_mode == "bypass"


def test_rejects_stale_process_start_token(tmp_path: Path) -> None:
    sessions_dir = tmp_path / "sessions"
    proc_root = tmp_path / "proc"
    write_session(sessions_dir, 123)
    write_process(proc_root, 123, "different", ["claude"])

    assert discover_live_sessions(sessions_dir, proc_root=proc_root) == {}


def test_warns_and_discovers_unverified_claude_version(
    tmp_path: Path, caplog,
) -> None:
    sessions_dir = tmp_path / "sessions"
    proc_root = tmp_path / "proc"
    write_session(sessions_dir, 123, version="2.1.295")
    write_process(proc_root, 123, "4242", ["claude"])

    sessions = discover_live_sessions(sessions_dir, proc_root=proc_root)

    assert list(sessions) == ["yrrp-release-manager"]
    assert "unverified Claude Code version 2.1.295; tested version is 2.1.294" in caplog.text


def test_rejects_unsupported_peer_protocol(tmp_path: Path) -> None:
    sessions_dir = tmp_path / "sessions"
    proc_root = tmp_path / "proc"
    write_session(sessions_dir, 123, peerProtocol=2)
    write_process(proc_root, 123, "4242", ["claude"])

    assert discover_live_sessions(sessions_dir, proc_root=proc_root) == {}


def test_infers_prompting_mode_without_bypass_flag() -> None:
    assert infer_permission_mode(["claude", "--agent", "worker"]) == "prompting"
