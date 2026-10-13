import json
import multiprocessing
import time
from pathlib import Path

import pytest

from distributedcc.protocol import ProtocolError, build_peer_envelope
from distributedcc.virtual_peer import (
    RemoteSession,
    VirtualPeerProcess,
    build_session_record,
    decode_native_send,
)


def test_builds_discoverable_transient_session_record(tmp_path: Path) -> None:
    remote = RemoteSession(
        name="yrrp-release-manager",
        session_id="manager-session",
        node="andre",
        permission_mode="bypass",
    )

    record = build_session_record(
        remote=remote,
        pid=123,
        proc_start="4242",
        socket_path=tmp_path / "123.sock",
        now_ms=1000,
    )

    assert record["pid"] == 123
    assert record["name"] == "yrrp-release-manager"
    assert record["sessionId"] == "manager-session"
    assert record["version"] == "2.1.294"
    assert record["peerProtocol"] == 1
    assert record["messagingSocketPath"] == str(tmp_path / "123.sock")


def test_decodes_native_send_without_trusting_claimed_identity() -> None:
    content = build_peer_envelope(
        address="uds:/run/user/1000/cc-socks/111.sock",
        session_id="claimed-session",
        name="claimed-name",
        mode="bypass",
        body="FEATURE_READY",
    )
    frame = json.dumps(
        {
            "type": "user",
            "from": "uds:/run/user/1000/cc-socks/111.sock",
            "message": {"role": "user", "content": content},
        }
    )

    assert decode_native_send(frame) == "FEATURE_READY"


def test_decodes_native_send_without_optional_from_session() -> None:
    frame = json.dumps(
        {
            "type": "user",
            "message": {
                "role": "user",
                "content": (
                    '<cross-session-message from="uds:/run/user/1000/cc-socks/111.sock" '
                    'from-name="feature-owner" from-mode="bypass">\n'
                    "hello\n"
                    "</cross-session-message>"
                ),
            },
        }
    )

    assert decode_native_send(frame) == "hello"


def test_rejects_non_user_native_frame() -> None:
    with pytest.raises(ProtocolError, match="native user frame"):
        decode_native_send(json.dumps({"type": "control", "action": "rename"}))


def test_rejects_native_frame_with_extra_message_fields() -> None:
    content = build_peer_envelope(
        address="bridge:dcc/node/session",
        session_id="session",
        name="session",
        mode="prompting",
        body="hello",
    )
    frame = json.dumps(
        {
            "type": "user",
            "message": {"role": "user", "content": content, "hidden": "payload"},
        }
    )

    with pytest.raises(ProtocolError, match="message object"):
        decode_native_send(frame)


def test_virtual_peer_process_registers_and_cleans_up(tmp_path: Path) -> None:
    sessions_dir = tmp_path / "sessions"
    sockets_dir = tmp_path / "sockets"
    sessions_dir.mkdir()
    sockets_dir.mkdir()
    outbound: multiprocessing.Queue[dict[str, str]] = multiprocessing.Queue()
    worker = VirtualPeerProcess(
        remote=RemoteSession(
            name="yrrp-release-manager",
            session_id="manager-session",
            node="andre",
            permission_mode="bypass",
        ),
        sessions_dir=sessions_dir,
        sockets_dir=sockets_dir,
        outbound=outbound,
    )

    worker.start()
    deadline = time.monotonic() + 3
    records: list[Path] = []
    while time.monotonic() < deadline:
        records = list(sessions_dir.glob("*.json"))
        if records:
            break
        time.sleep(0.02)

    assert len(records) == 1
    record = json.loads(records[0].read_text())
    assert record["name"] == "yrrp-release-manager"
    assert Path(record["messagingSocketPath"]).exists()

    worker.stop()

    assert list(sessions_dir.glob("*.json")) == []
    assert list(sockets_dir.glob("*.sock")) == []
