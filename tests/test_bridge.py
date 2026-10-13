import asyncio
import json
from pathlib import Path

import pytest

from distributedcc.bridge import Delivery, decode_delivery, decode_presence, inject_delivery
from distributedcc.protocol import parse_peer_envelope
from distributedcc.registry import LocalSession


def test_decode_presence_builds_remote_sessions() -> None:
    sessions = decode_presence(
        json.dumps(
            {
                "type": "presence",
                "sessions": [
                    {
                        "name": "yrrp-release-manager",
                        "session_id": "manager-session",
                        "mode": "bypass",
                        "node": "andre",
                    }
                ],
            }
        )
    )

    assert sessions[0].name == "yrrp-release-manager"
    assert sessions[0].node == "andre"


def test_decode_delivery_rejects_unknown_fields() -> None:
    raw = json.dumps(
        {
            "type": "deliver",
            "id": "60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
            "source_node": "friend",
            "source_session": "friend-feature",
            "source_session_id": "friend-session",
            "source_mode": "prompting",
            "target_session": "yrrp-release-manager",
            "body": "hello",
            "forged": True,
        }
    )

    with pytest.raises(ValueError, match="delivery fields"):
        decode_delivery(raw)


@pytest.mark.asyncio
async def test_injects_canonical_peer_event_into_target_socket(tmp_path: Path) -> None:
    socket_path = tmp_path / "target.sock"
    received: asyncio.Future[dict[str, object]] = asyncio.Future()

    async def capture(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        line = await reader.readline()
        received.set_result(json.loads(line))
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_unix_server(capture, path=socket_path)
    target = LocalSession(
        name="yrrp-release-manager",
        session_id="manager-session",
        pid=123,
        proc_start="4242",
        socket_path=socket_path,
        permission_mode="bypass",
        status="idle",
    )
    delivery = Delivery(
        id="60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
        source_node="friend",
        source_session="friend-feature",
        source_session_id="friend-session",
        source_mode="prompting",
        target_session="yrrp-release-manager",
        body="Please inspect attached phone.",
    )

    try:
        await inject_delivery(
            target,
            delivery,
            source_address="uds:/run/user/1000/cc-socks/999.sock",
        )
        frame = await asyncio.wait_for(received, timeout=1)
    finally:
        server.close()
        await server.wait_closed()

    assert frame["type"] == "user"
    assert frame["msgV"] == 1
    envelope = parse_peer_envelope(frame["message"]["content"])
    assert envelope.address == "uds:/run/user/1000/cc-socks/999.sock"
    assert envelope.name == "friend-feature"
    assert envelope.mode == "prompting"
    assert envelope.body == "Please inspect attached phone."
