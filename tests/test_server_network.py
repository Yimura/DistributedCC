import asyncio
import json

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from distributedcc.protocol import ProtocolError, decode_publish_message
from distributedcc.server import (
    Broker,
    TokenAuthenticator,
    handle_websocket,
    make_process_request,
)


def test_authenticator_maps_unique_secret_to_fixed_node() -> None:
    authenticator = TokenAuthenticator.from_json(
        json.dumps({"andre": "andre-secret", "friend": "friend-secret"})
    )

    assert authenticator.authenticate("Bearer friend-secret") == "friend"
    assert authenticator.authenticate("Bearer andre-secret") == "andre"


@pytest.mark.parametrize(
    "header",
    [None, "", "Basic friend-secret", "Bearer wrong", "Bearer"],
)
def test_authenticator_rejects_invalid_header(header: str | None) -> None:
    authenticator = TokenAuthenticator.from_json(json.dumps({"friend": "friend-secret"}))

    assert authenticator.authenticate(header) is None


def test_authenticator_rejects_duplicate_secret() -> None:
    with pytest.raises(ValueError, match="unique"):
        TokenAuthenticator.from_json(
            json.dumps({"andre": "shared-secret", "friend": "shared-secret"})
        )


def test_decode_publish_accepts_empty_session_list() -> None:
    assert decode_publish_message('{"type":"publish","sessions":[]}') == []


def test_decodes_strict_publish_message() -> None:
    sessions = decode_publish_message(
        json.dumps(
            {
                "type": "publish",
                "sessions": [
                    {
                        "name": "yrrp-release-manager",
                        "session_id": "session-manager",
                        "mode": "bypass",
                    }
                ],
            }
        )
    )

    assert sessions[0].name == "yrrp-release-manager"
    assert sessions[0].permission_mode == "bypass"


def test_publish_rejects_extra_session_fields() -> None:
    raw = json.dumps(
        {
            "type": "publish",
            "sessions": [
                {
                    "name": "friend-feature",
                    "session_id": "session-feature",
                    "mode": "prompting",
                    "node": "andre",
                }
            ],
        }
    )

    with pytest.raises(ProtocolError, match="session fields"):
        decode_publish_message(raw)


async def _wait_for_remote_sessions(
    broker: Broker,
    node_id: str,
    expected: list[dict[str, str]],
) -> None:
    async with asyncio.timeout(2):
        while await broker.remote_sessions(node_id) != expected:
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_abrupt_close_cleans_up_and_allows_reconnect(caplog) -> None:
    authenticator = TokenAuthenticator.from_json('{"andre":"secret"}')
    broker = Broker()

    async with serve(
        lambda websocket: handle_websocket(websocket, broker, authenticator),
        "127.0.0.1",
        0,
        process_request=make_process_request(authenticator),
    ) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://127.0.0.1:{port}"
        client = await connect(
            url,
            additional_headers={"Authorization": "Bearer secret"},
        )
        await client.send(
            json.dumps(
                {
                    "type": "publish",
                    "sessions": [
                        {"name": "manager", "session_id": "one", "mode": "bypass"}
                    ],
                }
            )
        )
        expected = [
            {
                "name": "manager",
                "session_id": "one",
                "mode": "bypass",
                "node": "andre",
            }
        ]
        await _wait_for_remote_sessions(broker, "friend", expected)

        client.transport.abort()
        await _wait_for_remote_sessions(broker, "friend", [])

        async with connect(
            url,
            additional_headers={"Authorization": "Bearer secret"},
        ) as replacement:
            await replacement.send('{"type":"publish","sessions":[]}')

    assert "abnormal WebSocket close for node andre" in caplog.text
    assert "secret" not in caplog.text
