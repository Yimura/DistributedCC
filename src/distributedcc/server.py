"""In-memory authenticated routing broker."""

from __future__ import annotations

import asyncio
import hmac
import json
import os
from dataclasses import dataclass
from http import HTTPStatus
from typing import Protocol

from websockets.asyncio.server import ServerConnection, serve

from distributedcc.protocol import (
    MAX_MESSAGE_BYTES,
    PeerEvent,
    ProtocolError,
    SessionAdvertisement,
    decode_client_message,
    decode_publish_message,
)


class BrokerError(RuntimeError):
    """Raised when broker ownership or routing rules reject an operation."""


class TokenAuthenticator:
    def __init__(self, tokens: dict[str, str]) -> None:
        if not tokens or any(not node or not token for node, token in tokens.items()):
            raise ValueError("node tokens must be non-empty")
        if len(tokens.values()) != len(set(tokens.values())):
            raise ValueError("node tokens must be unique")
        self._tokens = dict(tokens)

    @classmethod
    def from_json(cls, raw: str) -> TokenAuthenticator:
        value = json.loads(raw)
        if not isinstance(value, dict) or not all(
            isinstance(node, str) and isinstance(token, str) for node, token in value.items()
        ):
            raise ValueError("node token configuration must be a string map")
        return cls(value)

    def authenticate(self, authorization: str | None) -> str | None:
        if authorization is None or not authorization.startswith("Bearer "):
            return None
        supplied = authorization.removeprefix("Bearer ")
        if not supplied:
            return None
        for node_id, expected in self._tokens.items():
            if hmac.compare_digest(supplied, expected):
                return node_id
        return None


class Connection(Protocol):
    async def send_json(self, message: dict[str, object]) -> None: ...


class WebSocketConnection:
    def __init__(self, websocket: ServerConnection) -> None:
        self._websocket = websocket

    async def send_json(self, message: dict[str, object]) -> None:
        await self._websocket.send(json.dumps(message, separators=(",", ":")))


@dataclass(frozen=True, slots=True)
class PublishedSession:
    node_id: str
    advertisement: SessionAdvertisement


class Broker:
    def __init__(self) -> None:
        self._connections: dict[str, Connection] = {}
        self._sessions: dict[str, PublishedSession] = {}
        self._lock = asyncio.Lock()

    async def attach(self, node_id: str, connection: Connection) -> None:
        async with self._lock:
            if node_id in self._connections:
                raise BrokerError(f"node {node_id!r} is already connected")
            self._connections[node_id] = connection

    async def detach(self, node_id: str) -> None:
        async with self._lock:
            self._connections.pop(node_id, None)
            self._sessions = {
                name: session
                for name, session in self._sessions.items()
                if session.node_id != node_id
            }

    async def publish(
        self,
        node_id: str,
        advertisements: list[SessionAdvertisement],
    ) -> None:
        async with self._lock:
            if node_id not in self._connections:
                raise BrokerError(f"node {node_id!r} is not connected")
            incoming_names = [advertisement.name for advertisement in advertisements]
            if len(incoming_names) != len(set(incoming_names)):
                raise BrokerError("publication contains duplicate session names")
            for name in incoming_names:
                owner = self._sessions.get(name)
                if owner is not None and owner.node_id != node_id:
                    raise BrokerError(f"session {name!r} is already published")

            retained = {
                name: session
                for name, session in self._sessions.items()
                if session.node_id != node_id
            }
            retained.update(
                {
                    advertisement.name: PublishedSession(node_id, advertisement)
                    for advertisement in advertisements
                }
            )
            self._sessions = retained

    async def remote_sessions(self, node_id: str) -> list[dict[str, str]]:
        async with self._lock:
            sessions = [
                {
                    "name": published.advertisement.name,
                    "session_id": published.advertisement.session_id,
                    "mode": published.advertisement.permission_mode,
                    "node": published.node_id,
                }
                for published in self._sessions.values()
                if published.node_id != node_id
            ]
        return sorted(sessions, key=lambda session: session["name"])

    async def broadcast_presence(self) -> None:
        async with self._lock:
            connections = dict(self._connections)
        messages = {
            node_id: {
                "type": "presence",
                "sessions": await self.remote_sessions(node_id),
            }
            for node_id in connections
        }
        await asyncio.gather(
            *(
                connection.send_json(messages[node_id])
                for node_id, connection in connections.items()
            ),
            return_exceptions=True,
        )

    async def route(self, node_id: str, event: PeerEvent) -> None:
        async with self._lock:
            source = self._sessions.get(event.source_session)
            if source is None or source.node_id != node_id:
                raise BrokerError(
                    f"node {node_id!r} does not own session {event.source_session!r}"
                )
            target = self._sessions.get(event.target_session)
            if target is None:
                raise BrokerError(f"target session {event.target_session!r} is unavailable")
            connection = self._connections.get(target.node_id)
            if connection is None:
                raise BrokerError(f"target node {target.node_id!r} is unavailable")
            delivery = {
                "type": "deliver",
                "id": event.id,
                "source_node": node_id,
                "source_session": source.advertisement.name,
                "source_session_id": source.advertisement.session_id,
                "source_mode": source.advertisement.permission_mode,
                "target_session": target.advertisement.name,
                "body": event.body,
            }
        await connection.send_json(delivery)


def make_process_request(authenticator: TokenAuthenticator):
    def process_request(connection: ServerConnection, request):
        if request.path == "/healthz":
            return connection.respond(HTTPStatus.OK, "OK\n")
        node_id = authenticator.authenticate(request.headers.get("Authorization"))
        if node_id is None:
            return connection.respond(HTTPStatus.UNAUTHORIZED, "Unauthorized\n")
        return None

    return process_request


async def handle_websocket(
    websocket: ServerConnection,
    broker: Broker,
    authenticator: TokenAuthenticator,
) -> None:
    request = websocket.request
    node_id = authenticator.authenticate(
        request.headers.get("Authorization") if request is not None else None
    )
    if node_id is None:
        await websocket.close(code=1008, reason="unauthorized")
        return

    connection = WebSocketConnection(websocket)
    attached = False
    try:
        await broker.attach(node_id, connection)
        attached = True
        await broker.broadcast_presence()
        async for raw in websocket:
            if not isinstance(raw, str):
                await websocket.close(code=1003, reason="text messages required")
                return
            await _handle_client_message(node_id, raw, broker, connection)
    except BrokerError as error:
        await connection.send_json({"type": "error", "message": str(error)})
    finally:
        if attached:
            await broker.detach(node_id)
            await broker.broadcast_presence()


async def _handle_client_message(
    node_id: str,
    raw: str,
    broker: Broker,
    connection: Connection,
) -> None:
    try:
        preview = json.loads(raw)
        message_type = preview.get("type") if isinstance(preview, dict) else None
        if message_type == "publish":
            await broker.publish(node_id, decode_publish_message(raw))
            await broker.broadcast_presence()
            return
        if message_type == "route":
            await broker.route(node_id, decode_client_message(raw))
            return
        raise ProtocolError("unsupported message type")
    except (json.JSONDecodeError, ProtocolError, BrokerError) as error:
        await connection.send_json({"type": "error", "message": str(error)})


async def run_server() -> None:
    authenticator = TokenAuthenticator.from_json(os.environ["DCC_NODE_TOKENS"])
    host = os.environ.get("DCC_SERVER_HOST", "0.0.0.0")
    port = int(os.environ.get("DCC_SERVER_PORT", "8765"))
    broker = Broker()

    async with serve(
        lambda websocket: handle_websocket(websocket, broker, authenticator),
        host,
        port,
        process_request=make_process_request(authenticator),
        max_size=MAX_MESSAGE_BYTES,
        compression=None,
        ping_interval=20,
        ping_timeout=20,
    ) as server:
        await server.serve_forever()


def main() -> None:
    asyncio.run(run_server())
