"""Laptop bridge client and local Claude Code event injection."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from distributedcc.protocol import (
    MAX_MESSAGE_BYTES,
    SUPERSEDED_CLOSE_CODE,
    ProtocolError,
    build_peer_envelope,
)
from distributedcc.registry import LocalSession, discover_live_sessions
from distributedcc.virtual_peer import RemoteSession, VirtualPeerSupervisor

SESSION_POLL_SECONDS = 1.0
STABLE_CONNECTION_SECONDS = 30.0

DELIVERY_FIELDS = {
    "type",
    "id",
    "source_node",
    "source_session",
    "source_session_id",
    "source_mode",
    "target_session",
    "target_session_id",
    "body",
}


@dataclass(frozen=True, slots=True)
class Delivery:
    id: str
    source_node: str
    source_session: str
    source_session_id: str
    source_mode: Literal["bypass", "prompting"]
    target_session: str
    target_session_id: str
    body: str


class BridgeSuperseded(RuntimeError):
    """Raised when a newer bridge connection replaces this process."""


def decode_presence(raw: str) -> list[RemoteSession]:
    if len(raw.encode()) > MAX_MESSAGE_BYTES:
        raise ProtocolError("presence message is too large")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ProtocolError("presence message is not valid JSON") from error
    if not isinstance(value, dict) or set(value) != {"type", "sessions"}:
        raise ProtocolError("invalid presence fields")
    if value["type"] != "presence" or not isinstance(value["sessions"], list):
        raise ProtocolError("invalid presence message")

    sessions = []
    for session in value["sessions"]:
        if not isinstance(session, dict) or set(session) != {
            "name",
            "session_id",
            "mode",
            "node",
        }:
            raise ProtocolError("invalid presence session fields")
        if not all(isinstance(item, str) for item in session.values()):
            raise ProtocolError("presence session fields must be strings")
        sessions.append(
            RemoteSession(
                name=session["name"],
                session_id=session["session_id"],
                node=session["node"],
                permission_mode=session["mode"],
            )
        )
    return sessions


def decode_delivery(raw: str) -> Delivery:
    if len(raw.encode()) > MAX_MESSAGE_BYTES:
        raise ProtocolError("delivery is too large")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ProtocolError("delivery is not valid JSON") from error
    if not isinstance(value, dict) or set(value) != DELIVERY_FIELDS:
        raise ProtocolError("invalid delivery fields")
    if value["type"] != "deliver":
        raise ProtocolError("invalid delivery type")
    if not all(isinstance(value[field], str) for field in DELIVERY_FIELDS - {"type"}):
        raise ProtocolError("delivery fields must be strings")
    if value["source_mode"] not in {"bypass", "prompting"}:
        raise ProtocolError("invalid delivery source mode")
    return Delivery(
        id=value["id"],
        source_node=value["source_node"],
        source_session=value["source_session"],
        source_session_id=value["source_session_id"],
        source_mode=value["source_mode"],
        target_session=value["target_session"],
        target_session_id=value["target_session_id"],
        body=value["body"],
    )


async def inject_delivery(
    target: LocalSession,
    delivery: Delivery,
    *,
    source_address: str,
) -> None:
    if target.name != delivery.target_session:
        raise ProtocolError("delivery target does not match local session")
    if target.session_id != delivery.target_session_id:
        raise ProtocolError("delivery target generation does not match local session")
    content = build_peer_envelope(
        address=source_address,
        session_id=delivery.source_session_id,
        name=delivery.source_session,
        mode=delivery.source_mode,
        body=delivery.body,
    )
    frame = {
        "type": "user",
        "message": {"role": "user", "content": content},
        "from": source_address,
        "uuid": delivery.id,
        "msgV": 1,
        "msg_id": delivery.id,
        "priority": "next",
    }
    reader, writer = await asyncio.wait_for(
        asyncio.open_unix_connection(target.socket_path), timeout=5
    )
    del reader
    try:
        writer.write(json.dumps(frame, separators=(",", ":")).encode() + b"\n")
        await asyncio.wait_for(writer.drain(), timeout=5)
    finally:
        writer.close()
        await writer.wait_closed()


@dataclass(frozen=True, slots=True)
class BridgeConfig:
    server_url: str
    token: str
    publish_names: tuple[str, ...]
    sessions_dir: Path
    sockets_dir: Path

    @classmethod
    def from_environment(cls) -> BridgeConfig:
        names = tuple(
            name.strip()
            for name in os.environ["DCC_PUBLISH_SESSIONS"].split(",")
            if name.strip()
        )
        if not names:
            raise ValueError("DCC_PUBLISH_SESSIONS must name at least one session")
        return cls(
            server_url=os.environ["DCC_SERVER_URL"],
            token=os.environ["DCC_TOKEN"],
            publish_names=names,
            sessions_dir=Path(
                os.environ.get("DCC_SESSIONS_DIR", str(Path.home() / ".claude/sessions"))
            ),
            sockets_dir=Path(
                os.environ.get("DCC_SOCKETS_DIR", f"/run/user/{os.getuid()}/cc-socks")
            ),
        )


@dataclass(slots=True)
class LocalSessionState:
    sessions: dict[str, LocalSession]


async def _run_connection_tasks(
    websocket: ClientConnection,
    config: BridgeConfig,
    supervisor: VirtualPeerSupervisor,
) -> None:
    local_state = LocalSessionState({})
    tasks = (
        asyncio.create_task(
            _receive_loop(websocket, config, supervisor, local_state),
            name="dcc-receiver",
        ),
        asyncio.create_task(_outbound_loop(websocket, supervisor), name="dcc-sender"),
        asyncio.create_task(
            _watch_local_sessions(websocket, config, local_state),
            name="dcc-watcher",
        ),
    )
    try:
        _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    for task in pending:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    failures = [
        result
        for result in results
        if isinstance(result, BaseException)
        and not isinstance(result, asyncio.CancelledError)
    ]
    for failure in failures:
        if (
            isinstance(failure, ConnectionClosed)
            and failure.rcvd is not None
            and failure.rcvd.code == SUPERSEDED_CLOSE_CODE
        ):
            raise BridgeSuperseded from failure
    if failures:
        raise failures[0]
    raise ConnectionError("broker connection closed")


def _backoff_after_disconnect(
    delay: float,
    *,
    connected_for: float,
) -> tuple[float, float]:
    retry_delay = 1.0 if connected_for >= STABLE_CONNECTION_SECONDS else delay
    return retry_delay, min(retry_delay * 2, 30.0)


async def run_bridge(config: BridgeConfig) -> None:
    supervisor = VirtualPeerSupervisor(config.sessions_dir, config.sockets_dir)
    delay = 1.0
    try:
        while True:
            connected_at: float | None = None
            try:
                async with connect(
                    config.server_url,
                    additional_headers={"Authorization": f"Bearer {config.token}"},
                    max_size=MAX_MESSAGE_BYTES,
                    compression=None,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    connected_at = asyncio.get_running_loop().time()
                    await _run_connection_tasks(websocket, config, supervisor)
            except asyncio.CancelledError:
                raise
            except BridgeSuperseded:
                logging.error("bridge superseded by newer connection; waiting for restart")
                supervisor.reconcile([])
                await asyncio.Future()
            except Exception as error:
                connected_for = 0.0
                if connected_at is not None:
                    connected_for = asyncio.get_running_loop().time() - connected_at
                retry_delay, delay = _backoff_after_disconnect(
                    delay,
                    connected_for=connected_for,
                )
                logging.warning(
                    "bridge disconnected: %s; reconnecting in %.1fs",
                    error,
                    retry_delay,
                )
                supervisor.reconcile([])
                await asyncio.sleep(retry_delay)
    finally:
        supervisor.stop()


def _discover_selected_sessions(config: BridgeConfig) -> dict[str, LocalSession]:
    discovered = discover_live_sessions(config.sessions_dir)
    return {
        name: discovered[name]
        for name in config.publish_names
        if name in discovered
    }


def _snapshot_key(sessions: dict[str, LocalSession]) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        sorted(
            (session.name, session.session_id, session.permission_mode)
            for session in sessions.values()
        )
    )


async def _watch_local_sessions(
    websocket: ClientConnection,
    config: BridgeConfig,
    local_state: LocalSessionState,
) -> None:
    previous: tuple[tuple[str, str, str], ...] | None = None
    while True:
        sessions = _discover_selected_sessions(config)
        current = _snapshot_key(sessions)
        if current != previous:
            local_state.sessions = sessions
            await _publish_sessions(websocket, sessions)
            previous = current
        await asyncio.sleep(SESSION_POLL_SECONDS)


async def _publish_sessions(
    websocket: ClientConnection,
    sessions: dict[str, LocalSession],
) -> None:
    message = {
        "type": "publish",
        "sessions": [
            {
                "name": session.name,
                "session_id": session.session_id,
                "mode": session.permission_mode,
            }
            for session in sorted(sessions.values(), key=lambda item: item.name)
        ],
    }
    await websocket.send(json.dumps(message, separators=(",", ":")))


async def _receive_loop(
    websocket: ClientConnection,
    config: BridgeConfig,
    supervisor: VirtualPeerSupervisor,
    local_state: LocalSessionState,
) -> None:
    async for raw in websocket:
        if not isinstance(raw, str):
            raise ProtocolError("broker sent binary frame")
        preview = json.loads(raw)
        message_type = preview.get("type") if isinstance(preview, dict) else None
        if message_type == "presence":
            supervisor.reconcile(decode_presence(raw))
            continue
        if message_type == "deliver":
            delivery = decode_delivery(raw)
            published_target = local_state.sessions.get(delivery.target_session)
            target = _discover_selected_sessions(config).get(delivery.target_session)
            if published_target is None or target is None:
                logging.warning(
                    "dropping delivery %s: local target %s is unavailable",
                    delivery.id,
                    delivery.target_session,
                )
                continue
            if (
                published_target.session_id != delivery.target_session_id
                or target.session_id != delivery.target_session_id
            ):
                logging.warning(
                    "dropping delivery %s: local target %s generation is unavailable",
                    delivery.id,
                    delivery.target_session,
                )
                continue
            source_address = supervisor.address_for(delivery.source_session)
            if source_address is None:
                raise ProtocolError("delivery source has no local virtual peer")
            await inject_delivery(target, delivery, source_address=source_address)
            continue
        if message_type == "error":
            message = preview.get("message", "unknown error")
            raise RuntimeError(f"broker rejected request: {message}")
        raise ProtocolError("broker sent unsupported message type")


async def _outbound_loop(
    websocket: ClientConnection,
    supervisor: VirtualPeerSupervisor,
) -> None:
    while True:
        outbound = await asyncio.to_thread(supervisor.next_outbound)
        if outbound is None:
            continue
        event = {
            "type": "route",
            "id": str(uuid.uuid4()),
            "source_session": outbound["source_session"],
            "source_mode": outbound["source_mode"],
            "target_session": outbound["target_session"],
            "body": outbound["body"],
        }
        await websocket.send(json.dumps(event, separators=(",", ":")))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run_bridge(BridgeConfig.from_environment()))
