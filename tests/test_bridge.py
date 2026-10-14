import asyncio
import json
from pathlib import Path

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

import distributedcc.bridge as bridge
from distributedcc.bridge import (
    BridgeConfig,
    Delivery,
    decode_delivery,
    decode_presence,
    inject_delivery,
)
from distributedcc.protocol import parse_peer_envelope
from distributedcc.registry import LocalSession


def make_local_session(
    name: str,
    *,
    session_id: str | None = None,
    mode: str = "bypass",
) -> LocalSession:
    return LocalSession(
        name=name,
        session_id=session_id or f"{name}-id",
        pid=123,
        proc_start="4242",
        socket_path=Path(f"/run/user/1000/cc-socks/{name}.sock"),
        permission_mode=mode,
        status="idle",
    )


def make_bridge_config(tmp_path: Path, *names: str) -> BridgeConfig:
    return BridgeConfig(
        server_url="wss://broker.example.test",
        token="secret",
        publish_names=names,
        sessions_dir=tmp_path / "sessions",
        sockets_dir=tmp_path / "sockets",
    )


def test_discovers_available_subset_without_requiring_every_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live = make_local_session("one", session_id="one-id")
    monkeypatch.setattr(
        bridge,
        "discover_live_sessions",
        lambda _: {"one": live, "unconfigured": make_local_session("unconfigured")},
    )
    config = make_bridge_config(tmp_path, "one", "two")

    assert bridge._discover_selected_sessions(config) == {"one": live}


def test_discovers_empty_subset_when_no_configured_session_is_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bridge, "discover_live_sessions", lambda _: {})

    assert bridge._discover_selected_sessions(make_bridge_config(tmp_path, "manager")) == {}


class RecordingWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))


async def run_watcher_iterations(
    monkeypatch: pytest.MonkeyPatch,
    websocket: RecordingWebSocket,
    config: BridgeConfig,
    snapshots: list[dict[str, LocalSession]],
) -> None:
    remaining = iter(snapshots)
    monkeypatch.setattr(bridge, "_discover_selected_sessions", lambda _: next(remaining))
    sleeps = 0

    async def stop_after_snapshots(_: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == len(snapshots):
            raise asyncio.CancelledError

    monkeypatch.setattr(bridge.asyncio, "sleep", stop_after_snapshots)
    state = bridge.LocalSessionState({})
    with pytest.raises(asyncio.CancelledError):
        await bridge._watch_local_sessions(websocket, config, state)
    assert state.sessions == snapshots[-1]


@pytest.mark.asyncio
async def test_watcher_publishes_empty_then_appearing_session_without_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = RecordingWebSocket()
    session = make_local_session("manager", session_id="manager-id")

    await run_watcher_iterations(
        monkeypatch,
        websocket,
        make_bridge_config(tmp_path, "manager"),
        [{}, {}, {"manager": session}],
    )

    assert websocket.sent == [
        {"type": "publish", "sessions": []},
        {
            "type": "publish",
            "sessions": [
                {"name": "manager", "session_id": "manager-id", "mode": "bypass"}
            ],
        },
    ]


@pytest.mark.asyncio
async def test_watcher_unpublishes_disappearing_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = RecordingWebSocket()
    session = make_local_session("manager")

    await run_watcher_iterations(
        monkeypatch,
        websocket,
        make_bridge_config(tmp_path, "manager"),
        [{"manager": session}, {}],
    )

    assert websocket.sent[-1] == {"type": "publish", "sessions": []}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "replacement",
    [
        make_local_session("manager", session_id="replacement-id"),
        make_local_session("manager", mode="prompting"),
    ],
)
async def test_watcher_republishes_changed_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: LocalSession,
) -> None:
    websocket = RecordingWebSocket()
    original = make_local_session("manager")

    await run_watcher_iterations(
        monkeypatch,
        websocket,
        make_bridge_config(tmp_path, "manager"),
        [{"manager": original}, {"manager": replacement}],
    )

    assert len(websocket.sent) == 2


@pytest.mark.asyncio
async def test_connection_scope_cancels_siblings_when_receiver_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled: set[str] = set()

    async def receiver(*_: object) -> None:
        return

    async def blocked_sender(*_: object) -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.add("sender")
            raise

    async def blocked_watcher(*_: object) -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.add("watcher")
            raise

    monkeypatch.setattr(bridge, "_receive_loop", receiver)
    monkeypatch.setattr(bridge, "_outbound_loop", blocked_sender)
    monkeypatch.setattr(bridge, "_watch_local_sessions", blocked_watcher)

    with pytest.raises(ConnectionError, match="broker connection closed"):
        await bridge._run_connection_tasks(
            RecordingWebSocket(),
            make_bridge_config(tmp_path, "manager"),
            object(),
        )

    assert cancelled == {"sender", "watcher"}


@pytest.mark.asyncio
async def test_connection_scope_retrieves_all_simultaneous_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = asyncio.Event()
    created: list[asyncio.Task[None]] = []
    real_create_task = asyncio.create_task

    async def fail_receiver(*_: object) -> None:
        await release.wait()
        raise RuntimeError("receiver failed")

    async def block_sender(*_: object) -> None:
        release.set()
        await asyncio.Future()

    async def fail_watcher(*_: object) -> None:
        await release.wait()
        raise ValueError("watcher failed")

    def record_task(coro, *, name=None, context=None):
        task = real_create_task(coro, name=name, context=context)
        created.append(task)
        return task

    monkeypatch.setattr(bridge, "_receive_loop", fail_receiver)
    monkeypatch.setattr(bridge, "_outbound_loop", block_sender)
    monkeypatch.setattr(bridge, "_watch_local_sessions", fail_watcher)
    monkeypatch.setattr(bridge.asyncio, "create_task", record_task)

    with pytest.raises((RuntimeError, ValueError)):
        await bridge._run_connection_tasks(
            RecordingWebSocket(),
            make_bridge_config(tmp_path, "manager"),
            object(),
        )

    assert all(not task._log_traceback for task in created)


@pytest.mark.asyncio
async def test_connection_scope_translates_superseded_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def superseded_receiver(*_: object) -> None:
        raise ConnectionClosedError(
            Close(4001, "replaced by newer connection"),
            None,
        )

    async def blocked(*_: object) -> None:
        await asyncio.Future()

    monkeypatch.setattr(bridge, "_receive_loop", superseded_receiver)
    monkeypatch.setattr(bridge, "_outbound_loop", blocked)
    monkeypatch.setattr(bridge, "_watch_local_sessions", blocked)

    with pytest.raises(bridge.BridgeSuperseded):
        await bridge._run_connection_tasks(
            RecordingWebSocket(),
            make_bridge_config(tmp_path, "manager"),
            object(),
        )


@pytest.mark.asyncio
async def test_superseded_bridge_parks_without_reconnecting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    class FakeConnect:
        async def __aenter__(self):
            return RecordingWebSocket()

        async def __aexit__(self, *_: object) -> None:
            return None

    class FakeSupervisor:
        def reconcile(self, _: list[object]) -> None:
            pass

        def stop(self) -> None:
            pass

    def connect_once(*_: object, **__: object) -> FakeConnect:
        nonlocal attempts
        attempts += 1
        return FakeConnect()

    async def superseded(*_: object) -> None:
        raise bridge.BridgeSuperseded

    real_sleep = asyncio.sleep

    async def reject_backoff(_: float) -> None:
        raise AssertionError("superseded bridge must not enter reconnect backoff")

    monkeypatch.setattr(bridge, "connect", connect_once)
    monkeypatch.setattr(bridge, "VirtualPeerSupervisor", lambda *_: FakeSupervisor())
    monkeypatch.setattr(bridge, "_run_connection_tasks", superseded)
    monkeypatch.setattr(bridge.asyncio, "sleep", reject_backoff)
    task = asyncio.create_task(bridge.run_bridge(make_bridge_config(tmp_path, "manager")))
    await real_sleep(0)
    await real_sleep(0)

    assert attempts == 1
    assert not task.done()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class IncomingWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self.messages = messages

    async def __aiter__(self):
        for message in self.messages:
            yield message


@pytest.mark.asyncio
async def test_receive_loop_drops_delivery_when_local_target_disappears(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw = json.dumps(
        {
            "type": "deliver",
            "id": "60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
            "source_node": "friend",
            "source_session": "friend-feature",
            "source_session_id": "friend-session",
            "source_mode": "prompting",
            "target_session": "manager",
            "target_session_id": "manager-id",
            "body": "hello",
        }
    )
    monkeypatch.setattr(bridge, "_discover_selected_sessions", lambda _: {})

    await bridge._receive_loop(
        IncomingWebSocket([raw]),
        make_bridge_config(tmp_path, "manager"),
        object(),
        bridge.LocalSessionState({}),
    )

    assert "local target manager is unavailable" in caplog.text


@pytest.mark.asyncio
async def test_receive_loop_drops_delivery_for_restarted_target_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    published = make_local_session("manager", session_id="session-a")
    restarted = make_local_session("manager", session_id="session-b")
    raw = json.dumps(
        {
            "type": "deliver",
            "id": "60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
            "source_node": "friend",
            "source_session": "friend-feature",
            "source_session_id": "friend-session",
            "source_mode": "prompting",
            "target_session": "manager",
            "target_session_id": "session-a",
            "body": "hello",
        }
    )
    injected = False

    async def record_injection(*_: object, **__: object) -> None:
        nonlocal injected
        injected = True

    monkeypatch.setattr(
        bridge,
        "_discover_selected_sessions",
        lambda _: {"manager": restarted},
    )
    monkeypatch.setattr(bridge, "inject_delivery", record_injection)

    await bridge._receive_loop(
        IncomingWebSocket([raw]),
        make_bridge_config(tmp_path, "manager"),
        object(),
        bridge.LocalSessionState({"manager": published}),
    )

    assert not injected
    assert "local target manager generation is unavailable" in caplog.text


def test_reconnect_backoff_grows_after_early_failure() -> None:
    assert bridge._backoff_after_disconnect(2.0, connected_for=5.0) == (2.0, 4.0)


def test_reconnect_backoff_resets_after_stable_connection() -> None:
    assert bridge._backoff_after_disconnect(16.0, connected_for=30.0) == (1.0, 2.0)


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
            "target_session_id": "manager-session",
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
        target_session_id="manager-session",
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
