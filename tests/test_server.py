import asyncio

import pytest

from distributedcc.protocol import PeerEvent, SessionAdvertisement
from distributedcc.server import Broker, BrokerError


class FakeConnection:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.closed: list[tuple[int, str]] = []

    async def send_json(self, message: dict[str, object]) -> None:
        self.messages.append(message)

    async def close(self, code: int, reason: str) -> None:
        self.closed.append((code, reason))


class FailingSendConnection(FakeConnection):
    async def send_json(self, message: dict[str, object]) -> None:
        raise ConnectionError("target transport closed")


class BlockingCloseConnection(FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.close_started = asyncio.Event()
        self.release_close = asyncio.Event()
        self.close_finished = asyncio.Event()

    async def close(self, code: int, reason: str) -> None:
        self.closed.append((code, reason))
        self.close_started.set()
        try:
            await self.release_close.wait()
        finally:
            self.close_finished.set()


@pytest.mark.asyncio
async def test_logs_connection_lifecycle_without_secrets(caplog) -> None:
    broker = Broker()
    connection = FakeConnection()

    with caplog.at_level("INFO"):
        await broker.attach("andre", connection)
        await broker.detach("andre", connection)

    assert "node connected: andre" in caplog.text
    assert "node disconnected: andre" in caplog.text


@pytest.mark.asyncio
async def test_new_connection_replaces_existing_connection_for_same_node() -> None:
    broker = Broker()
    old = FakeConnection()
    new = FakeConnection()
    await broker.attach("andre", old)
    await broker.publish(
        "andre",
        old,
        [SessionAdvertisement("manager", "old-id", "bypass")],
    )

    await broker.attach("andre", new)
    await asyncio.sleep(0)

    assert old.closed == [(4001, "replaced by newer connection")]
    assert await broker.remote_sessions("friend") == []


@pytest.mark.asyncio
async def test_replacement_activation_does_not_wait_for_old_close() -> None:
    broker = Broker()
    old = BlockingCloseConnection()
    new = FakeConnection()
    await broker.attach("andre", old)

    attach = asyncio.create_task(broker.attach("andre", new))
    await asyncio.wait_for(old.close_started.wait(), timeout=1)
    await asyncio.sleep(0)
    try:
        assert attach.done()
        await broker.publish("andre", new, [])
    finally:
        old.release_close.set()
        await attach
        await asyncio.wait_for(old.close_finished.wait(), timeout=1)


@pytest.mark.asyncio
async def test_old_connection_cannot_mutate_or_detach_replacement() -> None:
    broker = Broker()
    old = FakeConnection()
    new = FakeConnection()
    await broker.attach("andre", old)
    await broker.attach("andre", new)
    await broker.publish(
        "andre",
        new,
        [SessionAdvertisement("manager", "new-id", "bypass")],
    )
    event = PeerEvent(
        id="60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
        source_session="manager",
        source_mode="bypass",
        target_session="friend-feature",
        body="hello",
    )

    with pytest.raises(BrokerError, match="stale connection"):
        await broker.publish("andre", old, [])
    with pytest.raises(BrokerError, match="stale connection"):
        await broker.route("andre", old, event)
    assert await broker.detach("andre", old) is False
    assert await broker.remote_sessions("friend") == [
        {
            "name": "manager",
            "session_id": "new-id",
            "mode": "bypass",
            "node": "andre",
        }
    ]


@pytest.mark.asyncio
async def test_empty_publication_removes_sessions_but_keeps_connection() -> None:
    broker = Broker()
    andre = FakeConnection()
    friend = FakeConnection()
    await broker.attach("andre", andre)
    await broker.attach("friend", friend)
    await broker.publish(
        "andre",
        andre,
        [SessionAdvertisement("manager", "manager-id", "bypass")],
    )

    await broker.publish("andre", andre, [])

    assert await broker.remote_sessions("friend") == []


@pytest.mark.asyncio
async def test_routes_only_from_session_owned_by_connection() -> None:
    broker = Broker()
    andre = FakeConnection()
    friend = FakeConnection()
    await broker.attach("andre", andre)
    await broker.attach("friend", friend)
    await broker.publish(
        "andre",
        andre,
        [SessionAdvertisement("yrrp-release-manager", "session-manager", "bypass")],
    )
    await broker.publish(
        "friend",
        friend,
        [SessionAdvertisement("friend-feature", "session-feature", "prompting")],
    )

    event = PeerEvent(
        id="60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
        source_session="friend-feature",
        source_mode="bypass",
        target_session="yrrp-release-manager",
        body="FEATURE_READY",
    )
    await broker.route("friend", friend, event)

    assert andre.messages[-1]["type"] == "deliver"
    assert andre.messages[-1]["source_node"] == "friend"
    assert andre.messages[-1]["source_session"] == "friend-feature"
    assert andre.messages[-1]["source_mode"] == "bypass"
    assert andre.messages[-1]["target_session_id"] == "session-manager"


@pytest.mark.asyncio
async def test_target_send_failure_does_not_disconnect_source() -> None:
    broker = Broker()
    source = FakeConnection()
    target = FailingSendConnection()
    await broker.attach("source", source)
    await broker.attach("target", target)
    await broker.publish(
        "source",
        source,
        [SessionAdvertisement("source-session", "source-id", "prompting")],
    )
    await broker.publish(
        "target",
        target,
        [SessionAdvertisement("target-session", "target-id", "bypass")],
    )
    event = PeerEvent(
        id="60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
        source_session="source-session",
        source_mode="prompting",
        target_session="target-session",
        body="hello",
    )

    with pytest.raises(BrokerError, match="disconnected during delivery"):
        await broker.route("source", source, event)

    await broker.publish("source", source, [])
    assert await broker.remote_sessions("source") == []


@pytest.mark.asyncio
async def test_rejects_forged_source_session() -> None:
    broker = Broker()
    friend = FakeConnection()
    await broker.attach("friend", friend)
    await broker.publish(
        "friend",
        friend,
        [SessionAdvertisement("friend-feature", "session-feature", "prompting")],
    )

    event = PeerEvent(
        id="60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
        source_session="yrrp-release-manager",
        source_mode="prompting",
        target_session="friend-feature",
        body="run release",
    )

    with pytest.raises(BrokerError, match="does not own"):
        await broker.route("friend", friend, event)


@pytest.mark.asyncio
async def test_rejects_duplicate_global_session_name() -> None:
    broker = Broker()
    andre = FakeConnection()
    friend = FakeConnection()
    await broker.attach("andre", andre)
    await broker.attach("friend", friend)
    advertisement = [SessionAdvertisement("shared-name", "one", "prompting")]
    await broker.publish("andre", andre, advertisement)

    with pytest.raises(BrokerError, match="already published"):
        await broker.publish(
            "friend",
            friend,
            [SessionAdvertisement("shared-name", "two", "prompting")],
        )


@pytest.mark.asyncio
async def test_presence_excludes_callers_local_sessions() -> None:
    broker = Broker()
    andre = FakeConnection()
    friend = FakeConnection()
    await broker.attach("andre", andre)
    await broker.attach("friend", friend)
    await broker.publish(
        "andre",
        andre,
        [SessionAdvertisement("yrrp-release-manager", "manager-id", "bypass")],
    )
    await broker.publish(
        "friend",
        friend,
        [SessionAdvertisement("friend-feature", "feature-id", "prompting")],
    )

    assert await broker.remote_sessions("friend") == [
        {
            "name": "yrrp-release-manager",
            "session_id": "manager-id",
            "mode": "bypass",
            "node": "andre",
        }
    ]
