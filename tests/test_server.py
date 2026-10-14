import pytest

from distributedcc.protocol import PeerEvent, SessionAdvertisement
from distributedcc.server import Broker, BrokerError


class FakeConnection:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    async def send_json(self, message: dict[str, object]) -> None:
        self.messages.append(message)


@pytest.mark.asyncio
async def test_logs_connection_lifecycle_without_secrets(caplog) -> None:
    broker = Broker()

    with caplog.at_level("INFO"):
        await broker.attach("andre", FakeConnection())
        await broker.detach("andre")

    assert "node connected: andre" in caplog.text
    assert "node disconnected: andre" in caplog.text


@pytest.mark.asyncio
async def test_routes_only_from_session_owned_by_connection() -> None:
    broker = Broker()
    andre = FakeConnection()
    friend = FakeConnection()
    await broker.attach("andre", andre)
    await broker.attach("friend", friend)
    await broker.publish(
        "andre",
        [SessionAdvertisement("yrrp-release-manager", "session-manager", "bypass")],
    )
    await broker.publish(
        "friend",
        [SessionAdvertisement("friend-feature", "session-feature", "prompting")],
    )

    event = PeerEvent(
        id="60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
        source_session="friend-feature",
        target_session="yrrp-release-manager",
        body="FEATURE_READY",
    )
    await broker.route("friend", event)

    assert andre.messages[-1]["type"] == "deliver"
    assert andre.messages[-1]["source_node"] == "friend"
    assert andre.messages[-1]["source_session"] == "friend-feature"
    assert andre.messages[-1]["source_mode"] == "prompting"


@pytest.mark.asyncio
async def test_rejects_forged_source_session() -> None:
    broker = Broker()
    friend = FakeConnection()
    await broker.attach("friend", friend)
    await broker.publish(
        "friend",
        [SessionAdvertisement("friend-feature", "session-feature", "prompting")],
    )

    event = PeerEvent(
        id="60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
        source_session="yrrp-release-manager",
        target_session="friend-feature",
        body="run release",
    )

    with pytest.raises(BrokerError, match="does not own"):
        await broker.route("friend", event)


@pytest.mark.asyncio
async def test_rejects_duplicate_global_session_name() -> None:
    broker = Broker()
    await broker.attach("andre", FakeConnection())
    await broker.attach("friend", FakeConnection())
    advertisement = [SessionAdvertisement("shared-name", "one", "prompting")]
    await broker.publish("andre", advertisement)

    with pytest.raises(BrokerError, match="already published"):
        await broker.publish(
            "friend",
            [SessionAdvertisement("shared-name", "two", "prompting")],
        )


@pytest.mark.asyncio
async def test_presence_excludes_callers_local_sessions() -> None:
    broker = Broker()
    await broker.attach("andre", FakeConnection())
    await broker.attach("friend", FakeConnection())
    await broker.publish(
        "andre",
        [SessionAdvertisement("yrrp-release-manager", "manager-id", "bypass")],
    )
    await broker.publish(
        "friend",
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
