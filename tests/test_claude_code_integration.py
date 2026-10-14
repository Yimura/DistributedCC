import json
import os
from pathlib import Path

import aiohttp
import claude_code_process as process_module
import pytest
from claude_code_fake_api import (
    ScriptedAnthropicServer,
    ScriptedScenario,
    ScriptStep,
    denial_steps,
    event_name,
    held_message_steps,
    peer_envelopes,
    receiver_round_trip_steps,
    sender_round_trip_steps,
    text_response,
    tool_response,
    tool_result_is_denied,
)
from claude_code_integration_support import (
    IntegrationHarness,
    bypass_receiver,
    bypass_sender,
    prompting_receiver,
    prompting_sender,
)
from claude_code_process import (
    ClaudeLaunchOptions,
    ClaudeProcess,
    DenyingProxy,
    EgressAttempt,
    ReadinessServer,
    redact,
    validate_version,
    wait_until,
)


@pytest.mark.claude_code_integration
def test_installed_cli_matches_expected_version() -> None:
    ClaudeProcess.resolve_version()


def test_expected_version_requires_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXPECTED_CLAUDE_CODE_VERSION", "2.1.296")

    assert validate_version("2.1.296 (Claude Code)") == "2.1.296"
    with pytest.raises(AssertionError, match="2.1.296"):
        validate_version("2.1.297 (Claude Code)")


def test_latest_channel_records_version_without_repinning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EXPECTED_CLAUDE_CODE_VERSION", "")

    assert validate_version("2.2.0 (Claude Code)") == "2.2.0"


@pytest.mark.claude_code_integration
async def test_persistent_stream_process_registers_real_session(tmp_path: Path) -> None:
    async with ReadinessServer() as api:
        process = ClaudeProcess(
            root=tmp_path / "receiver",
            options=ClaudeLaunchOptions(
                name="dcc-contract-receiver",
                session_id="11111111-1111-4111-8111-111111111111",
                api_key="integration-contract-token",
                base_url=api.base_url,
                permission_mode="dontAsk",
                allow_send_message=False,
                inbound_accept=False,
            ),
        )
        await process.start()
        try:
            record = await wait_until(
                "Claude session registration",
                process.session_record,
                lambda value: value is not None,
                timeout=10.0,
            )
            assert record is not None
            assert record["name"] == "dcc-contract-receiver"
            assert record["sessionId"] == process.options.session_id
            assert record["version"] == ClaudeProcess.resolve_version()
            assert record["peerProtocol"] == 1
            assert Path(record["messagingSocketPath"]).is_socket()
            assert process.returncode is None
        finally:
            await process.stop()


def test_text_response_has_anthropic_sse_order() -> None:
    frames = text_response("receiver complete")

    assert [event_name(frame) for frame in frames] == [
        "message_start",
        "ping",
        "content_block_start",
        "content_block_delta",
        "content_block_stop",
        "message_delta",
        "message_stop",
    ]
    assert '"stop_reason":"end_turn"' in frames[-2]


def test_tool_response_streams_parsed_json_input() -> None:
    frames = tool_response(
        "toolu_receiver_reply",
        "SendMessage",
        {
            "to": "dcc-sender",
            "summary": "Reply to integration sender",
            "message": "INTEGRATION_REPLY",
        },
    )

    assert [event_name(frame) for frame in frames] == [
        "message_start",
        "ping",
        "content_block_start",
        "content_block_delta",
        "content_block_stop",
        "message_delta",
        "message_stop",
    ]
    delta = json.loads(frames[3].split("data: ", 1)[1])
    assert json.loads(delta["delta"]["partial_json"])["to"] == "dcc-sender"
    assert '"stop_reason":"tool_use"' in frames[-2]


async def test_denying_proxy_records_and_rejects_egress() -> None:
    async with DenyingProxy() as proxy, aiohttp.ClientSession() as client:
        response = await client.get("http://example.invalid/", proxy=proxy.url)

    assert response.status == 502
    assert proxy.attempts == [EgressAttempt("GET", "http://example.invalid/")]


async def test_scripted_server_selects_scenario_by_dummy_api_key() -> None:
    sender = ScriptedScenario(
        "sender",
        [
            ScriptStep(
                "sender initial request",
                tool_response(
                    "toolu_sender_send",
                    "SendMessage",
                    {
                        "to": "dcc-receiver",
                        "summary": "Send integration ping",
                        "message": "INTEGRATION_PING",
                    },
                ),
            )
        ],
    )
    async with (
        ScriptedAnthropicServer({"sender-test-token": sender}) as server,
        aiohttp.ClientSession() as client,
    ):
        response = await client.post(
            f"{server.base_url}/v1/messages",
            headers={"x-api-key": "sender-test-token"},
            json={"model": "test-model", "stream": True, "messages": []},
        )
        body = await response.text()

    assert response.status == 200
    assert response.headers["Content-Type"].startswith("text/event-stream")
    assert "toolu_sender_send" in body
    assert sender.history[0].path == "/v1/messages"


@pytest.fixture
def integration_artifact_dir(tmp_path: Path) -> Path:
    configured = os.environ.get("DCC_INTEGRATION_ARTIFACT_DIR")
    path = Path(configured) if configured else tmp_path / "diagnostics"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.mark.claude_code_integration
async def test_two_claude_sessions_become_remote_peers(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with IntegrationHarness(
        tmp_path,
        integration_artifact_dir,
        sender=prompting_sender(allow_send_message=True),
        receiver=prompting_receiver(inbound_accept=True),
        sender_steps=[],
        receiver_steps=[],
    ) as harness:
        await harness.wait_ready()

        assert harness.real_session_names("sender") == {"dcc-sender"}
        assert harness.real_session_names("receiver") == {"dcc-receiver"}
        assert harness.virtual_session_names("sender") == {"dcc-receiver"}
        assert harness.virtual_session_names("receiver") == {"dcc-sender"}
        assert harness.sender.returncode is None
        assert harness.receiver.returncode is None
        harness.assert_egress_policy()


@pytest.mark.claude_code_integration
async def test_explicit_allow_round_trip(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with IntegrationHarness(
        tmp_path,
        integration_artifact_dir,
        sender=prompting_sender(allow_send_message=True),
        receiver=prompting_receiver(
            inbound_accept=True,
            allow_send_message=True,
        ),
        sender_steps=sender_round_trip_steps(),
        receiver_steps=receiver_round_trip_steps(),
    ) as harness:
        await harness.wait_ready()
        await harness.sender.send_user("Start the DistributedCC round-trip test.")
        await harness.wait_for_completed_round_trip()

        assert [
            (event.source_session, event.target_session, event.source_mode, event.body)
            for event in harness.broker.route_events
        ] == [
            ("dcc-sender", "dcc-receiver", "prompting", "INTEGRATION_PING"),
            ("dcc-receiver", "dcc-sender", "prompting", "INTEGRATION_REPLY"),
        ]
        assert harness.sender_saw_reply("INTEGRATION_REPLY")
        assert harness.receiver_saw_inbound("INTEGRATION_PING")
        assert harness.sender.has_result_text("Round trip complete.")
        assert harness.receiver.has_result_text("Receiver reply sent.")
        harness.assert_clean()


@pytest.mark.claude_code_integration
async def test_dont_ask_denies_disallowed_send_message(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with IntegrationHarness(
        tmp_path,
        integration_artifact_dir,
        sender=prompting_sender(allow_send_message=False),
        receiver=prompting_receiver(inbound_accept=True),
        sender_steps=denial_steps(),
        receiver_steps=[],
    ) as harness:
        await harness.wait_ready()
        route_count = len(harness.broker.route_events)
        await harness.sender.send_user("Attempt the denied integration send.")
        continuation = await harness.wait_for_sender_continuation()

        assert tool_result_is_denied(continuation, "toolu_denied_send")
        await harness.assert_route_count_stable(route_count, duration=1.0)
        assert not harness.receiver_saw_inbound("DENIED_MESSAGE")
        assert harness.sender.has_result_text("Send denied.")
        harness.assert_egress_policy()


@pytest.mark.claude_code_integration
async def test_bypass_mode_propagates_through_broker(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with IntegrationHarness(
        tmp_path,
        integration_artifact_dir,
        sender=bypass_sender(),
        receiver=prompting_receiver(
            inbound_accept=True,
            allow_send_message=True,
        ),
        sender_steps=sender_round_trip_steps(),
        receiver_steps=receiver_round_trip_steps("bypass"),
    ) as harness:
        await harness.wait_ready()
        assert harness.real_session("sender", "dcc-sender").permission_mode == "bypass"
        await harness.sender.send_user("Start the DistributedCC round-trip test.")
        await harness.wait_for_completed_round_trip()

        route = harness.broker.route_events[0]
        assert route.source_mode == "bypass"
        envelope = next(
            envelope
            for envelope in peer_envelopes(harness.receiver_peer_request())
            if envelope.name == "dcc-sender"
        )
        assert envelope.mode == "bypass"
        assert envelope.body == "INTEGRATION_PING"
        harness.assert_clean()


@pytest.mark.claude_code_integration
async def test_receiver_without_accept_holds_mismatched_inbound_message(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with IntegrationHarness(
        tmp_path,
        integration_artifact_dir,
        sender=prompting_sender(allow_send_message=True),
        receiver=bypass_receiver(inbound_accept=False),
        sender_steps=held_message_steps(),
        receiver_steps=[],
    ) as harness:
        await harness.wait_ready()
        receiver_requests = len(harness.receiver_scenario.history)
        await harness.sender.send_user("Start the held-message test.")
        await harness.wait_for_route_count(1)
        await harness.wait_for_sender_continuation()

        route = harness.broker.route_events[0]
        assert route.source_mode == "prompting"
        assert route.target_session == "dcc-receiver"
        await harness.assert_receiver_request_count_stable(
            receiver_requests,
            duration=1.5,
        )
        assert not harness.receiver_saw_inbound("HELD_MESSAGE")
        assert harness.receiver.returncode is None
        harness.assert_clean()


def test_diagnostics_redact_all_dummy_credentials() -> None:
    diagnostic = redact(
        {
            "authorization": "Bearer broker-secret",
            "headers": {"x-api-key": "sender-test-token"},
            "nested": {"token": "receiver-broker-token"},
            "terminal": "failed while using sender-test-token",
            "error": "broker-secret was rejected",
            "message": "INTEGRATION_PING",
        }
    )
    serialized = json.dumps(diagnostic)

    assert "broker-secret" not in serialized
    assert "sender-test-token" not in serialized
    assert "receiver-broker-token" not in serialized
    assert "INTEGRATION_PING" in serialized


async def test_spawn_failure_closes_pty_master(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptors: dict[str, int] = {}
    real_openpty = process_module.pty.openpty

    def tracked_openpty() -> tuple[int, int]:
        master_fd, slave_fd = real_openpty()
        descriptors["master"] = master_fd
        return master_fd, slave_fd

    async def fail_spawn(*args: object, **kwargs: object) -> None:
        raise OSError("forced spawn failure")

    monkeypatch.setattr(process_module.pty, "openpty", tracked_openpty)
    monkeypatch.setattr(process_module.asyncio, "create_subprocess_exec", fail_spawn)
    process = ClaudeProcess(
        tmp_path / "spawn-failure",
        ClaudeLaunchOptions(
            name="spawn-failure",
            session_id="44444444-4444-4444-8444-444444444444",
            api_key="spawn-token",
            base_url="http://127.0.0.1:9",
            permission_mode="dontAsk",
            allow_send_message=False,
            inbound_accept=False,
        ),
    )

    with pytest.raises(OSError, match="forced spawn failure"):
        await process.start()
    with pytest.raises(OSError):
        os.fstat(descriptors["master"])


async def test_setup_failure_cleans_resources_and_writes_diagnostics(
    tmp_path: Path,
    integration_artifact_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_server_start(server: ScriptedAnthropicServer) -> None:
        raise OSError("forced API startup failure")

    monkeypatch.setattr(ScriptedAnthropicServer, "__aenter__", fail_server_start)
    harness = IntegrationHarness(
        tmp_path,
        integration_artifact_dir,
        sender=prompting_sender(allow_send_message=True),
        receiver=prompting_receiver(inbound_accept=True),
        sender_steps=[],
        receiver_steps=[],
    )

    with pytest.raises(OSError, match="forced API startup failure"):
        async with harness:
            pytest.fail("setup failure should prevent context entry")

    assert harness.deny_proxy.server is not None
    assert not harness.deny_proxy.server.is_serving()
    diagnostics = list(integration_artifact_dir.glob("*.json"))
    assert len(diagnostics) == 1
    assert "forced API startup failure" in diagnostics[0].read_text()


@pytest.mark.claude_code_integration
async def test_failure_writes_diagnostics_and_cleans_owned_resources(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    harness = IntegrationHarness(
        tmp_path,
        integration_artifact_dir,
        sender=prompting_sender(allow_send_message=True),
        receiver=prompting_receiver(inbound_accept=True),
        sender_steps=[],
        receiver_steps=[],
    )

    with pytest.raises(AssertionError, match="forced harness failure"):
        async with harness:
            await harness.wait_ready()
            raise AssertionError("forced harness failure")

    assert harness.sender.returncode is not None
    assert harness.receiver.returncode is not None
    assert all(task.done() for task in harness.bridge_tasks)
    assert harness.owned_registry_paths() == []
    assert harness.owned_socket_paths() == []
    diagnostics = list(integration_artifact_dir.glob("*.json"))
    assert len(diagnostics) == 1
    payload = json.loads(diagnostics[0].read_text())
    assert payload["last_satisfied_condition"] == "receiver remote peer creation"
