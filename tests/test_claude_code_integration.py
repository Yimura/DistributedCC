import json
import os
import re
from pathlib import Path

import aiohttp
import pytest
from claude_code_integration_support import (
    ClaudeLaunchOptions,
    ClaudeProcess,
    DenyingProxy,
    EgressAttempt,
    ReadinessServer,
    ScriptedAnthropicServer,
    ScriptedScenario,
    ScriptStep,
    event_name,
    text_response,
    tool_response,
    validate_version,
    wait_until,
)


@pytest.mark.claude_code_integration
def test_installed_cli_matches_expected_version() -> None:
    resolved = ClaudeProcess.resolve_version()
    expected = os.environ.get("EXPECTED_CLAUDE_CODE_VERSION", "")

    assert re.fullmatch(r"\d+\.\d+\.\d+", resolved)
    if expected:
        assert resolved == expected


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
                bypass=False,
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


@pytest.mark.asyncio
async def test_denying_proxy_records_and_rejects_egress() -> None:
    async with DenyingProxy() as proxy, aiohttp.ClientSession() as client:
        response = await client.get("http://example.invalid/", proxy=proxy.url)

    assert response.status == 502
    assert proxy.attempts == [EgressAttempt("GET", "http://example.invalid/")]


@pytest.mark.asyncio
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
