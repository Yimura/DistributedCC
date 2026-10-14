from __future__ import annotations

import json
import re
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from aiohttp import web
from claude_code_process import DenyingProxy

from distributedcc.protocol import PeerEnvelope, parse_peer_envelope


def sse(event: str, payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, separators=(",", ":"))
    return f"event: {event}\ndata: {encoded}\n\n"


def event_name(frame: str) -> str:
    return frame.splitlines()[0].removeprefix("event: ")


def message_start(message_id: str) -> str:
    return sse(
        "message_start",
        {
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "model": "claude-opus-5-5",
                "content": [],
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {
                    "input_tokens": 1,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "output_tokens": 0,
                },
            },
        },
    )


def finish_response(stop_reason: str) -> list[str]:
    return [
        sse(
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        sse("message_stop", {"type": "message_stop"}),
    ]


def text_response(text: str) -> list[str]:
    return [
        message_start("msg_text"),
        sse("ping", {"type": "ping"}),
        sse(
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
        ),
        sse(
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            },
        ),
        sse("content_block_stop", {"type": "content_block_stop", "index": 0}),
        *finish_response("end_turn"),
    ]


def tool_response(
    tool_use_id: str,
    name: str,
    tool_input: dict[str, object],
) -> list[str]:
    return [
        message_start(f"msg_{tool_use_id}"),
        sse("ping", {"type": "ping"}),
        sse(
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "tool_use",
                    "id": tool_use_id,
                    "name": name,
                    "input": {},
                },
            },
        ),
        sse(
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": json.dumps(tool_input, separators=(",", ":")),
                },
            },
        ),
        sse("content_block_stop", {"type": "content_block_stop", "index": 0}),
        *finish_response("tool_use"),
    ]


@dataclass(frozen=True, slots=True)
class RequestRecord:
    path: str
    body: dict[str, object]


@dataclass(frozen=True, slots=True)
class ScriptStep:
    label: str
    response: list[str]
    predicate: Callable[[dict[str, object]], bool] = lambda request: True


class ScriptedScenario:
    def __init__(self, name: str, steps: list[ScriptStep]) -> None:
        self.name = name
        self.steps = deque(steps)
        self.history: list[RequestRecord] = []

    def next(self, path: str, body: dict[str, object]) -> list[str]:
        self.history.append(RequestRecord(path, body))
        if not self.steps:
            raise AssertionError(f"{self.name}: unexpected request with no remaining step")
        step = self.steps[0]
        if not step.predicate(body):
            raise AssertionError(f"{self.name}: request failed step {step.label!r}")
        self.steps.popleft()
        return step.response


class ScriptedAnthropicServer:
    def __init__(self, scenarios: dict[str, ScriptedScenario]) -> None:
        self.scenarios = scenarios
        self.failures: list[str] = []
        self.unexpected_requests: list[str] = []
        self.runner: web.AppRunner | None = None
        self.base_url = ""

    async def __aenter__(self) -> ScriptedAnthropicServer:
        app = web.Application()
        app.router.add_head("/api/hello", self.handle_readiness)
        app.router.add_post("/v1/messages/count_tokens", self.handle_count_tokens)
        app.router.add_post("/v1/messages", self.handle_messages)
        app.router.add_route("*", "/{tail:.*}", self.reject_unexpected)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        server = site._server
        assert server is not None and server.sockets
        port = server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *args: object) -> None:
        assert self.runner is not None
        await self.runner.cleanup()

    def _scenario(self, request: web.Request) -> ScriptedScenario:
        key = request.headers.get("x-api-key", "")
        if not key:
            key = request.headers.get("authorization", "").removeprefix("Bearer ")
        if key not in self.scenarios:
            raise web.HTTPUnauthorized()
        return self.scenarios[key]

    async def handle_readiness(self, request: web.Request) -> web.Response:
        return web.Response(status=200)

    async def handle_count_tokens(self, request: web.Request) -> web.Response:
        self._scenario(request)
        await request.json()
        return web.json_response({"input_tokens": 1})

    async def handle_messages(self, request: web.Request) -> web.StreamResponse:
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise AssertionError("Messages request must be a JSON object")
            frames = self._scenario(request).next(request.path, body)
            response = web.StreamResponse(
                status=200,
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                },
            )
            await response.prepare(request)
            for frame in frames:
                await response.write(frame.encode())
            await response.write_eof()
            return response
        except BaseException as error:
            self.failures.append(repr(error))
            raise

    async def reject_unexpected(self, request: web.Request) -> web.Response:
        self.unexpected_requests.append(f"{request.method} {request.path_qs}")
        return web.Response(status=404)

    def assert_clean(self, deny_proxy: DenyingProxy) -> None:
        assert self.failures == []
        assert self.unexpected_requests == []
        deny_proxy.assert_only_expected_attempts()
        incomplete = {
            name: [step.label for step in scenario.steps]
            for name, scenario in self.scenarios.items()
            if scenario.steps
        }
        assert incomplete == {}


def text_blocks(request: dict[str, object]) -> list[str]:
    texts: list[str] = []
    messages = request.get("messages", [])
    if not isinstance(messages, list):
        return texts
    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content", [])
        blocks = [{"type": "text", "text": content}] if isinstance(content, str) else content
        if not isinstance(blocks, list):
            continue
        texts.extend(
            block["text"]
            for block in blocks
            if isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        )
    return texts


def peer_envelopes(request: dict[str, object]) -> list[PeerEnvelope]:
    pattern = re.compile(
        r"<cross-session-message [\s\S]*?</cross-session-message>",
    )
    envelopes: list[PeerEnvelope] = []
    for text in text_blocks(request):
        for match in pattern.finditer(text):
            envelopes.append(parse_peer_envelope(match.group(0)))
    return envelopes


def contains_text(value: str) -> Callable[[dict[str, object]], bool]:
    return lambda request: any(value in text for text in text_blocks(request))


def find_tool_result(
    request: dict[str, object],
    tool_use_id: str,
) -> dict[str, object]:
    messages = request.get("messages", [])
    if not isinstance(messages, list):
        raise AssertionError("Messages request has no message list")
    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content", [])
        if not isinstance(content, list):
            continue
        for block in content:
            if (
                isinstance(block, dict)
                and block.get("type") == "tool_result"
                and block.get("tool_use_id") == tool_use_id
            ):
                return block
    raise AssertionError(f"missing tool_result for {tool_use_id}")


def contains_tool_result(tool_use_id: str) -> Callable[[dict[str, object]], bool]:
    def predicate(request: dict[str, object]) -> bool:
        try:
            find_tool_result(request, tool_use_id)
        except AssertionError:
            return False
        return True

    return predicate


def sender_round_trip_steps() -> list[ScriptStep]:
    return [
        ScriptStep(
            "sender kickoff",
            tool_response(
                "toolu_sender_send",
                "SendMessage",
                {
                    "to": "dcc-receiver",
                    "summary": "Send integration ping",
                    "message": "INTEGRATION_PING",
                },
            ),
            contains_text("Start the DistributedCC round-trip test."),
        ),
        ScriptStep(
            "sender send result",
            text_response("Waiting for receiver reply."),
            contains_tool_result("toolu_sender_send"),
        ),
        ScriptStep(
            "sender receives reply",
            text_response("Round trip complete."),
            lambda request: any(
                envelope.name == "dcc-receiver"
                and envelope.body == "INTEGRATION_REPLY"
                for envelope in peer_envelopes(request)
            ),
        ),
    ]


def tool_result_is_denied(
    request: dict[str, object],
    tool_use_id: str,
) -> bool:
    return find_tool_result(request, tool_use_id).get("is_error") is True


def denial_steps() -> list[ScriptStep]:
    return [
        ScriptStep(
            "sender requests denied send",
            tool_response(
                "toolu_denied_send",
                "SendMessage",
                {
                    "to": "dcc-receiver",
                    "summary": "Attempt denied send",
                    "message": "DENIED_MESSAGE",
                },
            ),
            contains_text("Attempt the denied integration send."),
        ),
        ScriptStep(
            "sender reports denial",
            text_response("Send denied."),
            contains_tool_result("toolu_denied_send"),
        ),
    ]


def held_message_steps() -> list[ScriptStep]:
    return [
        ScriptStep(
            "sender routes held message",
            tool_response(
                "toolu_held_send",
                "SendMessage",
                {
                    "to": "dcc-receiver",
                    "summary": "Send held message",
                    "message": "HELD_MESSAGE",
                },
            ),
            contains_text("Start the held-message test."),
        ),
        ScriptStep(
            "sender records held send result",
            text_response("Held-message route attempted."),
            contains_tool_result("toolu_held_send"),
        ),
    ]


def receiver_round_trip_steps(source_mode: str = "prompting") -> list[ScriptStep]:
    return [
        ScriptStep(
            "receiver receives ping",
            tool_response(
                "toolu_receiver_reply",
                "SendMessage",
                {
                    "to": "dcc-sender",
                    "summary": "Reply to integration sender",
                    "message": "INTEGRATION_REPLY",
                },
            ),
            lambda request: any(
                envelope.name == "dcc-sender"
                and envelope.mode == source_mode
                and envelope.body == "INTEGRATION_PING"
                for envelope in peer_envelopes(request)
            ),
        ),
        ScriptStep(
            "receiver reply result",
            text_response("Receiver reply sent."),
            contains_tool_result("toolu_receiver_reply"),
        ),
    ]


