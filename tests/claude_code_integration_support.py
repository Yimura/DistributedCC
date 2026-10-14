from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import os
import pty
import re
import shutil
import signal
import subprocess
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from aiohttp import web
from websockets.asyncio.server import Server, serve

from distributedcc.bridge import BridgeConfig, run_bridge
from distributedcc.protocol import MAX_MESSAGE_BYTES, PeerEnvelope, PeerEvent, parse_peer_envelope
from distributedcc.registry import LocalSession, discover_live_sessions
from distributedcc.server import (
    Broker,
    Connection,
    TokenAuthenticator,
    handle_websocket,
    make_process_request,
)


@dataclass(frozen=True, slots=True)
class ClaudeLaunchOptions:
    name: str
    session_id: str
    api_key: str
    base_url: str
    permission_mode: str
    allow_send_message: bool
    inbound_accept: bool
    bypass: bool
    deny_proxy_url: str | None = None


class WaitTimeout(TimeoutError):
    def __init__(self, label: str, last_observed: object) -> None:
        super().__init__(f"timed out waiting for {label}; last observed: {last_observed!r}")
        self.label = label
        self.last_observed = last_observed


async def wait_until[T](
    label: str,
    observe: Callable[[], T | Awaitable[T]],
    satisfied: Callable[[T], bool],
    *,
    timeout: float,
    interval: float = 0.05,
) -> T:
    deadline = asyncio.get_running_loop().time() + timeout
    last: T | None = None
    while asyncio.get_running_loop().time() < deadline:
        observed = observe()
        last = await observed if inspect.isawaitable(observed) else observed
        if satisfied(last):
            return last
        await asyncio.sleep(interval)
    raise WaitTimeout(label, last)


SENSITIVE_KEYS = re.compile(
    r"authorization|x-api-key|api_key|token|secret|credential|password|oauth|profile",
    re.IGNORECASE,
)


def redact(value: object) -> object:
    if isinstance(value, dict):
        return {
            str(key): "<redacted>" if SENSITIVE_KEYS.search(str(key)) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    return value


def validate_version(output: str) -> str:
    match = re.match(r"(\d+\.\d+\.\d+)", output.strip())
    if match is None:
        raise AssertionError(f"unexpected Claude Code version output: {output!r}")
    resolved = match.group(1)
    expected = os.environ.get("EXPECTED_CLAUDE_CODE_VERSION", "")
    if expected and resolved != expected:
        raise AssertionError(f"expected Claude Code {expected}, got {resolved}")
    return resolved


class ReadinessServer:
    def __init__(self) -> None:
        self.runner: web.AppRunner | None = None
        self.base_url = ""

    async def __aenter__(self) -> ReadinessServer:
        app = web.Application()
        app.router.add_head("/api/hello", self._readiness)
        app.router.add_route("*", "/{tail:.*}", self._unavailable)
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

    async def _readiness(self, request: web.Request) -> web.Response:
        return web.Response(status=200)

    async def _unavailable(self, request: web.Request) -> web.Response:
        return web.Response(status=503)


class ClaudeProcess:
    def __init__(self, root: Path, options: ClaudeLaunchOptions) -> None:
        self.root = root
        self.options = options
        self.home = root / "home"
        self.config_dir = self.home / ".claude"
        self.sessions_dir = self.config_dir / "sessions"
        self.workspace = root / "workspace"
        self.process: asyncio.subprocess.Process | None = None
        self.reader_tasks: list[asyncio.Task[None]] = []
        self.master_fd: int | None = None
        self.terminal_output = bytearray()

    @staticmethod
    def resolve_version() -> str:
        executable = shutil.which("claude")
        if executable is None:
            raise AssertionError("claude executable not found")
        completed = subprocess.run(
            [executable, "--version"],
            check=True,
            capture_output=True,
            text=True,
        )
        return validate_version(completed.stdout)

    @property
    def returncode(self) -> int | None:
        return None if self.process is None else self.process.returncode

    async def start(self) -> None:
        self.workspace.mkdir(parents=True)
        self.sessions_dir.mkdir(parents=True)
        self._prepare_config()
        master_fd, slave_fd = pty.openpty()
        os.set_blocking(master_fd, False)
        self.master_fd = master_fd
        try:
            self.process = await asyncio.create_subprocess_exec(
                *self._command(),
                cwd=self.workspace,
                env=self._environment(),
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                start_new_session=True,
            )
        finally:
            os.close(slave_fd)
        self.reader_tasks = [asyncio.create_task(self._read_terminal())]
        if self.options.bypass:
            await wait_until(
                "bypass warning",
                self.terminal_text,
                lambda text: "BypassPermissionsmode" in re.sub(r"\s+", "", text),
                timeout=5.0,
            )
            os.write(master_fd, b"\x1b[A\r")

    async def send_user(self, message: str) -> None:
        if self.master_fd is None or self.process is None:
            raise RuntimeError("Claude process is not running")
        if self.process.returncode is not None:
            raise RuntimeError(f"Claude process exited with {self.process.returncode}")
        os.write(self.master_fd, message.encode() + b"\r")

    async def stop(self) -> None:
        if self.process is None:
            return
        await self._wait_or_signal(signal.SIGINT, 2.0)
        await self._wait_or_signal(signal.SIGTERM, 2.0)
        await self._wait_or_signal(signal.SIGKILL, 2.0)
        await asyncio.gather(*self.reader_tasks, return_exceptions=True)
        if self.master_fd is not None:
            os.close(self.master_fd)
            self.master_fd = None

    def session_record(self) -> dict[str, object] | None:
        for path in self.sessions_dir.glob("*.json"):
            try:
                value = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(value, dict) and value.get("sessionId") == self.options.session_id:
                return value
        return None

    def terminal_text(self) -> str:
        decoded = self.terminal_output.decode(errors="replace")
        decoded = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", decoded)
        decoded = re.sub(r"\x1b\][^\x07]*(?:\x07|\x1b\\)", "", decoded)
        return decoded.replace("\r", "")

    def has_result_text(self, expected: str) -> bool:
        expected_compact = re.sub(r"\s+", "", expected).casefold()
        terminal_compact = re.sub(r"\s+", "", self.terminal_text()).casefold()
        return expected_compact in terminal_compact

    def has_denied_tool_result(self, tool_use_id: str) -> bool:
        text = self.terminal_text().lower()
        return tool_use_id.lower() in text and "denied" in text

    def _prepare_config(self) -> None:
        state = {
            "hasCompletedOnboarding": True,
            "lastOnboardingVersion": self.resolve_version(),
            "migrationVersion": 14,
            "theme": "dark",
            "projects": {str(self.workspace): {"hasTrustDialogAccepted": True}},
        }
        (self.config_dir / ".claude.json").write_text(json.dumps(state))
        helper = self.root / "api-key-helper"
        helper.write_text(f"#!/bin/sh\nprintf '%s' '{self.options.api_key}'\n")
        helper.chmod(0o700)

    def _command(self) -> list[str]:
        executable = shutil.which("claude") or "claude"
        helper = self.root / "api-key-helper"
        settings = json.dumps(
            {
                "apiKeyHelper": str(helper),
                "crossSessionInbound": (
                    "accept" if self.options.inbound_accept else "hold"
                ),
                "isolatePeerMachines": True,
            },
            separators=(",", ":"),
        )
        command = [
            executable,
            "--safe-mode",
            "--strict-mcp-config",
            "--mcp-config",
            '{"mcpServers":{}}',
            "--session-id",
            self.options.session_id,
            "--name",
            self.options.name,
            "--tools",
            "SendMessage",
            "--settings",
            settings,
        ]
        if self.options.bypass:
            command.extend(
                ["--allow-dangerously-skip-permissions", "--dangerously-skip-permissions"]
            )
        else:
            command.extend(["--permission-mode", self.options.permission_mode])
        if self.options.allow_send_message:
            command.extend(["--allowedTools", "SendMessage"])
        else:
            command.extend(["--disallowedTools", "SendMessage"])
        return command

    def _environment(self) -> dict[str, str]:
        environment = {
            key: value
            for key, value in os.environ.items()
            if not self._is_sensitive_environment_name(key)
        }
        environment.update(
            {
                "HOME": str(self.home),
                "CLAUDE_CONFIG_DIR": str(self.config_dir),
                "ANTHROPIC_BASE_URL": self.options.base_url,
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                "DISABLE_TELEMETRY": "1",
                "DISABLE_ERROR_REPORTING": "1",
                "DISABLE_UPDATES": "1",
                "DISABLE_AUTOUPDATER": "1",
                "DISABLE_INSTALLATION_CHECKS": "1",
            }
        )
        if self.options.deny_proxy_url is not None:
            environment.update(
                {
                    "HTTP_PROXY": self.options.deny_proxy_url,
                    "HTTPS_PROXY": self.options.deny_proxy_url,
                    "http_proxy": self.options.deny_proxy_url,
                    "https_proxy": self.options.deny_proxy_url,
                    "NO_PROXY": "127.0.0.1,localhost",
                    "no_proxy": "127.0.0.1,localhost",
                }
            )
        return environment

    @staticmethod
    def _is_sensitive_environment_name(name: str) -> bool:
        upper = name.upper()
        sensitive = ("API_KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL")
        providers = ("BEDROCK", "VERTEX", "FOUNDRY", "AWS_", "GOOGLE_")
        routing = ("PROXY", "OAUTH", "PROFILE")
        return upper.startswith(("CLAUDE", "ANTHROPIC")) or any(
            part in upper for part in (*sensitive, *providers, *routing)
        )

    async def _read_terminal(self) -> None:
        assert self.master_fd is not None
        while self.process is not None and self.process.returncode is None:
            try:
                chunk = os.read(self.master_fd, 65536)
            except BlockingIOError:
                await asyncio.sleep(0.01)
                continue
            except OSError:
                break
            if not chunk:
                break
            self.terminal_output.extend(chunk)

    async def _wait_or_signal(self, sig: signal.Signals, timeout: float) -> None:
        assert self.process is not None
        if self.process.returncode is not None:
            return
        try:
            await asyncio.wait_for(self.process.wait(), timeout=timeout)
            return
        except TimeoutError:
            os.killpg(self.process.pid, sig)
        try:
            await asyncio.wait_for(self.process.wait(), timeout=timeout)
        except TimeoutError:
            return


@dataclass(frozen=True, slots=True)
class EgressAttempt:
    method: str
    target: str


class DenyingProxy:
    def __init__(self) -> None:
        self.attempts: list[EgressAttempt] = []
        self.server: asyncio.AbstractServer | None = None
        self.url = ""

    async def __aenter__(self) -> DenyingProxy:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        socket = self.server.sockets[0]
        port = socket.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *args: object) -> None:
        assert self.server is not None
        self.server.close()
        await self.server.wait_closed()

    def assert_only_expected_attempts(self) -> None:
        unexpected = [
            attempt
            for attempt in self.attempts
            if attempt != EgressAttempt("CONNECT", "downloads.claude.ai:443")
        ]
        assert unexpected == []

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            method, target, _ = request.split(b"\r\n", 1)[0].decode().split(" ", 2)
            self.attempts.append(EgressAttempt(method, target))
            writer.write(
                b"HTTP/1.1 502 Bad Gateway\r\n"
                b"Content-Length: 0\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


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


class RecordingBroker(Broker):
    def __init__(self) -> None:
        super().__init__()
        self.route_events: list[PeerEvent] = []

    async def route(
        self,
        node_id: str,
        connection: Connection,
        event: PeerEvent,
    ) -> None:
        await super().route(node_id, connection, event)
        self.route_events.append(event)


def prompting_sender(*, allow_send_message: bool) -> ClaudeLaunchOptions:
    return ClaudeLaunchOptions(
        name="dcc-sender",
        session_id="22222222-2222-4222-8222-222222222222",
        api_key="sender-test-token",
        base_url="",
        permission_mode="dontAsk",
        allow_send_message=allow_send_message,
        inbound_accept=True,
        bypass=False,
    )


def prompting_receiver(
    *,
    inbound_accept: bool,
    allow_send_message: bool = False,
) -> ClaudeLaunchOptions:
    return ClaudeLaunchOptions(
        name="dcc-receiver",
        session_id="33333333-3333-4333-8333-333333333333",
        api_key="receiver-test-token",
        base_url="",
        permission_mode="dontAsk",
        allow_send_message=allow_send_message,
        inbound_accept=inbound_accept,
        bypass=False,
    )


def bypass_sender() -> ClaudeLaunchOptions:
    return dataclasses.replace(
        prompting_sender(allow_send_message=True),
        permission_mode="bypassPermissions",
        bypass=True,
    )


def bypass_receiver(*, inbound_accept: bool) -> ClaudeLaunchOptions:
    return dataclasses.replace(
        prompting_receiver(inbound_accept=inbound_accept),
        permission_mode="bypassPermissions",
        bypass=True,
    )


class IntegrationHarness:
    def __init__(
        self,
        tmp_path: Path,
        artifact_dir: Path,
        *,
        sender: ClaudeLaunchOptions,
        receiver: ClaudeLaunchOptions,
        sender_steps: list[ScriptStep],
        receiver_steps: list[ScriptStep],
    ) -> None:
        self.tmp_path = tmp_path
        self.artifact_dir = artifact_dir
        self.sender_options = sender
        self.receiver_options = receiver
        self.sender_scenario = ScriptedScenario("sender", sender_steps)
        self.receiver_scenario = ScriptedScenario("receiver", receiver_steps)
        self.fake_server = ScriptedAnthropicServer(
            {
                sender.api_key: self.sender_scenario,
                receiver.api_key: self.receiver_scenario,
            }
        )
        self.deny_proxy = DenyingProxy()
        self.broker = RecordingBroker()
        self.broker_server: Server | None = None
        self.bridge_tasks: list[asyncio.Task[None]] = []
        self.sender: ClaudeProcess
        self.receiver: ClaudeProcess
        self.last_satisfied_condition = "not started"

    async def __aenter__(self) -> IntegrationHarness:
        await self.deny_proxy.__aenter__()
        await self.fake_server.__aenter__()
        self.sender = self._make_process("sender", self.sender_options)
        self.receiver = self._make_process("receiver", self.receiver_options)
        try:
            await self._start_broker()
            await self.sender.start()
            await self.receiver.start()
            self._start_bridges()
        except BaseException:
            await self.cleanup()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        if exc is not None:
            self.write_diagnostics(exc)
        errors = await self.cleanup()
        if errors and exc is None:
            raise ExceptionGroup("integration cleanup failed", errors)

    async def wait_ready(self) -> None:
        await self._wait(
            "sender Claude registration",
            self.sender.session_record,
            self._record_has_socket,
        )
        await self._wait(
            "receiver Claude registration",
            self.receiver.session_record,
            self._record_has_socket,
        )
        await self._wait(
            "sender session publication",
            lambda: self.broker.remote_sessions("receiver-node"),
            lambda sessions: any(item["name"] == "dcc-sender" for item in sessions),
        )
        await self._wait(
            "receiver session publication",
            lambda: self.broker.remote_sessions("sender-node"),
            lambda sessions: any(item["name"] == "dcc-receiver" for item in sessions),
        )
        await self._wait(
            "sender remote peer creation",
            lambda: self.virtual_session_names("sender"),
            lambda names: names == {"dcc-receiver"},
        )
        await self._wait(
            "receiver remote peer creation",
            lambda: self.virtual_session_names("receiver"),
            lambda names: names == {"dcc-sender"},
        )

    async def wait_for_completed_round_trip(self) -> None:
        await self.wait_for_route_count(2)
        await self._wait(
            "sender receives reply",
            lambda: len(self.sender_scenario.history),
            lambda count: count >= 3,
        )
        await self._wait(
            "receiver sends reply",
            lambda: len(self.receiver_scenario.history),
            lambda count: count >= 2,
        )
        await self._wait(
            "sender renders completion",
            lambda: self.sender.has_result_text("Round trip complete."),
            bool,
        )
        await self._wait(
            "receiver renders completion",
            lambda: self.receiver.has_result_text("Receiver reply sent."),
            bool,
        )

    async def wait_for_route_count(self, count: int) -> None:
        await self._wait(
            f"broker route count {count}",
            lambda: len(self.broker.route_events),
            lambda observed: observed >= count,
        )

    async def wait_for_sender_continuation(self) -> dict[str, object]:
        record = await self._wait(
            "sender tool continuation",
            lambda: self.sender_scenario.history,
            lambda history: len(history) >= 2,
        )
        return record[-1].body

    async def assert_route_count_stable(self, count: int, *, duration: float) -> None:
        deadline = asyncio.get_running_loop().time() + duration
        while asyncio.get_running_loop().time() < deadline:
            self._raise_if_unhealthy()
            observed = len(self.broker.route_events)
            if observed != count:
                raise AssertionError(f"route count changed from {count} to {observed}")
            await asyncio.sleep(0.05)

    async def assert_receiver_request_count_stable(
        self,
        count: int,
        *,
        duration: float,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + duration
        while asyncio.get_running_loop().time() < deadline:
            self._raise_if_unhealthy()
            observed = len(self.receiver_scenario.history)
            if observed != count:
                raise AssertionError(
                    f"receiver request count changed from {count} to {observed}"
                )
            await asyncio.sleep(0.05)

    def sender_saw_reply(self, body: str) -> bool:
        return any(
            envelope.body == body
            for record in self.sender_scenario.history
            for envelope in peer_envelopes(record.body)
        )

    def receiver_saw_inbound(self, body: str) -> bool:
        return any(
            envelope.body == body
            for record in self.receiver_scenario.history
            for envelope in peer_envelopes(record.body)
        )

    def receiver_peer_request(self) -> dict[str, object]:
        for record in self.receiver_scenario.history:
            if peer_envelopes(record.body):
                return record.body
        raise AssertionError("receiver has no peer request")

    def real_session_names(self, side: str) -> set[str]:
        process = self._process(side)
        return {
            name
            for name, session in discover_live_sessions(process.sessions_dir).items()
            if session.pid == process.process.pid
        }

    def virtual_session_names(self, side: str) -> set[str]:
        process = self._process(side)
        own_pid = process.process.pid
        return {
            name
            for name, session in discover_live_sessions(process.sessions_dir).items()
            if session.pid != own_pid
        }

    def real_session(self, side: str, name: str) -> LocalSession:
        process = self._process(side)
        session = discover_live_sessions(process.sessions_dir)[name]
        if session.pid != process.process.pid:
            raise AssertionError(f"{name} is not the real {side} session")
        return session

    def owned_registry_paths(self) -> list[Path]:
        directories = (self.sender.sessions_dir, self.receiver.sessions_dir)
        return [path for directory in directories for path in directory.glob("*.json")]

    def owned_socket_paths(self) -> list[Path]:
        paths = list((self.tmp_path / "sender-virtual-sockets").glob("*.sock"))
        paths.extend((self.tmp_path / "receiver-virtual-sockets").glob("*.sock"))
        for process in (self.sender, self.receiver):
            if process.process is not None:
                paths.append(Path(f"/run/user/{os.getuid()}/cc-socks/{process.process.pid}.sock"))
        return [path for path in paths if path.exists()]

    async def cleanup(self) -> list[BaseException]:
        errors: list[BaseException] = []
        for task in self.bridge_tasks:
            task.cancel()
        await self._collect_cleanup(
            errors,
            asyncio.gather(*self.bridge_tasks, return_exceptions=True),
        )
        for process in (getattr(self, "sender", None), getattr(self, "receiver", None)):
            if process is not None:
                await self._collect_cleanup(errors, process.stop())
        if self.broker_server is not None:
            self.broker_server.close()
            await self._collect_cleanup(errors, self.broker_server.wait_closed())
        await self._collect_cleanup(errors, self.fake_server.__aexit__(None, None, None))
        await self._collect_cleanup(errors, self.deny_proxy.__aexit__(None, None, None))
        return errors

    def write_diagnostics(self, error: BaseException) -> None:
        payload = {
            "error": repr(error),
            "claude_version": ClaudeProcess.resolve_version(),
            "last_satisfied_condition": self.last_satisfied_condition,
            "processes": {
                "sender": self._process_diagnostic(self.sender),
                "receiver": self._process_diagnostic(self.receiver),
            },
            "fake_api_failures": self.fake_server.failures,
            "fake_api_history": {
                "sender": [record.body for record in self.sender_scenario.history],
                "receiver": [record.body for record in self.receiver_scenario.history],
            },
            "broker_routes": [dataclasses.asdict(event) for event in self.broker.route_events],
            "deny_proxy_attempts": [
                dataclasses.asdict(attempt) for attempt in self.deny_proxy.attempts
            ],
        }
        path = self.artifact_dir / "integration-failure.json"
        path.write_text(json.dumps(redact(payload), indent=2))

    def _make_process(self, side: str, options: ClaudeLaunchOptions) -> ClaudeProcess:
        configured = dataclasses.replace(
            options,
            base_url=self.fake_server.base_url,
            deny_proxy_url=self.deny_proxy.url,
        )
        return ClaudeProcess(self.tmp_path / side, configured)

    async def _start_broker(self) -> None:
        authenticator = TokenAuthenticator(
            {
                "sender-node": "sender-broker-token",
                "receiver-node": "receiver-broker-token",
            }
        )
        self.broker_server = await serve(
            lambda websocket: handle_websocket(websocket, self.broker, authenticator),
            "127.0.0.1",
            0,
            process_request=make_process_request(authenticator),
            max_size=MAX_MESSAGE_BYTES,
            compression=None,
        )
        assert self.broker_server.sockets
        port = self.broker_server.sockets[0].getsockname()[1]
        self.broker_url = f"ws://127.0.0.1:{port}"

    def _start_bridges(self) -> None:
        sender_sockets = self.tmp_path / "sender-virtual-sockets"
        receiver_sockets = self.tmp_path / "receiver-virtual-sockets"
        sender_sockets.mkdir()
        receiver_sockets.mkdir()
        configs = (
            BridgeConfig(
                server_url=self.broker_url,
                token="sender-broker-token",
                publish_names=("dcc-sender",),
                sessions_dir=self.sender.sessions_dir,
                sockets_dir=sender_sockets,
            ),
            BridgeConfig(
                server_url=self.broker_url,
                token="receiver-broker-token",
                publish_names=("dcc-receiver",),
                sessions_dir=self.receiver.sessions_dir,
                sockets_dir=receiver_sockets,
            ),
        )
        self.bridge_tasks = [
            asyncio.create_task(run_bridge(config), name=f"integration-bridge-{index}")
            for index, config in enumerate(configs)
        ]

    async def _wait[T](
        self,
        label: str,
        observe: Callable[[], T | Awaitable[T]],
        satisfied: Callable[[T], bool],
        timeout: float = 15.0,
    ) -> T:
        async def healthy_observe() -> T:
            self._raise_if_unhealthy()
            value = observe()
            return await value if inspect.isawaitable(value) else value

        result = await wait_until(label, healthy_observe, satisfied, timeout=timeout)
        self.last_satisfied_condition = label
        return result

    def _raise_if_unhealthy(self) -> None:
        if self.fake_server.failures:
            raise AssertionError(f"fake API failed: {self.fake_server.failures}")
        for label, process in (("sender", self.sender), ("receiver", self.receiver)):
            if process.returncode is not None:
                raise AssertionError(
                    f"{label} Claude exited with {process.returncode}: "
                    f"{process.terminal_text()[-2000:]}"
                )
        for task in self.bridge_tasks:
            if task.done() and not task.cancelled():
                raise AssertionError(f"{task.get_name()} failed: {task.exception()!r}")

    def _process(self, side: str) -> ClaudeProcess:
        if side == "sender":
            return self.sender
        if side == "receiver":
            return self.receiver
        raise ValueError(f"unknown side: {side}")

    @staticmethod
    def _record_has_socket(record: dict[str, object] | None) -> bool:
        if record is None:
            return False
        socket_path = record.get("messagingSocketPath")
        return isinstance(socket_path, str) and Path(socket_path).is_socket()

    @staticmethod
    async def _collect_cleanup(
        errors: list[BaseException],
        awaitable: Awaitable[object],
    ) -> None:
        try:
            await awaitable
        except BaseException as error:
            errors.append(error)

    @staticmethod
    def _process_diagnostic(process: ClaudeProcess) -> dict[str, object]:
        return {
            "returncode": process.returncode,
            "record": process.session_record(),
            "terminal": process.terminal_text()[-10000:],
        }
