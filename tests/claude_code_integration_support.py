from __future__ import annotations

import asyncio
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
        return expected in self.terminal_text()

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
            command.append("--dangerously-skip-permissions")
        else:
            command.extend(["--permission-mode", self.options.permission_mode])
        if self.options.allow_send_message:
            command.extend(["--allowedTools", "SendMessage"])
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
        step = self.steps.popleft()
        if not step.predicate(body):
            raise AssertionError(f"{self.name}: request failed step {step.label!r}")
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
        assert deny_proxy.attempts == []
        incomplete = {
            name: [step.label for step in scenario.steps]
            for name, scenario in self.scenarios.items()
            if scenario.steps
        }
        assert incomplete == {}
