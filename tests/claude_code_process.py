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


