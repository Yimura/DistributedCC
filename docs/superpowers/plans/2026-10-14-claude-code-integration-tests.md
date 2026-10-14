# Claude Code Integration Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic integration coverage that drives two persistent real Claude Code processes through a fake local Anthropic API and real DistributedCC routing, including permission behavior and CI compatibility checks.

**Architecture:** A test-only `aiohttp` server emits scripted Anthropic Messages SSE responses selected by per-process dummy API keys. Two persistent Claude Code subprocesses use isolated homes and configuration while a real in-process broker and two real bridges exercise registry, Unix-socket, virtual-peer, and WebSocket behavior. One cleanup stack owns every process, task, server, socket, and diagnostic.

**Tech Stack:** Python 3.12+, asyncio, aiohttp, websockets 16.0, pytest, pytest-asyncio, uv, Claude Code CLI 2.1.296, GitHub Actions.

---

## Implementation constraints

- Keep production modules unchanged unless the early real-CLI contract test proves the approved architecture cannot work.
- Use real Claude Code registry records and sockets. Never synthesize them for the two CLI processes.
- Use `--dangerously-skip-permissions` only for the isolated bypass scenario because `infer_permission_mode()` detects that exact argument.
- Restrict every CLI process to `SendMessage`; expose no Bash, file, web, or MCP tools.
- Use dummy credentials and loopback endpoints only. Strip inherited provider credentials. Replace inherited HTTP(S) proxy settings with a fixture-owned loopback deny proxy, set `NO_PROXY` only for `127.0.0.1` and `localhost`, reject every proxy request, and assert zero attempts after every successful scenario.
- Preserve parsed-object assertions. Do not compare complete serialized requests or complete system prompts.
- Treat `@latest` as a non-blocking compatibility probe. Never change `TESTED_CLAUDE_CODE_VERSION` implicitly.
- Follow `AI_Agent_Security_Cheat_Sheet#1-tool-security--least-privilege` and redact diagnostics according to ASVS 5.0.0 V16.2.5.

## File map

- Create `tests/claude_code_integration_support.py`: SSE builders, fake API, CLI process wrapper, broker/bridge harness, waits, cleanup, diagnostics.
- Create `tests/test_claude_code_integration.py`: helper contract tests and marked real-CLI scenarios.
- Modify `pyproject.toml`: add `aiohttp`, register marker, exclude integration tests by default.
- Modify `uv.lock`: lock new test dependency.
- Modify `.gitignore`: ignore `.artifacts/`.
- Create `.github/workflows/tests.yml`: normal test matrix and quality checks.
- Create `.github/workflows/claude-code-integration.yml`: pinned blocking and latest non-blocking integration jobs.

## Execution findings

Implementation validated the early CLI checkpoint and took its documented PTY branch:

- print-stream mode registers without `messagingSocketPath`;
- interactive `--bare` mode also omits the peer socket;
- non-bare PTY mode creates the native socket after isolated onboarding, workspace trust, and API-key-helper setup;
- inherited `CLAUDE_CODE_CHILD_SESSION` and related parent markers must be stripped;
- bypass mode requires deterministic acceptance of its safety warning before registration;
- native `SendMessage` is safe by default, so denial coverage uses explicit `--disallowedTools SendMessage`;
- interactive mode attempts model-catalog connections to `downloads.claude.ai`; the fixture-owned proxy blocks and records them;
- support code split into `claude_code_process.py`, `claude_code_fake_api.py`, and `claude_code_integration_support.py` to keep each file focused;
- normal CI uses only Python 3.14, matching the project container instead of maintaining a redundant interpreter matrix.

## Known assumptions to validate first

1. `claude -p --input-format stream-json` must remain alive and create a real registry record while stdin stays open. If not, switch the fixture to a PTY-backed interactive process before building the rest.
2. Receiver replies require a scripted `SendMessage` tool call. Plain assistant text does not traverse DistributedCC.
3. Complete kernel-level egress isolation is unavailable on generic GitHub-hosted runners. The harness strips credentials, disables nonessential traffic, uses loopback, and fails unexpected HTTP requests.
4. Untested Claude versions are warned about rather than rejected. Latest-job diagnostics should report `unverified_registry_version`, not claim registry rejection.

---

### Task 1: Register integration boundary and dependencies

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `.gitignore`

- [ ] **Step 1: Verify marker is absent**

Run:

```bash
uv run --extra dev pytest --markers | grep claude_code_integration
```

Expected: exit `1`; no marker appears.

- [ ] **Step 2: Add dev dependency and pytest configuration**

Update `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = [
    "aiohttp>=3.13,<4",
    "pytest>=8.4,<10",
    "pytest-asyncio>=1.2,<2",
    "ruff>=0.14,<1",
]

[tool.pytest.ini_options]
addopts = [
    "--strict-config",
    "--strict-markers",
    "-m",
    "not claude_code_integration",
]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = [
    "claude_code_integration: requires a real Claude Code CLI and Linux peer messaging",
]
```

Preserve any dependency bounds already newer or narrower than these.

- [ ] **Step 3: Ignore failure artifacts**

Add to `.gitignore`:

```gitignore
.artifacts/
```

- [ ] **Step 4: Regenerate lock and verify marker**

Run:

```bash
uv lock
uv lock --check
uv run --extra dev pytest --markers | grep claude_code_integration
uv run --extra dev pytest -q
```

Expected: lock checks pass, marker prints, and normal suite passes without launching `claude`.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock .gitignore
git commit -m "test: configure Claude integration suite"
```

---

### Task 2: Prove persistent Claude CLI contract

**Files:**
- Create: `tests/claude_code_integration_support.py`
- Create: `tests/test_claude_code_integration.py`

- [ ] **Step 1: Write failing contract tests**

Create `tests/test_claude_code_integration.py`:

```python
import os
import re
from pathlib import Path

import pytest

from claude_code_integration_support import (
    ClaudeLaunchOptions,
    ClaudeProcess,
    ReadinessServer,
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


def test_expected_version_requires_exact_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
async def test_persistent_stream_process_registers_real_session(
    tmp_path: Path,
) -> None:
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
            assert record["name"] == "dcc-contract-receiver"
            assert record["sessionId"] == process.options.session_id
            assert record["version"] == ClaudeProcess.resolve_version()
            assert record["peerProtocol"] == 1
            assert Path(record["messagingSocketPath"]).is_socket()
            assert process.returncode is None
        finally:
            await process.stop()
```

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py -q
```

Expected: collection fails because `claude_code_integration_support` does not exist.

- [ ] **Step 3: Implement launch options and deadline wait**

Create `tests/claude_code_integration_support.py` with these interfaces:

```python
from __future__ import annotations

import asyncio
import inspect
import os
import re
from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

from aiohttp import web

T = TypeVar("T")


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
        app.router.add_head("/api/hello", lambda request: web.Response(status=200))
        app.router.add_route("*", "/{tail:.*}", lambda request: web.Response(status=503))
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


class WaitTimeout(TimeoutError):
    def __init__(self, label: str, last_observed: object) -> None:
        super().__init__(
            f"timed out waiting for {label}; last observed: {last_observed!r}"
        )
        self.label = label
        self.last_observed = last_observed


async def wait_until(
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
```

- [ ] **Step 4: Implement minimal `ClaudeProcess`**

Build command from:

```python
command = [
    shutil.which("claude") or "claude",
    "-p",
    "--bare",
    "--strict-mcp-config",
    "--mcp-config",
    "{}",
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    "--verbose",
    "--session-id",
    options.session_id,
    "--name",
    options.name,
    "--permission-prompts",
    "none",
    "--tools",
    "SendMessage",
]
```

Append `--permission-mode`, append `--allowedTools SendMessage` only when allowed, append `--dangerously-skip-permissions` only for bypass, and pass one session-scoped setting:

```python
settings = {
    "crossSessionInbound": "accept" if options.inbound_accept else "hold",
    "isolatePeerMachines": True,
}
```

Build isolated environment from a cleaned copy of `os.environ`. Remove names containing `API_KEY`, `TOKEN`, `SECRET`, `PASSWORD`, or `CREDENTIAL`, plus Anthropic profile, OAuth, proxy, Bedrock, Vertex, and Foundry variables. Then add:

```python
{
    "HOME": str(home),
    "CLAUDE_CONFIG_DIR": str(config),
    "ANTHROPIC_API_KEY": options.api_key,
    "ANTHROPIC_BASE_URL": options.base_url,
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
    "DISABLE_TELEMETRY": "1",
    "DISABLE_ERROR_REPORTING": "1",
    "DISABLE_UPDATES": "1",
    "DISABLE_AUTOUPDATER": "1",
    "DISABLE_INSTALLATION_CHECKS": "1",
}
if options.deny_proxy_url is not None:
    environment.update(
        {
            "HTTP_PROXY": options.deny_proxy_url,
            "HTTPS_PROXY": options.deny_proxy_url,
            "http_proxy": options.deny_proxy_url,
            "https_proxy": options.deny_proxy_url,
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
        }
    )
```

Start with:

```python
self.process = await asyncio.create_subprocess_exec(
    *command,
    cwd=self.workspace,
    env=environment,
    stdin=asyncio.subprocess.PIPE,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    start_new_session=True,
)
assert self.process.stdout is not None
assert self.process.stderr is not None
self.reader_tasks = [
    asyncio.create_task(self._read_stdout(self.process.stdout)),
    asyncio.create_task(self._read_stderr(self.process.stderr)),
]
```

Parse valid stdout JSONL and preserve invalid lines for diagnostics. Implement `session_record()` by reading this process's isolated registry directory. Add structured-output helpers:

```python
def has_result_text(self, expected: str) -> bool:
    return any(
        event.get("type") == "result"
        and isinstance(event.get("result"), str)
        and expected in event["result"]
        for event in self.structured_output
    )


def has_denied_tool_result(self, tool_use_id: str) -> bool:
    return any(
        event.get("type") == "tool_result"
        and event.get("tool_use_id") == tool_use_id
        and event.get("is_error") is True
        for event in self.structured_output
    )
```

If pinned CLI emits denial inside another documented stream-json event, capture that exact parsed shape in the RED run and update only this structural matcher; never match raw human-readable stderr.

- [ ] **Step 5: Implement cleanup escalation**

Close stdin, then send `SIGINT`, `SIGTERM`, and `SIGKILL` to process group with two-second deadlines. Gather reader tasks with `return_exceptions=True`. Check `returncode` before each signal.

- [ ] **Step 6: Verify GREEN or take PTY branch**

Run Task 2 command again.

Expected: both tests pass and process stays alive with real registry record and socket.

If registration times out, keep diagnostics and replace print-stream process management with PTY-backed interactive Claude. Do not synthesize registry records.

- [ ] **Step 7: Check cleanup and commit**

```bash
pgrep -af '[d]cc-contract-receiver' || true
git add tests/claude_code_integration_support.py tests/test_claude_code_integration.py
git commit -m "test: add persistent Claude process fixture"
```

Expected: no test process remains. Assert test-owned registry and socket paths inside fixture cleanup; do not require unrelated user socket directories to be empty.

---

### Task 3: Add deterministic Anthropic SSE server

**Files:**
- Modify: `tests/claude_code_integration_support.py`
- Modify: `tests/test_claude_code_integration.py`

- [ ] **Step 1: Write failing unmarked SSE tests**

```python
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
    delta = json.loads(frames[3].split("data: ", 1)[1])
    assert json.loads(delta["delta"]["partial_json"])["to"] == "dcc-sender"
    assert '"stop_reason":"tool_use"' in frames[-2]
```

- [ ] **Step 2: Verify RED**

```bash
uv run --extra dev pytest \
  tests/test_claude_code_integration.py::test_text_response_has_anthropic_sse_order \
  tests/test_claude_code_integration.py::test_tool_response_streams_parsed_json_input \
  -q
```

Expected: imports for SSE helpers fail.

- [ ] **Step 3: Implement SSE builders**

```python
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
```

- [ ] **Step 4: Add and test fixture-owned deny proxy**

Add this support code:

```python
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
```

Add test:

```python
@pytest.mark.asyncio
async def test_denying_proxy_records_and_rejects_egress() -> None:
    async with DenyingProxy() as proxy:
        async with aiohttp.ClientSession() as client:
            response = await client.get("http://example.invalid/", proxy=proxy.url)
    assert response.status == 502
    assert proxy.attempts == [EgressAttempt("GET", "http://example.invalid/")]
```

Run:

```bash
uv run --extra dev pytest \
  tests/test_claude_code_integration.py::test_denying_proxy_records_and_rejects_egress \
  -q
```

Expected before implementation: import failures. Expected after implementation: `1 passed`.

- [ ] **Step 5: Write failing HTTP state-machine test**

```python
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
    async with ScriptedAnthropicServer({"sender-test-token": sender}) as server:
        async with aiohttp.ClientSession() as client:
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
```

- [ ] **Step 6: Implement fake server and strict state machine**

```python
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
            authorization = request.headers.get("authorization", "")
            key = authorization.removeprefix("Bearer ")
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
```

Every harness wait begins by raising when `fake_server.failures` is nonempty, before checking process and bridge health. Add imports for `deque` and `Callable`.

- [ ] **Step 7: Verify helper tests and commit**

```bash
uv run --extra dev pytest tests/test_claude_code_integration.py -q
uv run --extra dev pytest -q
git add tests/claude_code_integration_support.py tests/test_claude_code_integration.py
git commit -m "test: add scripted Anthropic SSE server"
```

Expected: unmarked helper tests pass; marked CLI tests remain deselected by default.

---

### Task 4: Orchestrate real broker, bridges, and peers

**Files:**
- Modify: `tests/claude_code_integration_support.py`
- Modify: `tests/test_claude_code_integration.py`

- [ ] **Step 1: Write failing visibility test**

```python
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
```

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_two_claude_sessions_become_remote_peers \
  -q
```

Expected: imports for `IntegrationHarness` and scenario factories fail.

- [ ] **Step 3: Add observable production broker**

Subclass only to record routes:

```python
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
```

Start real production handlers:

```python
authenticator = TokenAuthenticator(
    {
        "sender-node": "sender-broker-token",
        "receiver-node": "receiver-broker-token",
    }
)
server = await serve(
    lambda websocket: handle_websocket(websocket, broker, authenticator),
    "127.0.0.1",
    0,
    process_request=make_process_request(authenticator),
    max_size=MAX_MESSAGE_BYTES,
    compression=None,
)
assert server.sockets
port = server.sockets[0].getsockname()[1]
broker_url = f"ws://127.0.0.1:{port}"
```

- [ ] **Step 4: Start real bridges with isolated registries**

```python
sender_config = BridgeConfig(
    server_url=broker_url,
    token="sender-broker-token",
    publish_names=("dcc-sender",),
    sessions_dir=sender.sessions_dir,
    sockets_dir=tmp_path / "sender-virtual-sockets",
)
receiver_config = BridgeConfig(
    server_url=broker_url,
    token="receiver-broker-token",
    publish_names=("dcc-receiver",),
    sessions_dir=receiver.sessions_dir,
    sockets_dir=tmp_path / "receiver-virtual-sockets",
)
```

Add scenario factories used by all later tests:

```python
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
        prompting_sender(allow_send_message=False),
        permission_mode="bypassPermissions",
        bypass=True,
    )


def bypass_receiver(*, inbound_accept: bool) -> ClaudeLaunchOptions:
    return dataclasses.replace(
        prompting_receiver(inbound_accept=inbound_accept),
        permission_mode="bypassPermissions",
        bypass=True,
    )
```

`IntegrationHarness.__aenter__()` replaces each option's empty `base_url` and `deny_proxy_url` with fixture-owned URLs before starting processes. Run each bridge as named task. Separate session directories prevent either bridge from publishing both real sessions.

- [ ] **Step 5: Add lifecycle and health waits**

Track ordered labels:

```python
LIFECYCLE = (
    "sender Claude registration",
    "receiver Claude registration",
    "sender bridge connection",
    "receiver bridge connection",
    "sender session publication",
    "receiver session publication",
    "sender remote peer creation",
    "receiver remote peer creation",
    "request route",
    "receiver delivery",
    "reply route",
    "sender delivery",
)
```

Every wait checks Claude exit codes, bridge task exceptions, fake-server errors, broker state, peer worker PIDs, registry record, socket existence, and socket mode `0o600`. Update `last_satisfied_condition` after each successful wait. `IntegrationHarness.real_session(side, name)` returns the matching production `LocalSession` from `discover_live_sessions()` for that side, so permission assertions use `.permission_mode`. Expose task state for diagnostics:

```python
def task_statuses(self) -> dict[str, dict[str, object]]:
    return {
        task.get_name(): {
            "done": task.done(),
            "cancelled": task.cancelled(),
            "exception": (
                repr(task.exception())
                if task.done() and not task.cancelled()
                else None
            ),
        }
        for task in self.bridge_tasks
    }
```

- [ ] **Step 6: Implement reverse-order cleanup**

Use `AsyncExitStack`. Stop Claude input and process groups, cancel and await bridges, let bridge cleanup stop peers, close broker, deny proxy, then fake API. Collect cleanup errors after every attempt. Preserve original test failure:

```python
async def __aexit__(
    self,
    exc_type: type[BaseException] | None,
    exc: BaseException | None,
    traceback: TracebackType | None,
) -> None:
    cleanup_errors: list[BaseException] = []
    if exc is not None:
        try:
            self.write_diagnostics(exc)
        except BaseException as diagnostic_error:
            cleanup_errors.append(diagnostic_error)
    cleanup_errors.extend(await self.cleanup())
    if cleanup_errors and exc is None:
        raise ExceptionGroup("integration cleanup failed", cleanup_errors)
```

If both original failure and cleanup failures exist, attach cleanup failures to diagnostics and let original failure propagate.

- [ ] **Step 7: Add failure diagnostics**

Write `.artifacts/claude-code-integration/<test>.json` only on failure. Snapshot registries without trusting malformed entries:

```python
def snapshot_registry(directory: Path) -> list[dict[str, object]]:
    snapshots: list[dict[str, object]] = []
    for path in sorted(directory.glob("*.json")):
        try:
            value = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            snapshots.append({"path": str(path), "error": repr(error)})
        else:
            snapshots.append({"path": str(path), "record": redact(value)})
    return snapshots


{
    "claude_version": resolved_version,
    "last_satisfied_condition": last_satisfied_condition,
    "processes": {"sender": sender.diagnostic(), "receiver": receiver.diagnostic()},
    "fake_api_history": fake_server.redacted_history(),
    "broker_routes": [dataclasses.asdict(event) for event in broker.route_events],
    "logs": self.memory_logs,
    "registry": {
        "sender": snapshot_registry(sender.sessions_dir),
        "receiver": snapshot_registry(receiver.sessions_dir),
    },
    "tasks": self.task_statuses(),
    "compatibility_failure": classify_failure(
        last_satisfied_condition=last_satisfied_condition,
        scenario_failures=fake_server.failures,
        process_diagnostics={
            "sender": sender.diagnostic(),
            "receiver": receiver.diagnostic(),
        },
    ),
}
```

Recursively redact keys matching authorization, API key, token, secret, credential, password, OAuth, or profile. `classify_failure()` returns one of `cli_invocation_or_output`, `messages_api_or_sse`, `unverified_registry_version`, `registry_or_unix_socket`, `native_peer_envelope`, `permission_or_inbound_admission`, `distributedcc_routing`, `cleanup`, or `unknown`, based on last lifecycle stage and captured failure source. Include category in raised assertion text and JSON artifact.

- [ ] **Step 8: Verify GREEN and commit**

Run:

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_two_claude_sessions_become_remote_peers \
  -q
```

Expected: `1 passed`; both real sessions and opposite virtual peers appear, then all owned resources disappear during cleanup.

```bash
git add tests/claude_code_integration_support.py tests/test_claude_code_integration.py
git commit -m "test: orchestrate real DistributedCC integration"
```

---

### Task 5: Add explicit-allow round trip

**Files:**
- Modify: `tests/claude_code_integration_support.py`
- Modify: `tests/test_claude_code_integration.py`

- [ ] **Step 1: Write failing round-trip test**

```python
@pytest.mark.claude_code_integration
async def test_explicit_allow_round_trip(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with round_trip_harness(
        tmp_path,
        integration_artifact_dir,
        sender=prompting_sender(allow_send_message=True),
        receiver=prompting_receiver(
            inbound_accept=True,
            allow_send_message=True,
        ),
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
        assert harness.fake_server.sender_saw_reply("INTEGRATION_REPLY")
        assert harness.fake_server.receiver_saw_inbound("INTEGRATION_PING")
        assert harness.sender.has_result_text("Round trip complete.")
        assert harness.receiver.has_result_text("Receiver reply sent.")
        harness.fake_server.assert_clean(harness.deny_proxy)
```

- [ ] **Step 2: Verify RED**

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_explicit_allow_round_trip \
  -q
```

Expected: timeout at request route because scripts are absent.

- [ ] **Step 3: Add parsed request predicates**

```python
@dataclass(frozen=True, slots=True)
class PeerEnvelope:
    name: str
    mode: str
    body: str


def text_blocks(request: dict[str, object]) -> list[str]:
    texts: list[str] = []
    for message in request.get("messages", []):
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
    envelopes: list[PeerEnvelope] = []
    pattern = re.compile(
        r"<cross-session-message\b.*?</cross-session-message>",
        re.DOTALL,
    )
    for text in text_blocks(request):
        for match in pattern.finditer(text):
            element = ElementTree.fromstring(match.group(0))
            envelopes.append(
                PeerEnvelope(
                    name=element.attrib["from-name"],
                    mode=element.attrib["from-mode"],
                    body=element.text or "",
                )
            )
    return envelopes


def contains_text(value: str) -> Callable[[dict[str, object]], bool]:
    return lambda request: any(value in text for text in text_blocks(request))


def find_tool_result(
    request: dict[str, object],
    tool_use_id: str,
) -> dict[str, object]:
    for message in request.get("messages", []):
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
```

- [ ] **Step 4: Add exact sender and receiver scripts**

```python
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


@asynccontextmanager
async def round_trip_harness(
    tmp_path: Path,
    artifact_dir: Path,
    *,
    sender: ClaudeLaunchOptions,
    receiver: ClaudeLaunchOptions,
) -> AsyncIterator[IntegrationHarness]:
    source_mode = "bypass" if sender.bypass else "prompting"
    harness = IntegrationHarness(
        tmp_path,
        artifact_dir,
        sender=sender,
        receiver=receiver,
        sender_steps=sender_round_trip_steps(),
        receiver_steps=receiver_round_trip_steps(source_mode),
    )
    async with harness:
        yield harness
```

Add imports for `asynccontextmanager`, `AsyncIterator`, and `xml.etree.ElementTree`.

- [ ] **Step 5: Verify GREEN and commit**

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_explicit_allow_round_trip -vv
git add tests/claude_code_integration_support.py tests/test_claude_code_integration.py
git commit -m "test: cover allowed Claude round trip"
```

Expected: one passing test and exactly two broker routes.

---

### Task 6: Add explicit tool-denial scenario

**Files:**
- Modify: `tests/claude_code_integration_support.py`
- Modify: `tests/test_claude_code_integration.py`

- [ ] **Step 1: Write failing denial test**

```python
@pytest.mark.claude_code_integration
async def test_dont_ask_denies_unallowed_send_message(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with denial_harness(tmp_path, integration_artifact_dir) as harness:
        await harness.wait_ready()
        route_count = len(harness.broker.route_events)
        await harness.sender.send_user("Attempt the denied integration send.")
        continuation = await harness.fake_server.wait_for_sender_continuation(10.0)

        assert tool_result_is_denied(continuation, "toolu_denied_send")
        assert harness.sender.has_denied_tool_result("toolu_denied_send")
        await harness.assert_route_count_stable(route_count, duration=1.0)
        assert not harness.fake_server.receiver_saw_inbound("DENIED_MESSAGE")
        assert harness.sender.returncode is None
        assert harness.receiver.returncode is None
        assert harness.deny_proxy.attempts == []
```

- [ ] **Step 2: Verify RED**

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_dont_ask_denies_unallowed_send_message \
  -q
```

Expected: script mismatch or route increase before denial setup is correct.

- [ ] **Step 3: Implement denial script and stable absence check**

Launch sender with `--tools SendMessage`, `--permission-mode dontAsk`, `--permission-prompts none`, and no `--allowedTools SendMessage`. Return a `SendMessage` call containing `DENIED_MESSAGE`. Parse denial structurally:

Reuse `find_tool_result()` from Task 5 and add:

```python
def tool_result_is_denied(
    request: dict[str, object],
    tool_use_id: str,
) -> bool:
    return find_tool_result(request, tool_use_id).get("is_error") is True


@asynccontextmanager
async def denial_harness(
    tmp_path: Path,
    artifact_dir: Path,
) -> AsyncIterator[IntegrationHarness]:
    sender_steps = [
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
    harness = IntegrationHarness(
        tmp_path,
        artifact_dir,
        sender=prompting_sender(allow_send_message=False),
        receiver=prompting_receiver(inbound_accept=True),
        sender_steps=sender_steps,
        receiver_steps=[],
    )
    async with harness:
        yield harness
```

`assert_route_count_stable()` polls broker count, receiver API history, deny-proxy attempts, and all component health until one-second deadline. Any change fails immediately.

- [ ] **Step 4: Verify GREEN and commit**

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_dont_ask_denies_unallowed_send_message \
  -q
```

Expected: denied tool result, zero new broker route, no receiver request, healthy processes.

```bash
git add tests/claude_code_integration_support.py tests/test_claude_code_integration.py
git commit -m "test: cover denied SendMessage tool"
```

---

### Task 7: Add bypass propagation scenario

**Files:**
- Modify: `tests/claude_code_integration_support.py`
- Modify: `tests/test_claude_code_integration.py`

- [ ] **Step 1: Write failing bypass test**

```python
@pytest.mark.claude_code_integration
async def test_bypass_mode_propagates_through_broker(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with round_trip_harness(
        tmp_path,
        integration_artifact_dir,
        sender=bypass_sender(),
        receiver=prompting_receiver(
            inbound_accept=True,
            allow_send_message=True,
        ),
    ) as harness:
        await harness.wait_ready()
        assert harness.real_session("sender", "dcc-sender").permission_mode == "bypass"
        await harness.sender.send_user("Start the bypass propagation test.")
        await harness.wait_for_completed_round_trip()

        route = harness.broker.route_events[0]
        assert route.source_session == "dcc-sender"
        assert route.target_session == "dcc-receiver"
        assert route.source_mode == "bypass"
        assert route.body == "INTEGRATION_PING"

        envelope = next(
            envelope
            for envelope in peer_envelopes(
                harness.fake_server.receiver_peer_request()
            )
            if envelope.name == "dcc-sender"
        )
        assert envelope.mode == "bypass"
        assert envelope.body == "INTEGRATION_PING"
        assert harness.deny_proxy.attempts == []
```

- [ ] **Step 2: Verify RED**

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_bypass_mode_propagates_through_broker \
  -q
```

Expected: sender appears as `prompting` until dangerous flag is present.

- [ ] **Step 3: Implement isolated bypass launch**

`bypass_sender()` sets `bypass=True`, adds `--dangerously-skip-permissions`, limits tools to `SendMessage`, and uses empty isolated workspace/home/config. Receiver keeps session-scoped `crossSessionInbound: accept`.

- [ ] **Step 4: Verify GREEN and commit**

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_bypass_mode_propagates_through_broker \
  -q
```

Expected: registry, broker route, and receiver envelope all report bypass; reply returns.

```bash
git add tests/claude_code_integration_support.py tests/test_claude_code_integration.py
git commit -m "test: cover bypass mode propagation"
```

---

### Task 8: Add receiver-admission hold scenario

**Files:**
- Modify: `tests/claude_code_integration_support.py`
- Modify: `tests/test_claude_code_integration.py`

- [ ] **Step 1: Write failing hold test**

```python
@pytest.mark.claude_code_integration
async def test_receiver_without_accept_holds_mismatched_inbound_message(
    tmp_path: Path,
    integration_artifact_dir: Path,
) -> None:
    async with admission_harness(
        tmp_path,
        integration_artifact_dir,
        sender=prompting_sender(allow_send_message=True),
        receiver=bypass_receiver(inbound_accept=False),
    ) as harness:
        await harness.wait_ready()
        receiver_requests = harness.fake_server.receiver_request_count
        await harness.sender.send_user("Start the held-message test.")
        await harness.wait_for_route_count(1)

        route = harness.broker.route_events[0]
        assert route.source_mode == "prompting"
        assert route.target_session == "dcc-receiver"
        await harness.assert_receiver_request_count_stable(
            receiver_requests,
            duration=1.5,
        )
        assert not harness.fake_server.receiver_saw_inbound("HELD_MESSAGE")
        assert harness.receiver.returncode is None
        assert harness.deny_proxy.attempts == []
```

- [ ] **Step 2: Verify RED**

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_receiver_without_accept_holds_mismatched_inbound_message \
  -q
```

Expected: receiver history increases before hold settings are applied correctly.

- [ ] **Step 3: Implement mismatched hold scenario**

Add:

```python
@asynccontextmanager
async def admission_harness(
    tmp_path: Path,
    artifact_dir: Path,
    *,
    sender: ClaudeLaunchOptions,
    receiver: ClaudeLaunchOptions,
) -> AsyncIterator[IntegrationHarness]:
    sender_steps = [
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
    harness = IntegrationHarness(
        tmp_path,
        artifact_dir,
        sender=sender,
        receiver=receiver,
        sender_steps=sender_steps,
        receiver_steps=[],
    )
    async with harness:
        yield harness
```

Sender is prompting class. Receiver is bypass class with `crossSessionInbound: hold`. Stable observation proves broker route occurred, receiver process and bridge stayed alive, receiver API count did not change, and body stayed absent.

- [ ] **Step 4: Verify GREEN and commit**

```bash
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py::test_receiver_without_accept_holds_mismatched_inbound_message \
  -q
```

Expected: one broker route, no receiver model request, no reply route, healthy components.

```bash
git add tests/claude_code_integration_support.py tests/test_claude_code_integration.py
git commit -m "test: cover receiver admission hold"
```

---

### Task 9: Harden diagnostics and cleanup

**Files:**
- Modify: `tests/claude_code_integration_support.py`
- Modify: `tests/test_claude_code_integration.py`

- [ ] **Step 1: Write failing recursive-redaction test**

```python
def test_diagnostics_redact_all_dummy_credentials() -> None:
    diagnostic = redact(
        {
            "authorization": "Bearer broker-secret",
            "headers": {"x-api-key": "sender-test-token"},
            "nested": {"token": "receiver-broker-token"},
            "message": "INTEGRATION_PING",
        }
    )
    serialized = json.dumps(diagnostic)
    assert "broker-secret" not in serialized
    assert "sender-test-token" not in serialized
    assert "receiver-broker-token" not in serialized
    assert "INTEGRATION_PING" in serialized
```

- [ ] **Step 2: Verify RED and implement redaction**

Run:

```bash
uv run --extra dev pytest \
  tests/test_claude_code_integration.py::test_diagnostics_redact_all_dummy_credentials \
  -q
```

Expected before implementation: nested credentials remain.

Implement:

```python
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
```

Run same test again. Expected: `1 passed`; deterministic fixture bodies remain visible.

- [ ] **Step 3: Add cleanup regression test**

```python
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
```

- [ ] **Step 4: Run full local verification**

```bash
rm -rf .artifacts/claude-code-integration
DCC_INTEGRATION_ARTIFACT_DIR=.artifacts/claude-code-integration \
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py -vv
uv run --extra dev pytest -q
uv run --extra dev ruff check .
uv lock --check
git diff --check
```

Expected: all integration scenarios pass, normal suite passes with marked tests deselected, Ruff passes, lock is current, and diff check is silent.

- [ ] **Step 5: Commit**

```bash
git add tests/claude_code_integration_support.py tests/test_claude_code_integration.py
git commit -m "test: harden integration diagnostics"
```

---

### Task 10: Add normal pull-request workflow

**Files:**
- Create: `.github/workflows/tests.yml`

Use immutable action SHAs shown below. Before committing, verify each SHA resolves to its annotated major tag with `git ls-remote`; never substitute a movable tag. Do not modify existing `docker-publish.yaml` in this change.

- [ ] **Step 1: Verify workflow is absent**

```bash
test -f .github/workflows/tests.yml
```

Expected: exit `1`.

- [ ] **Step 2: Create workflow**

```yaml
name: Tests

on:
  pull_request:
    types: [opened, reopened, synchronize]

permissions:
  contents: read

concurrency:
  group: tests-${{ github.workflow }}-${{ github.event.pull_request.number }}
  cancel-in-progress: true

jobs:
  pytest:
    name: Python ${{ matrix.python }}
    runs-on: ubuntu-latest
    timeout-minutes: 10
    strategy:
      fail-fast: false
      matrix:
        python: ["3.12", "3.13", "3.14"]
    steps:
      - name: Check out repository
        uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803
        with:
          fetch-depth: 0
          persist-credentials: false
      - name: Set up Python
        uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1
        with:
          python-version: ${{ matrix.python }}
      - name: Set up uv
        uses: astral-sh/setup-uv@37802adc94f370d6bfd71619e3f0bf239e1f3b78
        with:
          enable-cache: true
          cache-dependency-glob: uv.lock
      - name: Install dependencies
        run: uv sync --frozen --extra dev
      - name: Run normal tests
        run: uv run --no-sync pytest -q

  quality:
    name: Lint and diff hygiene
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - name: Check out repository
        uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803
        with:
          fetch-depth: 0
          persist-credentials: false
      - name: Set up Python
        uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1
        with:
          python-version: "3.14"
      - name: Set up uv
        uses: astral-sh/setup-uv@37802adc94f370d6bfd71619e3f0bf239e1f3b78
      - name: Install dependencies
        run: uv sync --frozen --extra dev
      - name: Check lock file
        run: uv lock --check
      - name: Run Ruff
        run: uv run --no-sync ruff check .
      - name: Check changed lines for whitespace errors
        run: >-
          git diff --check
          "${{ github.event.pull_request.base.sha }}"
          "${{ github.event.pull_request.head.sha }}"
```

Before committing, verify the pinned action SHAs with `git ls-remote` and keep the immutable revisions shown above.

- [ ] **Step 3: Validate and commit**

```bash
grep -q 'persist-credentials: false' .github/workflows/tests.yml
grep -q 'uv run --no-sync pytest -q' .github/workflows/tests.yml
git diff --check -- .github/workflows/tests.yml
git add .github/workflows/tests.yml
git commit -m "ci: add normal test workflow"
```

Expected: all checks exit `0`.

---

### Task 11: Add pinned and latest integration workflow

**Files:**
- Create: `.github/workflows/claude-code-integration.yml`
- Modify: `tests/claude_code_integration_support.py`

- [ ] **Step 1: Verify workflow is absent**

```bash
test -f .github/workflows/claude-code-integration.yml
```

Expected: exit `1`.

- [ ] **Step 2: Verify channel-aware version tests from Task 2**

Run:

```bash
uv run --extra dev pytest \
  tests/test_claude_code_integration.py::test_expected_version_requires_exact_match \
  tests/test_claude_code_integration.py::test_latest_channel_records_version_without_repinning \
  -q
```

Expected: `2 passed`. Pinned channel enforces exact version; latest channel records any semantic version without changing production constant.

- [ ] **Step 3: Create integration workflow with two explicit jobs**

```yaml
name: Claude Code integration

on:
  pull_request:
    types: [opened, reopened, synchronize]
  schedule:
    - cron: "17 4 * * 1"
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: claude-code-integration-${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: true

jobs:
  supported:
    name: Claude Code 2.1.296
    runs-on: ubuntu-latest
    timeout-minutes: 20
    env:
      CLAUDE_PACKAGE_VERSION: 2.1.296
      EXPECTED_CLAUDE_CODE_VERSION: 2.1.296
      DCC_INTEGRATION_ARTIFACT_DIR: .artifacts/claude-code-integration/supported
      CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1"
      DISABLE_TELEMETRY: "1"
      DISABLE_ERROR_REPORTING: "1"
      DISABLE_UPDATES: "1"
      DISABLE_AUTOUPDATER: "1"
      DISABLE_INSTALLATION_CHECKS: "1"
    steps:
      - name: Check out repository
        uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803
        with:
          persist-credentials: false
      - name: Set up Python
        uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1
        with:
          python-version: "3.14"
      - name: Set up Node
        uses: actions/setup-node@249970729cb0ef3589644e2896645e5dc5ba9c38
        with:
          node-version: "24"
      - name: Set up uv
        uses: astral-sh/setup-uv@37802adc94f370d6bfd71619e3f0bf239e1f3b78
        with:
          enable-cache: true
          cache-dependency-glob: uv.lock
      - name: Install Python dependencies
        run: uv sync --frozen --extra dev
      - name: Install Claude Code
        run: npm install --global "@anthropic-ai/claude-code@${CLAUDE_PACKAGE_VERSION}"
      - name: Record resolved Claude version
        run: |
          resolved="$(claude --version)"
          printf '### Claude Code supported\n\n`%s`\n' "$resolved" >> "$GITHUB_STEP_SUMMARY"
      - name: Run Claude Code integration tests
        run: >-
          uv run --no-sync pytest
          -o "addopts=--strict-config --strict-markers"
          -m claude_code_integration
          tests/test_claude_code_integration.py
          -vv
      - name: Upload failure diagnostics
        if: failure()
        uses: actions/upload-artifact@b7c566a772e6b6bfb58ed0dc250532a479d7789f
        with:
          name: claude-code-integration-supported
          path: .artifacts/claude-code-integration/supported
          if-no-files-found: warn
          retention-days: 7

  latest:
    name: Claude Code latest compatibility
    runs-on: ubuntu-latest
    timeout-minutes: 20
    continue-on-error: true
    env:
      CLAUDE_PACKAGE_VERSION: latest
      EXPECTED_CLAUDE_CODE_VERSION: ""
      DCC_INTEGRATION_ARTIFACT_DIR: .artifacts/claude-code-integration/latest
      CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1"
      DISABLE_TELEMETRY: "1"
      DISABLE_ERROR_REPORTING: "1"
      DISABLE_UPDATES: "1"
      DISABLE_AUTOUPDATER: "1"
      DISABLE_INSTALLATION_CHECKS: "1"
    steps:
      - name: Check out repository
        uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803
        with:
          persist-credentials: false
      - name: Set up Python
        uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1
        with:
          python-version: "3.14"
      - name: Set up Node
        uses: actions/setup-node@249970729cb0ef3589644e2896645e5dc5ba9c38
        with:
          node-version: "24"
      - name: Set up uv
        uses: astral-sh/setup-uv@37802adc94f370d6bfd71619e3f0bf239e1f3b78
        with:
          enable-cache: true
          cache-dependency-glob: uv.lock
      - name: Install Python dependencies
        run: uv sync --frozen --extra dev
      - name: Install Claude Code
        run: npm install --global "@anthropic-ai/claude-code@${CLAUDE_PACKAGE_VERSION}"
      - name: Record resolved Claude version
        run: |
          resolved="$(claude --version)"
          printf '### Claude Code latest\n\n`%s`\n' "$resolved" >> "$GITHUB_STEP_SUMMARY"
      - name: Run Claude Code integration tests
        run: >-
          uv run --no-sync pytest
          -o "addopts=--strict-config --strict-markers"
          -m claude_code_integration
          tests/test_claude_code_integration.py
          -vv
      - name: Upload failure diagnostics
        if: failure()
        uses: actions/upload-artifact@b7c566a772e6b6bfb58ed0dc250532a479d7789f
        with:
          name: claude-code-integration-latest
          path: .artifacts/claude-code-integration/latest
          if-no-files-found: warn
          retention-days: 7
```

- [ ] **Step 4: Verify immutable action SHAs and workflow policy**

Run `git ls-remote` for each action repository and confirm each pinned SHA remains reachable under the annotated major release. Then run:

```bash
grep -q '^  supported:' .github/workflows/claude-code-integration.yml
grep -q '^  latest:' .github/workflows/claude-code-integration.yml
grep -q 'CLAUDE_PACKAGE_VERSION: 2.1.296' .github/workflows/claude-code-integration.yml
grep -q 'CLAUDE_PACKAGE_VERSION: latest' .github/workflows/claude-code-integration.yml
grep -q 'continue-on-error: true' .github/workflows/claude-code-integration.yml
grep -q 'contents: read' .github/workflows/claude-code-integration.yml
grep -q 'if: failure()' .github/workflows/claude-code-integration.yml
! grep -q 'secrets\.' .github/workflows/claude-code-integration.yml
git diff --check -- .github/workflows/claude-code-integration.yml
```

Expected: all checks exit `0`; supported job has stable blocking name and latest job is visibly non-blocking.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/claude-code-integration.yml \
  tests/claude_code_integration_support.py \
  tests/test_claude_code_integration.py
git commit -m "ci: test pinned and latest Claude Code"
```


---

### Task 12: Final verification and review

**Files:**
- Review all modified files.

- [ ] **Step 1: Run normal suite**

```bash
uv run --extra dev pytest -q
```

Expected: normal tests and unmarked helper tests pass; marked real-CLI tests are deselected.

- [ ] **Step 2: Run pinned integration suite**

```bash
test "$(claude --version | awk '{print $1}')" = "2.1.296"
rm -rf .artifacts/claude-code-integration
EXPECTED_CLAUDE_CODE_VERSION=2.1.296 \
DCC_INTEGRATION_ARTIFACT_DIR=.artifacts/claude-code-integration/local \
uv run --extra dev pytest \
  -o 'addopts=--strict-config --strict-markers' \
  -m claude_code_integration \
  tests/test_claude_code_integration.py -vv
```

Expected: CLI contract, peer visibility, explicit allow, denial, bypass, receiver hold, diagnostics, and cleanup tests pass.

- [ ] **Step 3: Verify no leaked resources**

```bash
pgrep -af '[d]cc-(sender|receiver)' || true
find .artifacts/claude-code-integration -maxdepth 3 -type f -print
```

Expected: no test Claude process and diagnostics only for failures. Test assertions verify fixture-owned registry and socket paths are gone.

- [ ] **Step 4: Run quality gates**

```bash
uv run --extra dev ruff check .
uv lock --check
git diff --check
```

Expected: Ruff passes, lock is current, diff check is silent.

- [ ] **Step 5: Confirm workflow credential policy**

```bash
! grep -RInE 'secrets\.|ANTHROPIC_AUTH_TOKEN|AWS_|GOOGLE_' \
  .github/workflows/tests.yml \
  .github/workflows/claude-code-integration.yml
```

Expected: no production secret or cloud credential reference.

- [ ] **Step 6: Request code review**

Use `superpowers:requesting-code-review` against implementation commits. Fix verified findings and rerun affected tests plus full normal suite.

- [ ] **Step 7: Final commit only if review fixes remain uncommitted**

```bash
git status --short
```

If review produced changes:

```bash
git add .github/workflows/tests.yml \
  .github/workflows/claude-code-integration.yml \
  .gitignore pyproject.toml uv.lock \
  tests/claude_code_integration_support.py \
  tests/test_claude_code_integration.py
git commit -m "test: finalize Claude Code integration"
```
