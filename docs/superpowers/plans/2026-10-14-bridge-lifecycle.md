# Bridge Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep idle bridges connected, publish changing local-session snapshots, recover immediately after reboot, and relay each sender's actual permission-mode class.

**Architecture:** One bridge connection runs receiver, sender, and session-watcher tasks under one cancellation scope. Broker state binds every operation to a concrete connection object, so a newly authenticated connection can replace a stale same-node connection safely. Native Claude Code `from-mode` becomes authoritative per message; publication mode remains a compatibility hint.

**Tech Stack:** Python 3.12+, asyncio, websockets 16.0, pytest, pytest-asyncio, Ruff

**Source structure:** Keep existing focused modules. `protocol.py` owns wire contracts, `virtual_peer.py` owns native socket decoding, `bridge.py` owns client lifecycle, and `server.py` owns broker lifecycle. Add no new production module.

**Commit policy:** Do not create commits unless user explicitly requests them. Each task ends with a verification checkpoint instead.

---

### Task 1: Carry event-time permission mode through native and broker protocols

**Files:**
- Modify: `src/distributedcc/virtual_peer.py:30-95,233-279`
- Modify: `src/distributedcc/protocol.py:56-71,123-148`
- Modify: `src/distributedcc/bridge.py:273-288`
- Test: `tests/test_virtual_peer.py:45-76`
- Test: `tests/test_protocol.py:44-75`

- [ ] **Step 1: Add failing native-envelope mode tests**

Change `tests/test_virtual_peer.py` imports to include `NativeSend`, then replace body-only assertions with value assertions:

```python
def test_decodes_native_send_with_permission_mode() -> None:
    content = build_peer_envelope(
        address="uds:/run/user/1000/cc-socks/111.sock",
        session_id="session-123",
        name="claimed-name",
        mode="bypass",
        body="FEATURE_READY",
    )
    frame = json.dumps(
        {
            "type": "user",
            "from": "uds:/run/user/1000/cc-socks/111.sock",
            "message": {"role": "user", "content": content},
        }
    )

    assert decode_native_send(frame) == NativeSend("FEATURE_READY", "bypass")


def test_decodes_native_send_without_mode_as_prompting() -> None:
    frame = json.dumps(
        {
            "type": "user",
            "message": {
                "role": "user",
                "content": (
                    '<cross-session-message from="uds:/run/user/1000/cc-socks/111.sock" '
                    'from-name="feature-owner">\n'
                    "hello\n"
                    "</cross-session-message>"
                ),
            },
        }
    )

    assert decode_native_send(frame) == NativeSend("hello", "prompting")
```

- [ ] **Step 2: Run native tests and verify RED**

Run:

```bash
uv run --extra dev pytest tests/test_virtual_peer.py -q
```

Expected: FAIL because `NativeSend` does not exist and `decode_native_send()` returns `str`.

- [ ] **Step 3: Return body and actual mode from native decoder**

Add beside `RemoteSession` in `src/distributedcc/virtual_peer.py`:

```python
@dataclass(frozen=True, slots=True)
class NativeSend:
    body: str
    source_mode: Literal["bypass", "prompting"]
```

Give `NATIVE_ENVELOPE_RE` named groups for optional mode and body:

```python
NATIVE_ENVELOPE_RE = re.compile(
    rf'^<cross-session-message from="{ADDRESS_PATTERN}"'
    rf'(?: from-session="{SESSION_PATTERN}")?'
    r'(?: hop-chain="[0-9a-f]{24}(?:,[0-9a-f]{24}){0,31}")?'
    r'(?: from-name="[^"<>\n\r]+")?'
    r'(?: from-mode="(?P<mode>bypass|prompting)")?'
    r'(?: from-plugin="[^"<>\n\r]+")?>\n'
    r'(?P<body>[\s\S]*)\n</cross-session-message>$'
)
```

Change decoder return:

```python
def decode_native_send(raw: str) -> NativeSend:
    # Existing size, JSON, frame, message, and canonical-envelope checks remain.
    envelope = NATIVE_ENVELOPE_RE.fullmatch(message["content"])
    if envelope is None:
        raise ProtocolError("peer envelope is not canonical")
    source_mode = envelope.group("mode") or "prompting"
    return NativeSend(envelope.group("body"), source_mode)
```

Change `_handle_connection()` queue payload:

```python
native_send = decode_native_send(raw)
outbound.put(
    {
        "source_session": source.name,
        "source_mode": native_send.source_mode,
        "target_session": remote.name,
        "body": native_send.body,
    }
)
```

- [ ] **Step 4: Run native tests and verify GREEN**

Run:

```bash
uv run --extra dev pytest tests/test_virtual_peer.py -q
```

Expected: all `test_virtual_peer.py` tests PASS.

- [ ] **Step 5: Add failing route-schema tests**

Update `test_client_event_schema_round_trip()` in `tests/test_protocol.py` to include:

```python
"source_mode": "bypass",
```

and assert:

```python
assert message.source_mode == "bypass"
```

Replace the prior unknown-`from_mode` test with:

```python
def test_client_event_rejects_invalid_source_mode() -> None:
    raw = json.dumps(
        {
            "type": "route",
            "id": "60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
            "source_session": "feature-owner",
            "source_mode": "unattested",
            "target_session": "yrrp-release-manager",
            "body": "FEATURE_READY",
        }
    )

    with pytest.raises(ProtocolError, match="source mode"):
        decode_client_message(raw)
```

Keep one strict unknown-field test by adding `"forged": True` to an otherwise valid route and expecting `unknown fields`.

- [ ] **Step 6: Run route tests and verify RED**

Run:

```bash
uv run --extra dev pytest tests/test_protocol.py -q
```

Expected: FAIL because `source_mode` is unknown and `PeerEvent` lacks field.

- [ ] **Step 7: Add required `source_mode` to route contract**

Update `PeerEvent` in `src/distributedcc/protocol.py`:

```python
@dataclass(frozen=True, slots=True)
class PeerEvent:
    id: str
    source_session: str
    source_mode: Literal["bypass", "prompting"]
    target_session: str
    body: str

    def __post_init__(self) -> None:
        if not UUID_RE.fullmatch(self.id):
            raise ProtocolError("invalid event id")
        _validate_identifier("source session", self.source_session)
        if self.source_mode not in {"bypass", "prompting"}:
            raise ProtocolError("invalid source mode")
        _validate_identifier("target session", self.target_session)
        if not self.body:
            raise ProtocolError("message body is empty")
        if len(self.body.encode()) > MAX_BODY_BYTES:
            raise ProtocolError("message body is too large")
```

Update `decode_client_message()`:

```python
required = {
    "type",
    "id",
    "source_session",
    "source_mode",
    "target_session",
    "body",
}
```

Pass `source_mode=value["source_mode"]` into `PeerEvent`.

Update `_outbound_loop()` in `src/distributedcc/bridge.py`:

```python
event = {
    "type": "route",
    "id": str(uuid.uuid4()),
    "source_session": outbound["source_session"],
    "source_mode": outbound["source_mode"],
    "target_session": outbound["target_session"],
    "body": outbound["body"],
}
```

- [ ] **Step 8: Run focused protocol and virtual-peer tests**

Run:

```bash
uv run --extra dev pytest tests/test_protocol.py tests/test_virtual_peer.py -q
```

Expected: PASS.

---

### Task 2: Route broker deliveries with event-time mode

**Files:**
- Modify: `src/distributedcc/server.py:167-190,250-269`
- Test: `tests/test_server.py:27-75`

- [ ] **Step 1: Write failing broker mode-propagation test**

Change routed event construction in `tests/test_server.py`:

```python
event = PeerEvent(
    id="60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
    source_session="friend-feature",
    source_mode="bypass",
    target_session="yrrp-release-manager",
    body="FEATURE_READY",
)
```

Keep published `friend-feature` mode as `prompting`, then assert:

```python
assert andre.messages[-1]["source_mode"] == "bypass"
```

Add `source_mode="prompting"` to every other `PeerEvent` fixture.

- [ ] **Step 2: Run broker test and verify RED**

Run:

```bash
uv run --extra dev pytest tests/test_server.py::test_routes_only_from_session_owned_by_connection -q
```

Expected: FAIL because delivery uses advertisement mode `prompting` instead of event mode `bypass`.

- [ ] **Step 3: Use validated event-time mode in delivery**

Change delivery mapping in `Broker.route()`:

```python
"source_mode": event.source_mode,
```

Retain source-session ownership validation. Do not accept node, session ID, or sender name from event body.

- [ ] **Step 4: Run focused broker and protocol tests**

Run:

```bash
uv run --extra dev pytest tests/test_server.py tests/test_protocol.py -q
```

Expected: PASS.

---

### Task 3: Keep bridge connected and reconcile local-session snapshots

**Files:**
- Modify: `src/distributedcc/bridge.py:16-22,173-270`
- Test: `tests/test_bridge.py`
- Test: `tests/test_server.py`
- Test: `tests/test_server_network.py`

- [ ] **Step 1: Add explicit empty-publication contract tests**

Add to `tests/test_server_network.py`:

```python
def test_decode_publish_accepts_empty_session_list() -> None:
    assert decode_publish_message('{"type":"publish","sessions":[]}') == []
```

Add to `tests/test_server.py`:

```python
@pytest.mark.asyncio
async def test_empty_publication_removes_sessions_but_keeps_connection() -> None:
    broker = Broker()
    andre = FakeConnection()
    friend = FakeConnection()
    await broker.attach("andre", andre)
    await broker.attach("friend", friend)
    await broker.publish(
        "andre",
        [SessionAdvertisement("manager", "manager-id", "bypass")],
    )

    await broker.publish("andre", [])

    assert await broker.remote_sessions("friend") == []
```

- [ ] **Step 2: Run empty-publication tests**

Run:

```bash
uv run --extra dev pytest \
  tests/test_server_network.py::test_decode_publish_accepts_empty_session_list \
  tests/test_server.py::test_empty_publication_removes_sessions_but_keeps_connection -q
```

Expected: PASS, documenting existing load-bearing behavior.

- [ ] **Step 3: Add failing selected-session behavior tests**

Rename `_selected_local_sessions` to `_discover_selected_sessions` in test imports. Monkeypatch `distributedcc.bridge.discover_live_sessions` and assert:

```python
def test_discovers_available_subset_without_requiring_every_name(monkeypatch, bridge_config) -> None:
    live = make_local_session("one", session_id="one-id")
    monkeypatch.setattr(
        "distributedcc.bridge.discover_live_sessions",
        lambda _: {"one": live, "unconfigured": make_local_session("unconfigured")},
    )
    config = dataclasses.replace(bridge_config, publish_names=("one", "two"))

    assert _discover_selected_sessions(config) == {"one": live}
```

Add a zero-live case expecting `{}`. Use a local `make_local_session()` fixture returning real `LocalSession` objects; no mock production models.

- [ ] **Step 4: Run selected-session tests and verify RED**

Run:

```bash
uv run --extra dev pytest tests/test_bridge.py -q
```

Expected: FAIL because current helper raises on missing configured sessions.

- [ ] **Step 5: Allow empty and partial discovery**

Replace helper in `src/distributedcc/bridge.py`:

```python
def _discover_selected_sessions(config: BridgeConfig) -> dict[str, LocalSession]:
    discovered = discover_live_sessions(config.sessions_dir)
    return {
        name: discovered[name]
        for name in config.publish_names
        if name in discovered
    }
```

- [ ] **Step 6: Add failing watcher tests**

Create a `RecordingWebSocket` test helper with `sent: list[dict[str, object]]` and async `send()` that JSON-decodes frames. Patch `asyncio.sleep` with a controlled cancellation after three iterations.

Test sequence `{}` → unchanged `{}` → `{"manager": session}` and assert:

```python
assert websocket.sent == [
    {"type": "publish", "sessions": []},
    {
        "type": "publish",
        "sessions": [
            {"name": "manager", "session_id": "manager-id", "mode": "bypass"}
        ],
    },
]
```

Add sequence `{session}` → `{}` and assert second frame unpublishes. Add same-name changed session ID and changed mode-hint cases.

- [ ] **Step 7: Run watcher tests and verify RED**

Run:

```bash
uv run --extra dev pytest tests/test_bridge.py -q
```

Expected: FAIL because `_watch_local_sessions()` does not exist.

- [ ] **Step 8: Implement changed-snapshot publication**

Add constants and helpers in `src/distributedcc/bridge.py`:

```python
SESSION_POLL_SECONDS = 1.0
STABLE_CONNECTION_SECONDS = 30.0


def _snapshot_key(sessions: dict[str, LocalSession]) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        sorted(
            (session.name, session.session_id, session.permission_mode)
            for session in sessions.values()
        )
    )


async def _watch_local_sessions(
    websocket: ClientConnection,
    config: BridgeConfig,
) -> None:
    previous: tuple[tuple[str, str, str], ...] | None = None
    while True:
        sessions = _discover_selected_sessions(config)
        current = _snapshot_key(sessions)
        if current != previous:
            await _publish_sessions(websocket, sessions)
            previous = current
        await asyncio.sleep(SESSION_POLL_SECONDS)
```

Sort `_publish_sessions()` values by session name so wire snapshots stay deterministic.

- [ ] **Step 9: Add failing connection-scope cancellation test**

Patch `_receive_loop` to return normally and patch sender/watcher to block until cancelled. Call new `_run_connection_tasks()` and assert both blocked tasks observe `CancelledError` and finish.

- [ ] **Step 10: Implement first-completion task ownership**

Add:

```python
async def _run_connection_tasks(
    websocket: ClientConnection,
    config: BridgeConfig,
    supervisor: VirtualPeerSupervisor,
) -> None:
    tasks = {
        asyncio.create_task(_receive_loop(websocket, config, supervisor), name="dcc-receiver"),
        asyncio.create_task(_outbound_loop(websocket, supervisor), name="dcc-sender"),
        asyncio.create_task(_watch_local_sessions(websocket, config), name="dcc-watcher"),
    }
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        task.result()
```

Call this helper inside WebSocket context. Remove one-time `_selected_local_sessions()` and publication from `run_bridge()`.

- [ ] **Step 11: Keep local target loss inside connection**

In `_receive_loop()`, replace all-or-nothing target lookup with current discovery:

```python
targets = _discover_selected_sessions(config)
target = targets.get(delivery.target_session)
if target is None:
    logging.warning(
        "dropping delivery %s: local target %s is unavailable",
        delivery.id,
        delivery.target_session,
    )
    continue
```

Keep missing virtual source as protocol failure because broker presence and mirrored-peer state disagree.

- [ ] **Step 12: Preserve backoff after early application failures**

Track `connected_at` with event-loop monotonic time. In exception handling, reset delay to `1.0` only when connection lasted at least `STABLE_CONNECTION_SECONDS`; then sleep current delay and double to 30 seconds. Do not reset immediately after handshake.

```python
connected_at: float | None = None
async with connect(...) as websocket:
    connected_at = asyncio.get_running_loop().time()
    await _run_connection_tasks(websocket, config, supervisor)
```

Before logging retry:

```python
if connected_at is not None:
    age = asyncio.get_running_loop().time() - connected_at
    if age >= STABLE_CONNECTION_SECONDS:
        delay = 1.0
```

- [ ] **Step 13: Run bridge-focused tests**

Run:

```bash
uv run --extra dev pytest tests/test_bridge.py tests/test_server.py tests/test_server_network.py -q
```

Expected: PASS.

---

### Task 4: Replace stale same-node connections safely

**Files:**
- Modify: `src/distributedcc/server.py:12-14,63-103,105-190,217-269`
- Test: `tests/test_server.py`

- [ ] **Step 1: Extend fake connection for replacement assertions**

Update `FakeConnection`:

```python
class FakeConnection:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.closed: list[tuple[int, str]] = []

    async def send_json(self, message: dict[str, object]) -> None:
        self.messages.append(message)

    async def close(self, code: int, reason: str) -> None:
        self.closed.append((code, reason))
```

Add `close()` to `Connection` protocol and `WebSocketConnection`:

```python
async def close(self, code: int, reason: str) -> None:
    await self._websocket.close(code=code, reason=reason)
```

- [ ] **Step 2: Add failing replacement and stale-generation tests**

Add tests covering:

```python
@pytest.mark.asyncio
async def test_new_connection_replaces_existing_connection_for_same_node() -> None:
    broker = Broker()
    old = FakeConnection()
    new = FakeConnection()
    await broker.attach("andre", old)
    await broker.publish(
        "andre", old,
        [SessionAdvertisement("manager", "old-id", "bypass")],
    )

    await broker.attach("andre", new)

    assert old.closed == [(4001, "replaced by newer connection")]
    assert await broker.remote_sessions("friend") == []


@pytest.mark.asyncio
async def test_old_detach_cannot_remove_replacement() -> None:
    broker = Broker()
    old = FakeConnection()
    new = FakeConnection()
    await broker.attach("andre", old)
    await broker.attach("andre", new)
    await broker.publish(
        "andre", new,
        [SessionAdvertisement("manager", "new-id", "bypass")],
    )

    assert await broker.detach("andre", old) is False
    assert await broker.remote_sessions("friend") != []
```

Add tests that `publish("andre", old, ...)` and `route("andre", old, ...)` raise `BrokerError("stale connection")` after replacement.

- [ ] **Step 3: Run replacement tests and verify RED**

Run:

```bash
uv run --extra dev pytest tests/test_server.py -q
```

Expected: FAIL because duplicate connection is rejected and broker methods lack connection identity.

- [ ] **Step 4: Make attach newest-wins and operations generation-aware**

Implement `attach()`:

```python
async def attach(self, node_id: str, connection: Connection) -> None:
    async with self._lock:
        previous = self._connections.get(node_id)
        self._connections[node_id] = connection
        self._sessions = {
            name: session
            for name, session in self._sessions.items()
            if session.node_id != node_id
        }
    if previous is not None and previous is not connection:
        await previous.close(4001, "replaced by newer connection")
        logging.info("node connection replaced: %s", node_id)
        return
    logging.info("node connected: %s", node_id)
```

Implement identity helper under lock:

```python
def _require_current(self, node_id: str, connection: Connection) -> None:
    if self._connections.get(node_id) is not connection:
        raise BrokerError(f"node {node_id!r} has a stale connection")
```

Change signatures:

```python
async def detach(self, node_id: str, connection: Connection) -> bool
async def publish(
    self,
    node_id: str,
    connection: Connection,
    advertisements: list[SessionAdvertisement],
) -> None
async def route(self, node_id: str, connection: Connection, event: PeerEvent) -> None
```

`detach()` checks identity before removal and returns whether it detached. `publish()` and `route()` call `_require_current()` while holding lock.

Update handler dispatch exactly:

```python
if message_type == "publish":
    await broker.publish(node_id, connection, decode_publish_message(raw))
    await broker.broadcast_presence()
    return
if message_type == "route":
    await broker.route(node_id, connection, decode_client_message(raw))
    return
```

Update every direct broker call in `tests/test_server.py`:

- store each `FakeConnection` passed to `attach()` in a named variable;
- pass that same variable as second argument to its node's `publish()` call;
- pass sender's connection as second argument to `route()`;
- pass attached connection as second argument to `detach()` in lifecycle logging test.

For example:

```python
andre = FakeConnection()
await broker.attach("andre", andre)
await broker.publish(
    "andre",
    andre,
    [SessionAdvertisement("manager", "manager-id", "bypass")],
)
await broker.route("andre", andre, event)
assert await broker.detach("andre", andre) is True
```

- [ ] **Step 5: Make handler cleanup conditional**

In `handle_websocket()`:

```python
await broker.attach(node_id, connection)
attached = True
await broker.broadcast_presence()
```

In `finally`:

```python
if attached and await broker.detach(node_id, connection):
    await broker.broadcast_presence()
```

This prevents old handler cleanup from broadcasting false removal after takeover.

- [ ] **Step 6: Run broker tests**

Run:

```bash
uv run --extra dev pytest tests/test_server.py -q
```

Expected: PASS.

---

### Task 5: Classify abrupt WebSocket loss without traceback

**Files:**
- Modify: `src/distributedcc/server.py:12-15,217-247`
- Test: `tests/test_server_network.py`

- [ ] **Step 1: Add abrupt-close network regression test**

Use real `serve()` and `connect()` with a shared `Broker`. Connect using valid bearer header, publish one session, abort client transport, and wait until `broker.remote_sessions("friend")` becomes empty. Reconnect same node and publish again.

Core test body:

```python
@pytest.mark.asyncio
async def test_abrupt_close_cleans_up_and_allows_reconnect(caplog) -> None:
    authenticator = TokenAuthenticator.from_json('{"andre":"secret"}')
    broker = Broker()

    async with serve(
        lambda websocket: handle_websocket(websocket, broker, authenticator),
        "127.0.0.1",
        0,
        process_request=make_process_request(authenticator),
    ) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://127.0.0.1:{port}"
        client = await connect(url, additional_headers={"Authorization": "Bearer secret"})
        await client.send(
            json.dumps(
                {
                    "type": "publish",
                    "sessions": [
                        {"name": "manager", "session_id": "one", "mode": "bypass"}
                    ],
                }
            )
        )
        client.transport.abort()
        await _wait_for_remote_sessions(broker, "friend", [])

        async with connect(
            url,
            additional_headers={"Authorization": "Bearer secret"},
        ) as replacement:
            await replacement.send('{"type":"publish","sessions":[]}')

    assert "abnormal WebSocket close for node andre" in caplog.text
    assert "secret" not in caplog.text
```

Add this bounded async helper to `tests/test_server_network.py`:

```python
async def _wait_for_remote_sessions(
    broker: Broker,
    node_id: str,
    expected: list[dict[str, str]],
) -> None:
    async with asyncio.timeout(2):
        while await broker.remote_sessions(node_id) != expected:
            await asyncio.sleep(0.01)
```

Import `asyncio`, `connect`, `serve`, `Broker`, `handle_websocket`, and `make_process_request` in that test file.

- [ ] **Step 2: Run network test and verify RED**

Run:

```bash
uv run --extra dev pytest tests/test_server_network.py::test_abrupt_close_cleans_up_and_allows_reconnect -q
```

Expected: FAIL because handler emits uncaught `ConnectionClosedError` and expected log is absent.

- [ ] **Step 3: Catch abnormal closure and keep conditional cleanup**

Import:

```python
from websockets.exceptions import ConnectionClosedError
```

Add before `except BrokerError`:

```python
except ConnectionClosedError as error:
    logging.warning(
        "abnormal WebSocket close for node %s: %s",
        node_id,
        error,
    )
```

Do not log request headers, tokens, message bodies, or session IDs.

- [ ] **Step 4: Run network and server tests**

Run:

```bash
uv run --extra dev pytest tests/test_server_network.py tests/test_server.py -q
```

Expected: PASS with no unhandled task exception.

### Task 5A: Harden reviewed lifecycle races

**Files:**
- Modify: `src/distributedcc/protocol.py`
- Modify: `src/distributedcc/server.py`
- Modify: `src/distributedcc/bridge.py`
- Test: `tests/test_server.py`
- Test: `tests/test_bridge.py`

- [ ] **Step 1: Keep replacement activation independent from old close**

Add an owned `Broker._retirement_tasks` set. Schedule old transport close with private code `4001` in a task that catches and logs close failure, removes itself from task set on completion, and never delays `attach()` return. Test with old connection whose `close()` blocks until released; replacement must publish before release.

- [ ] **Step 2: Drain every connection-scoped task result**

Store receiver, sender, and watcher tasks in deterministic tuple. After first completion, cancel pending tasks and gather all tasks with `return_exceptions=True`. Translate private close code `4001` to `BridgeSuperseded`, propagate first remaining failure only after every result is observed, and raise `ConnectionError("broker connection closed")` after clean completion so normal cleanup and backoff run.

- [ ] **Step 3: Prevent displaced bridge eviction loops**

When `run_bridge()` receives `BridgeSuperseded`, clear virtual peers and wait indefinitely until process cancellation. This keeps `restart: unless-stopped` container alive instead of exiting and reclaiming node token.

- [ ] **Step 4: Bind delivery to target session generation**

Add broker-owned `target_session_id` to delivery schema. Keep latest published local snapshot in connection-scoped `LocalSessionState`. Before injection, require delivery target ID to match both snapshot and freshly rediscovered local session ID. Drop disappearance or restart races without disconnecting broker connection.

- [ ] **Step 5: Isolate target transport send failure**

If target `send_json()` fails, conditionally detach only that target connection generation, broadcast updated presence, and raise `BrokerError` to healthy source handler. Test that source remains able to publish.

- [ ] **Step 6: Run hardening regressions**

Run:

```bash
uv run --extra dev pytest tests/test_server.py tests/test_bridge.py -q
```

Expected: PASS without warnings or unobserved task exceptions.

---

### Task 6: Update operating documentation

**Files:**
- Modify: `README.md:8-19,95-161,190-198`
- Modify: `docs/architecture.md:80-130`

- [ ] **Step 1: Update tested-version statement only if verification confirms current version**

Run:

```bash
claude --version
```

If output is `2.1.296`, update `TESTED_CLAUDE_CODE_VERSION` in `src/distributedcc/registry.py`, related test fixtures, README warning, and architecture references to `2.1.296`. If runtime differs, retain `2.1.294` and document actual tested version from integration run instead of guessing.

- [ ] **Step 2: Document idle snapshot behavior**

Add after client startup instructions in `README.md`:

```markdown
The bridge connects even when none of its configured sessions are running. It publishes an empty snapshot, watches local Claude Code sessions, and announces each configured session when it appears. Stopping a session removes its advertisement without disconnecting the bridge.
```

- [ ] **Step 3: Document permission-mode parity and trusted receiver setting**

Add:

```markdown
Claude Code applies its normal inbound permission-mode parity checks to relayed messages. A mismatched message is held for approval. For a dedicated trusted unattended receiver only, start that session with:

```sh
claude --settings '{"crossSessionInbound":"accept","isolatePeerMachines":true}' \
  --agent yrrp-release-manager --name yrrp-release-manager
```

`crossSessionInbound: "accept"` delivers admitted peer messages but does not approve tool calls. Keep this session-scoped instead of enabling it globally.
```

- [ ] **Step 4: Update architecture lifecycle and trust text**

State that newest authenticated connection supersedes stale same-node connection, broker binds mutations to connection identity, publication is complete-snapshot replacement, and event-time native `from-mode` is authoritative. Remove claim that command-line process inference alone preserves mode parity.

- [ ] **Step 5: Check documentation diff**

Run:

```bash
git diff --check -- README.md docs/architecture.md
```

Expected: exit 0, no output.

---

### Task 7: Full verification and focused review

**Files:**
- Review all modified source, tests, and docs

- [ ] **Step 1: Run full test suite**

Run:

```bash
uv run --extra dev pytest -q
```

Expected: all tests PASS with no warnings or unhandled task exceptions.

- [ ] **Step 2: Run Ruff**

Run:

```bash
uv run --extra dev ruff check .
```

Expected: `All checks passed!`

- [ ] **Step 3: Check diff hygiene**

Run:

```bash
git diff --check
```

Expected: exit 0, no output.

- [ ] **Step 4: Review security invariants**

Confirm from diff:

- broker derives node from bearer-authenticated connection;
- broker validates source-session ownership;
- event mode accepts only `bypass|prompting`;
- destination envelope uses broker metadata, not arbitrary sender envelope text;
- stale connection cannot publish, route, or detach replacement;
- logs contain no tokens or bodies;
- absent native mode becomes `prompting`.

Applicable references: OWASP ASVS 5.0.0 V4.4.3, `WebSocket_Security_Cheat_Sheet#denial-of-service-protection`, and `WebSocket_Security_Cheat_Sheet#security-monitoring-and-logging`.

- [ ] **Step 5: Run maintainability and correctness review**

Review changed code for functions over 50 lines, nesting beyond three levels, duplicated snapshot logic, uncancelled tasks, stale-generation races, and swallowed send failures. Convert each verified issue into a failing regression test before changing production code.

- [ ] **Step 6: Perform supervised two-machine acceptance**

Follow design spec sequence in `docs/superpowers/specs/2026-10-14-bridge-lifecycle-design.md`. Record broker logs proving stable idle connection, appearance/disappearance without reconnect, reboot takeover, expected mode hold, and session-scoped `accept` delivery.

Do not claim two-machine behavior verified if only automated tests ran.
