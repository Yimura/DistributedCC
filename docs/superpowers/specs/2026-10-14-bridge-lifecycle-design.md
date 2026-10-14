# Bridge lifecycle and permission-mode design

## Goals

DistributedCC must:

- keep each bridge connected when zero configured local sessions are live;
- announce configured sessions when they appear, change, restart, or disappear;
- recover promptly when a restarted bridge reconnects before the broker detects its old dead transport;
- treat abrupt WebSocket loss as an expected lifecycle event without an unhandled traceback;
- preserve Claude Code's actual sender permission-mode class across the broker;
- retain Claude Code's inbound mode-parity policy unless a trusted receiver explicitly opts into `crossSessionInbound: "accept"`.

## Current root causes

`run_bridge()` resets reconnect backoff immediately after the WebSocket handshake, then calls `_selected_local_sessions()`. That helper raises when any configured session is absent. The healthy connection closes and retries every second. Discovery runs only at connection startup and delivery time, so an idle bridge cannot announce a later session.

`Broker.attach()` rejects every second connection for a node until the first handler exits. After reboot or network loss, the old connection remains registered until TCP or ping timeout detects failure. `handle_websocket()` cleans up in `finally`, but it does not catch `ConnectionClosedError`, so expected abrupt loss produces a traceback.

Permission mode traverses the current protocol, but `infer_permission_mode()` classifies only `--dangerously-skip-permissions` as bypass. Claude Code can enter the bypass class through settings or runtime Plan mode, so the bridge can advertise a false `prompting` class. Claude Code then correctly holds a message whose asserted class differs from the receiver.

## Architecture

### Connection-scoped bridge tasks

One broker connection owns three concurrent tasks:

1. inbound broker receiver;
2. outbound local-peer sender;
3. local-session snapshot watcher.

If any task completes or fails, the connection scope cancels and awaits the remaining tasks before reconnecting. This replaces `FIRST_EXCEPTION`, which can hang when the receiver completes normally.

Reconnect backoff resets only after application-level initialization succeeds. Missing local sessions are valid state and never trigger reconnect. Broker rejection or early task failure retains increasing bounded backoff.

### Local-session snapshot watcher

The watcher polls existing `discover_live_sessions()` validation at a named interval. It filters discovery to configured names without requiring every name to exist.

The watcher immediately publishes a complete snapshot, including `[]`. It then publishes only when protocol-visible fields change:

- session name;
- session ID;
- permission-mode hint.

Each publication replaces that node's prior broker snapshot. Empty publication removes stale advertisements while preserving the node connection. Partial publication advertises every configured session currently live.

The bridge stores the latest validated snapshot in connection-scoped state. Each broker delivery carries target session ID from broker-owned advertisement. Delivery targeting requires that ID to match both published snapshot and freshly rediscovered local process before socket injection. Local target disappearance or restart drops that delivery without classifying it as broker transport failure.

### Same-node connection replacement

A newly authenticated connection replaces the broker's current connection for that node. Replacement clears old published sessions and closes the superseded WebSocket where practical.

Every broker operation carries the connection object as its generation identity:

- `publish(node_id, connection, advertisements)` accepts only the current connection;
- `route(node_id, connection, event)` accepts only the current connection;
- `detach(node_id, connection)` removes state only if that connection remains current.

This prevents a superseded handler from publishing, routing, or detaching its replacement. Replacement uses a private close code that makes a still-live displaced bridge park until restart instead of reconnecting and evicting its replacement. Replacement broadcasts one authoritative presence snapshot. A stolen node token already grants that node identity; replacement does not create a broader authorization capability than current token possession.

### WebSocket closure handling

`handle_websocket()` catches `websockets.exceptions.ConnectionClosed` around its receive loop. It logs abnormal closure once with node identity and close details, then relies on conditional `detach()` in `finally`.

Logs exclude bearer tokens, message bodies, and session IDs. Existing ping/pong remains responsible for detecting dead transports that produce no immediate TCP failure.

This follows `WebSocket_Security_Cheat_Sheet#denial-of-service-protection` for heartbeat cleanup and `WebSocket_Security_Cheat_Sheet#security-monitoring-and-logging` for abnormal-disconnect logging.

## Permission-mode propagation

### Event-time mode source

The native message sent by Claude Code to a virtual peer already contains an optional canonical `from-mode="bypass|prompting"` attribute. `decode_native_send()` will return both body and asserted mode instead of discarding mode.

The bridge authenticates the local sender through Unix peer credentials and live-session registry matching before accepting this frame. It sends event-time source mode in the route message. The broker validates the enum and derives node/session identity from its authenticated connection and publication ownership.

If the native frame omits `from-mode`, the bridge uses `prompting`. This fails safely: it can cause a hold at a bypass receiver but cannot falsely grant bypass-class trust.

The destination bridge reconstructs the canonical peer envelope from broker delivery metadata. It never forwards sender-supplied address, name, session ID, or arbitrary envelope text.

### Publication mode hint

Session advertisements retain a mode hint for compatibility and presence metadata. Command-line inference may recognize documented explicit permission-mode flags, but it is not authoritative for delivery. Runtime mode can change after launch; event-time `from-mode` controls each routed message.

### Receiver policy

DistributedCC preserves Claude Code's default inbound rules:

- prompting receivers hold messages from bypass senders;
- bypass receivers hold messages unless sender also attests bypass;
- matching classes deliver automatically.

DistributedCC does not rewrite mode to match target. A deliberately trusted unattended receiver may start with session-scoped configuration:

```sh
claude --settings '{"crossSessionInbound":"accept","isolatePeerMachines":true}'
```

`crossSessionInbound: "accept"` delivers admitted peer messages but does not approve receiver tool calls. Session-scoped `--settings` avoids enabling acceptance for unrelated sessions. `isolatePeerMachines` controls native cross-machine `SendMessage`; it does not gate DistributedCC's same-machine virtual socket, but remains useful defense in depth.

## Protocol changes

Route messages gain required `source_mode` with enum `bypass|prompting`. Broker delivery carries `source_mode` and broker-authorized `target_session_id`.

The protocol remains snapshot-based for publication. Empty session arrays stay valid and receive explicit regression coverage.

Protocol validation rejects unknown fields, invalid mode values, oversized payloads, forged source ownership, and stale connection generations.

## Error handling

- Missing configured sessions produce empty or partial snapshots, not exceptions.
- Local discovery read failures omit invalid records and retry on next poll.
- Local target loss rejects only affected delivery and keeps broker connection alive.
- Broker protocol errors end connection scope and use bounded reconnect backoff.
- Superseded connections cannot mutate broker state.
- Target delivery transport failure retires only failed target generation and reports routing error without disconnecting healthy source.
- Abrupt WebSocket closure logs once and cleans up without framework traceback.
- Clean receiver completion cancels sibling tasks and reconnects.

## Verification

Implementation uses red-green tests before production changes.

### Unit and component tests

Bridge tests cover:

- initial empty snapshot without reconnect;
- later session appearance on same connection;
- disappearance publishing `[]`;
- partial configured-session availability;
- unchanged snapshots producing no duplicate publish;
- session ID or mode-hint changes producing a publish;
- clean task completion cancelling siblings;
- application-level failures retaining backoff;
- delivery using current validated target state.

Protocol and virtual-peer tests cover:

- empty publication acceptance;
- native `from-mode` extraction;
- missing native mode falling back to `prompting`;
- route-mode validation;
- destination envelope reconstruction from broker metadata.

Broker tests cover:

- new same-node connection replacing old;
- old detach leaving replacement intact;
- superseded publish and route rejection;
- old handler cleanup not broadcasting false removal;
- empty publication removing sessions while retaining connection.

### Network integration tests

An ephemeral WebSocket test aborts client transport without a close frame. It verifies node/session cleanup, replacement connection success, one abnormal-close log, and no unhandled `ConnectionClosedError`.

A connection-replacement test keeps the old socket open while a new authenticated socket attaches. It verifies immediate takeover and stale-handler isolation.

### Repository and runtime checks

Run full pytest suite, Ruff checks, and `git diff --check`. Then run a supervised two-machine check:

1. Start bridge with no local configured session and confirm one stable broker connection.
2. Start configured Claude Code session and confirm remote presence appears without bridge reconnect.
3. Stop session and confirm remote presence disappears while bridge stays connected.
4. Reboot or abort one bridge host and confirm replacement connects without duplicate-node loop or server traceback.
5. Send matching-mode message and confirm automatic delivery.
6. Send mismatched-mode message and confirm Claude Code holds it.
7. Start dedicated receiver with session-scoped `crossSessionInbound: "accept"` and confirm cross-mode delivery while receiver-side tool permissions remain enforced.

## Compatibility and documentation

Integration depends on private Claude Code registry, socket, and envelope details. Version warnings remain. Tests pin observed envelope behavior, and architecture/deployment documentation will explain:

- stable idle connections;
- snapshot publication semantics;
- newest-connection takeover;
- session-scoped `crossSessionInbound` for trusted unattended receivers;
- mode parity as default security behavior;
- private-protocol compatibility risk.
