# DistributedCC MVP architecture

## Purpose

DistributedCC lets Claude Code sessions on different laptops communicate through native peer-origin events. Primary use case connects contributor laptop to dedicated `yrrp-release-manager` session that controls shared release resources.

Each laptop runs dedicated bridge client. Central public server routes events between clients. Remote sessions appear as transient local Claude Code peers, so existing `ListAgents` and `SendMessage` behavior remains usable.

## Agreed scope

MVP provides:

- event-driven, bidirectional Claude Code session communication;
- plain WebSocket broker intended for an operator-supplied TLS reverse proxy;
- one dedicated bridge client per laptop;
- one unique pre-shared bearer secret per laptop;
- transient virtual sessions visible through `ListAgents`;
- native peer-origin delivery into target agent context;
- containerized broker and bridge deployments;
- tested compatibility with Claude Code 2.1.296; other versions continue with a warning.

MVP excludes:

- lease management;
- durable queues and offline delivery;
- message history;
- browser clients;
- MCP;
- Claude Code setting changes;
- guaranteed compatibility with untested Claude Code versions.

`yrrp-release-manager` remains coordination authority. Its instructions serialize access to shared resources.

## Components

### Broker

Broker runs as a plain Python WebSocket service bound to loopback by default. An operator-supplied reverse proxy terminates public TLS and forwards authenticated WSS connections to the broker port.

Broker keeps volatile directory of connected nodes and published sessions. It routes semantic peer events by globally unique session name. A newly authenticated connection replaces an older connection for the same node. Broker activates replacement immediately and retires old transport in an owned background task, so a half-open old socket cannot block recovery. Every publish, route, and detach operation must come from the node's current connection, so stale handlers cannot mutate replacement state. Displaced live bridge recognizes private superseded close code and parks until restart, preventing two bridges sharing one token from evicting each other forever.

Broker does not execute commands, inspect repositories, manage Android devices, or control build infrastructure.

### Laptop bridge

Each laptop runs one dedicated bridge client. Client connects to broker over WSS and presents bearer secret in `Authorization` header.

Server maps secret to fixed node identity. Client cannot select different node identity. Each laptop gets different secret, so revoking one laptop does not invalidate every client.

Bridge discovers selected local Claude Code sessions from `~/.claude/sessions`. It keeps broker connection open with zero live sessions and publishes complete snapshots only when selected live sessions change. Each snapshot contains only sessions whose PID and process start token are valid and whose peer protocol matches. Empty snapshot removes stale advertisements without disconnecting node. A Claude Code version mismatch logs a compatibility warning but does not block publication.

### Virtual peers

Bridge mirrors each remote session as local virtual peer:

1. Bridge starts one worker process for remote session.
2. Worker creates mode `0600` Unix socket under `/run/user/<uid>/cc-socks/`.
3. Worker writes transient record under `~/.claude/sessions/` with its real host PID and process start token.
4. Claude Code discovers record through `ListAgents`.
5. Native `SendMessage` connects to worker socket.
6. Worker verifies socket peer is registered, live, same-user local Claude Code process.
7. Bridge sends semantic event to broker.
8. Destination bridge requires broker-authorized target session ID to match both published snapshot and current live process, then reconstructs canonical peer envelope and injects event into real target session socket.

Bridge removes owned records and sockets when remote session disappears or bridge stops.

## Container boundary

Broker stack uses ordinary isolated containers.

Bridge container requires host integration:

- `pid: host` so generated session records contain host-visible PIDs;
- host user and group IDs so Unix socket ownership matches Claude Code user;
- writable bind mount for `~/.claude/sessions`;
- writable bind mount for `/run/user/<uid>/cc-socks`.

These privileges are necessary because Claude Code validates live process identity and Unix socket ownership. Bridge image must remain small and dedicated to this function.

## Identity and authentication

Each laptop has unique random bearer secret. Broker configuration maps secret to node ID. Client sends secret only through WSS `Authorization: Bearer ...` handshake header.

Session names must be globally unique. Existing manager keeps exact name `yrrp-release-manager`. Broker rejects duplicate publication.

Broker derives source node from authenticated connection. Source session must be published by that connection. Broker rejects source names not owned by connection.

Destination bridge builds peer envelope from broker-validated metadata. It never forwards sender-supplied `from`, `from-name`, `from-session`, or arbitrary envelope text. Source bridge authenticates local sender through Unix peer credentials and live registry matching, extracts canonical event-time `from-mode`, and sends only validated enum. Envelope `from` points to source session's local mirrored Unix socket so automatic replies remain native.

## Peer-origin behavior

Claude Code wire frame still has `type: "user"`, but receiver converts it to internal peer origin after parsing canonical `<cross-session-message>` envelope. Target model receives cross-session event, not ordinary human prompt.

Permission-mode class travels as `from-mode="bypass"` or `from-mode="prompting"`. Source bridge reads class from native event at send time; missing class falls back to `prompting`. This preserves runtime mode changes and fails safely at bypass receiver.

Claude Code keeps its default mode-parity policy. Mismatched messages are held for approval. A dedicated trusted unattended receiver can set session-scoped `crossSessionInbound: "accept"`; this delivers peer messages but does not approve receiver tool calls.

Busy target queues event between tool calls. Idle target starts turn. This is same receive behavior as local `SendMessage`.

## Security boundaries

TLS provides confidentiality and integrity between laptop clients and public broker. Bearer secret authenticates laptop, not person or individual Claude session.

Bridge must authenticate local socket sender through operating-system peer credentials and live Claude registry. Any process running as same Unix user can read user-owned files, so local-user compromise remains outside MVP boundary.

Peer message requests do not grant authorization. `yrrp-release-manager` still applies its agent instructions, permission checks, review gates, and explicit release approval.

Broker enforces message schema, size limits, destination ownership, duplicate session rejection, connection limits, and bounded queues. Logs exclude bearer secrets and full message bodies.

## Failure behavior

Bridge fails closed when:

- Claude Code version metadata is missing or malformed;
- peer protocol differs from `1`;
- target session PID or start token is stale;
- target socket ownership or permissions are unsafe;
- local sender is not registered live Claude process;
- broker authentication fails;
- source session is not owned by authenticated connection;
- remote destination is absent;
- frame is malformed or oversized.

Broker restart clears presence and in-flight events. Clients reconnect with bounded exponential backoff and republish current snapshots. Early application failures retain increasing backoff; stable connections reset it. Abrupt transport loss logs one abnormal close and conditionally removes only current connection state.

## Known limitations

Integration uses private Claude Code implementation details. Future Claude Code release may change session records, socket validation, envelope syntax, or message schema.

Initial send success means local bridge accepted event. It does not prove remote agent acted on request. Durable acknowledgements remain future work.

Virtual peers use transient registry records. They are not genuine Claude Code processes, despite behaving as discoverable messaging endpoints.

## Future extensions

Potential later work:

- durable delivery acknowledgements and replay protection;
- offline queues;
- per-session authorization policies;
- generic resource leasing with fencing tokens;
- operator dashboard;
- support matrix for newer Claude Code versions.
