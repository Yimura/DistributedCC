# DistributedCC MVP architecture

## Purpose

DistributedCC lets Claude Code sessions on different laptops communicate through native peer-origin events. Primary use case connects contributor laptop to dedicated `yrrp-release-manager` session that controls shared release resources.

Each laptop runs dedicated bridge client. Central public server routes events between clients. Remote sessions appear as transient local Claude Code peers, so existing `ListAgents` and `SendMessage` behavior remains usable.

## Agreed scope

MVP provides:

- event-driven, bidirectional Claude Code session communication;
- public TLS WebSocket broker;
- one dedicated bridge client per laptop;
- one unique pre-shared bearer secret per laptop;
- transient virtual sessions visible through `ListAgents`;
- native peer-origin delivery into target agent context;
- containerized broker and bridge deployments;
- compatibility with Claude Code 2.1.294.

MVP excludes:

- lease management;
- durable queues and offline delivery;
- message history;
- browser clients;
- MCP;
- Claude Code setting changes;
- support for unverified Claude Code versions.

`yrrp-release-manager` remains coordination authority. Its instructions serialize access to shared resources.

## Components

### Broker

Broker runs as Python WebSocket service behind Caddy. Caddy terminates public TLS and forwards authenticated WSS connections to broker container.

Broker keeps volatile directory of connected nodes and published sessions. It routes semantic peer events by globally unique session name.

Broker does not execute commands, inspect repositories, manage Android devices, or control build infrastructure.

### Laptop bridge

Each laptop runs one dedicated bridge client. Client connects to broker over WSS and presents bearer secret in `Authorization` header.

Server maps secret to fixed node identity. Client cannot select different node identity. Each laptop gets different secret, so revoking one laptop does not invalidate every client.

Bridge discovers selected local Claude Code sessions from `~/.claude/sessions`. It publishes only live sessions whose PID, process start token, Claude Code version, and peer protocol match expected values.

### Virtual peers

Bridge mirrors each remote session as local virtual peer:

1. Bridge starts one worker process for remote session.
2. Worker creates mode `0600` Unix socket under `/run/user/<uid>/cc-socks/`.
3. Worker writes transient record under `~/.claude/sessions/` with its real host PID and process start token.
4. Claude Code discovers record through `ListAgents`.
5. Native `SendMessage` connects to worker socket.
6. Worker verifies socket peer is registered, live, same-user local Claude Code process.
7. Bridge sends semantic event to broker.
8. Destination bridge reconstructs canonical peer envelope and injects event into real target session socket.

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

Destination bridge builds peer envelope from authenticated broker metadata. It does not trust sender-supplied `from`, `from-name`, `from-session`, or `from-mode` fields.

## Peer-origin behavior

Claude Code wire frame still has `type: "user"`, but receiver converts it to internal peer origin after parsing canonical `<cross-session-message>` envelope. Target model receives cross-session event, not ordinary human prompt.

Permission-mode class travels as `from-mode="bypass"` or `from-mode="prompting"`. Bridge derives class from real sender process invocation. This preserves Claude Code mode-parity policy and avoids untrusted mode assertions.

Busy target queues event between tool calls. Idle target starts turn. This is same receive behavior as local `SendMessage`.

## Security boundaries

TLS provides confidentiality and integrity between laptop clients and public broker. Bearer secret authenticates laptop, not person or individual Claude session.

Bridge must authenticate local socket sender through operating-system peer credentials and live Claude registry. Any process running as same Unix user can read user-owned files, so local-user compromise remains outside MVP boundary.

Peer message requests do not grant authorization. `yrrp-release-manager` still applies its agent instructions, permission checks, review gates, and explicit release approval.

Broker enforces message schema, size limits, destination ownership, duplicate session rejection, connection limits, and bounded queues. Logs exclude bearer secrets and full message bodies.

## Failure behavior

Bridge fails closed when:

- Claude Code version differs from `2.1.294`;
- peer protocol differs from `1`;
- target session PID or start token is stale;
- target socket ownership or permissions are unsafe;
- local sender is not registered live Claude process;
- broker authentication fails;
- source session is not owned by authenticated connection;
- remote destination is absent;
- frame is malformed or oversized.

Broker restart clears presence and in-flight events. Clients reconnect with bounded exponential backoff and republish live sessions.

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
