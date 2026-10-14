# DistributedCC

DistributedCC bridges Claude Code sessions across Linux machines while preserving native `ListAgents`, `SendMessage`, and peer-origin event delivery.

> [!IMPORTANT]
> DistributedCC is Linux-only. It has only been built and tested on Linux. The bridge depends on Linux `/proc`, Unix-domain sockets, `SO_PEERCRED`, host PID namespaces, and `/run/user/<uid>`.

> [!WARNING]
> DistributedCC uses private Claude Code session-registry and Unix-socket protocols. Version 2.1.294 is tested. Other valid Claude Code versions produce a warning but remain usable; protocol changes may still break messaging.

Architecture and trust boundaries: [`docs/architecture.md`](docs/architecture.md).

## How it works

Each machine runs a bridge client with its own pre-shared secret. Clients connect to a central broker over secure WebSockets. The broker maps each secret to a fixed node identity and routes messages by globally unique Claude session name.

The bridge mirrors remote sessions into local `~/.claude/sessions` and `/run/user/<uid>/cc-socks`. Remote sessions then appear in `ListAgents`. Native `SendMessage` reaches the virtual socket, crosses the broker, and enters the target Claude session as a peer-origin event.

DistributedCC does not provide lease management. Agents such as `yrrp-release-manager` remain responsible for coordination and authorization.

## Requirements

- Linux with `/proc`, Unix peer credentials, and `/run/user/<uid>`
- Docker Engine with Compose
- Claude Code 2.1.294 tested; other versions allowed with a compatibility warning
- globally unique published Claude session names
- an external reverse proxy that terminates TLS and supports WebSocket upgrades

Published container image:

```text
ghcr.io/yimura/distributedcc:latest
```

## Deploy the broker

Clone this repository on the public broker host:

```bash
git clone https://github.com/Yimura/DistributedCC.git
cd DistributedCC
cp .env.example .env
```

Generate one secret for each client machine:

```bash
openssl rand -hex 32
openssl rand -hex 32
```

Edit `.env` and configure broker binding plus a distinct token for each node:

```dotenv
DCC_IMAGE=ghcr.io/yimura/distributedcc:latest
DCC_BIND_ADDRESS=127.0.0.1
DCC_PUBLISHED_PORT=8765
DCC_NODE_TOKEN_ANDRE=<andre-secret>
DCC_NODE_TOKEN_FRIEND=<friend-secret>
```

`compose.server.yaml` renders these values into readable multiline JSON in `DCC_NODE_TOKENS`. This avoids inline-config injection, which fails with read-only containers on some deployment engines. To add or rename nodes, edit the JSON map and matching `.env` variables.

Node names such as `andre` and `friend` are arbitrary stable machine labels. They do not need to match host usernames, hostnames, or Claude session names. `DCC_UID` and `DCC_GID` are separate and must match the host user running Claude Code.

Start the broker:

```bash
docker compose -f compose.server.yaml pull
docker compose -f compose.server.yaml up -d
```

The broker exposes insecure WebSocket traffic at `ws://127.0.0.1:8765` by default. Configure your external reverse proxy to terminate TLS and forward WebSocket upgrades to that address. Clients still connect through `wss://<your-domain>`.

Verify the broker directly on its host:

```bash
curl http://127.0.0.1:8765/healthz
```

Expected response:

```text
OK
```

Follow broker logs with:

```bash
docker compose -f compose.server.yaml logs -f broker
```

The broker logs successful startup, node connections and disconnections, publication counts, routed message metadata, and rejected requests. It never logs bearer tokens or message bodies.

## Set up a client machine

Start Claude Code with a globally unique name. For a release manager:

```bash
claude --agent yrrp-release-manager --name yrrp-release-manager
```

For another contributor session:

```bash
claude --name friend-feature
```

Clone this repository on the client and create its local environment file:

```bash
git clone https://github.com/Yimura/DistributedCC.git
cd DistributedCC
cp .env.example .env
```

Edit `.env` with this machine's token, exact Claude session name, and host user and group IDs:

```dotenv
DCC_IMAGE=ghcr.io/yimura/distributedcc:latest
DCC_SERVER_URL=wss://dcc.example.com
DCC_TOKEN=<this-machine-secret>
DCC_PUBLISH_SESSIONS=friend-feature
DCC_UID=1000
DCC_GID=1000
```

Get the correct IDs with:

```bash
id -u
id -g
```

Start the bridge:

```bash
docker compose -f compose.bridge.yaml pull
docker compose -f compose.bridge.yaml up -d
```

The bridge container runs with the host PID namespace and host UID/GID. It also mounts `~/.claude/sessions` and `/run/user/<uid>/cc-socks`. These permissions let Claude Code validate virtual peer PIDs, socket ownership, and process start tokens.

After two bridges connect, each published remote session appears in native `ListAgents`. Either session can use native `SendMessage`; an idle recipient starts a turn, while a busy recipient receives the event between tool calls.

Every session that sends or receives through the broker must appear in its bridge's `DCC_PUBLISH_SESSIONS`. Publication is bidirectional. An unpublished session cannot receive remote messages, and the broker rejects messages claiming it as their source.

## Publish more than one local session

Start each Claude Code session with a unique name, then use a comma-separated list:

```dotenv
DCC_PUBLISH_SESSIONS=session-one,session-two
```

Restart the bridge after changing the list:

```bash
docker compose -f compose.bridge.yaml up -d
```

## Build from source

Build a local image and tell Compose to use it:

```bash
docker build -t distributedcc:dev .
DCC_IMAGE=distributedcc:dev docker compose -f compose.bridge.yaml up -d
```

For Python development:

```bash
uv sync --extra dev
uv run --extra dev pytest -q
uv run --extra dev ruff check .
```

Run the broker directly for trusted local tests:

```bash
export DCC_NODE_TOKENS='{"andre":"andre-secret","friend":"friend-secret"}'
uv run distributedcc-server
```

Never expose plain `ws://` traffic publicly. Keep the broker on loopback or a trusted private network, and terminate public WSS at your reverse proxy.

## Stop and clean up

Stop a bridge cleanly:

```bash
docker compose -f compose.bridge.yaml down
```

The bridge removes owned virtual session records and sockets during normal shutdown. If the container is killed, Claude Code ignores stale records after the worker PID or process start token no longer matches.

## Container publishing

[GitHub Actions](.github/workflows/docker-publish.yaml) builds the shared broker and bridge image on every push to `main`. It publishes `latest` and commit-SHA tags to GitHub Container Registry. OCI labels and tags come from GitHub repository and commit metadata.
