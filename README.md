# DistributedCC

DistributedCC bridges Claude Code sessions across Linux machines while preserving native `ListAgents`, `SendMessage`, and peer-origin event delivery.

> [!IMPORTANT]
> DistributedCC is Linux-only. It has only been built and tested on Linux. The bridge depends on Linux `/proc`, Unix-domain sockets, `SO_PEERCRED`, host PID namespaces, and `/run/user/<uid>`.

> [!WARNING]
> DistributedCC uses private Claude Code 2.1.294 session-registry and Unix-socket protocols. It fails closed on unsupported versions, but Claude Code updates may require bridge changes.

Architecture and trust boundaries: [`docs/architecture.md`](docs/architecture.md).

## How it works

Each machine runs a bridge client with its own pre-shared secret. Clients connect to a central broker over secure WebSockets. The broker maps each secret to a fixed node identity and routes messages by globally unique Claude session name.

The bridge mirrors remote sessions into local `~/.claude/sessions` and `/run/user/<uid>/cc-socks`. Remote sessions then appear in `ListAgents`. Native `SendMessage` reaches the virtual socket, crosses the broker, and enters the target Claude session as a peer-origin event.

DistributedCC does not provide lease management. Agents such as `yrrp-release-manager` remain responsible for coordination and authorization.

## Requirements

- Linux with `/proc`, Unix peer credentials, and `/run/user/<uid>`
- Docker Engine with Compose
- Claude Code 2.1.294 on every client machine
- globally unique published Claude session names
- a broker host with a public DNS name and inbound TCP ports 80 and 443

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

Edit `.env` and configure the public domain plus a distinct token for each node:

```dotenv
DCC_IMAGE=ghcr.io/yimura/distributedcc:latest
DCC_DOMAIN=dcc.example.com
DCC_NODE_TOKENS={"andre":"<andre-secret>","friend":"<friend-secret>"}
```

Point the domain's DNS record at the broker host, then start the stack:

```bash
docker compose -f compose.server.yaml pull
docker compose -f compose.server.yaml up -d
```

Caddy obtains the TLS certificate and exposes the broker at `wss://<DCC_DOMAIN>`. Verify it:

```bash
curl https://<DCC_DOMAIN>/healthz
```

Expected response:

```text
OK
```

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

Run the broker without Caddy only for trusted local tests:

```bash
export DCC_NODE_TOKENS='{"andre":"andre-secret","friend":"friend-secret"}'
uv run distributedcc-server
```

Use `ws://127.0.0.1:8765` only for local testing. Public deployments must use WSS through Caddy.

## Stop and clean up

Stop a bridge cleanly:

```bash
docker compose -f compose.bridge.yaml down
```

The bridge removes owned virtual session records and sockets during normal shutdown. If the container is killed, Claude Code ignores stale records after the worker PID or process start token no longer matches.

## Container publishing

[GitHub Actions](.github/workflows/docker-publish.yaml) builds the shared broker and bridge image on every push to `main`. It publishes `latest` and commit-SHA tags to GitHub Container Registry. OCI labels and tags come from GitHub repository and commit metadata.
