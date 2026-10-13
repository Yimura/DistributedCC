# DistributedCC

DistributedCC bridges Claude Code 2.1.294 sessions across machines while preserving `ListAgents`, native `SendMessage`, and peer-origin event delivery.

Architecture baseline: [`docs/architecture.md`](docs/architecture.md).

> [!WARNING]
> DistributedCC uses private Claude Code session-registry and Unix-socket protocols. It fails closed on unsupported versions, but Claude Code updates may still require bridge changes.

## How it works

Each laptop runs dedicated bridge client with unique pre-shared secret. Clients connect to central broker over WSS. Broker maps each secret to fixed node identity and routes messages by globally unique Claude session name.

Bridge mirrors remote sessions into local `~/.claude/sessions` and `/run/user/<uid>/cc-socks`. Remote sessions then appear in `ListAgents`. Native `SendMessage` reaches virtual socket, crosses broker, and enters target Claude session as peer-origin event.

No lease service exists in MVP. `yrrp-release-manager` remains coordination authority.

## Requirements

- Linux with Unix peer credentials and `/proc`
- Docker Engine with Compose
- public DNS name pointing to broker host
- inbound TCP 80 and 443 to broker host
- Claude Code 2.1.294 on both laptops
- globally unique published session names

## Create laptop secrets

Create separate secret for each laptop:

```bash
openssl rand -hex 32
openssl rand -hex 32
```

Server maps secrets to node identities:

```dotenv
DCC_NODE_TOKENS={"andre":"<andre-secret>","friend":"<friend-secret>"}
```

Do not reuse one secret across laptops. Do not put secrets in Git.

## Run public broker

Copy `.env.example` to `.env`, set `DCC_DOMAIN` and `DCC_NODE_TOKENS`, then run:

```bash
docker compose -f compose.server.yaml up -d --build
```

Caddy obtains TLS certificate and exposes broker at `wss://<DCC_DOMAIN>`.

Health check:

```bash
curl https://<DCC_DOMAIN>/healthz
```

Expected response:

```text
OK
```

## Run bridge on release-manager laptop

Start dedicated manager session:

```bash
claude --agent yrrp-release-manager --name yrrp-release-manager
```

Set bridge environment:

```dotenv
DCC_SERVER_URL=wss://dcc.example.com
DCC_TOKEN=<andre-secret>
DCC_PUBLISH_SESSIONS=yrrp-release-manager
DCC_UID=1000
DCC_GID=1000
```

Use actual IDs from:

```bash
id -u
id -g
```

Run bridge:

```bash
docker compose -f compose.bridge.yaml up -d --build
```

## Run bridge on contributor laptop

Contributor starts named Claude session, for example:

```bash
claude --name friend-feature
```

Contributor bridge uses its own secret and publishes that exact name:

```dotenv
DCC_SERVER_URL=wss://dcc.example.com
DCC_TOKEN=<friend-secret>
DCC_PUBLISH_SESSIONS=friend-feature
DCC_UID=1000
DCC_GID=1000
```

Then run:

```bash
docker compose -f compose.bridge.yaml up -d --build
```

After both bridges connect:

- contributor `ListAgents` shows `yrrp-release-manager`;
- manager `ListAgents` shows `friend-feature`;
- either session can use native `SendMessage`;
- idle recipient starts turn immediately;
- busy recipient receives event between tool calls.

## Local development

Install and test:

```bash
uv sync --extra dev
uv run --extra dev pytest -q
uv run --extra dev ruff check .
```

Run broker without Caddy for local testing:

```bash
export DCC_NODE_TOKENS='{"andre":"andre-secret","friend":"friend-secret"}'
uv run distributedcc-server
```

Use `ws://127.0.0.1:8765` only for trusted local tests. Public deployments must use WSS through Caddy.

## Stop and clean up

Stop bridge cleanly:

```bash
docker compose -f compose.bridge.yaml down
```

Bridge removes virtual session records and sockets during normal shutdown. If container is killed, Claude Code ignores stale records once worker PID and process start token no longer match.
