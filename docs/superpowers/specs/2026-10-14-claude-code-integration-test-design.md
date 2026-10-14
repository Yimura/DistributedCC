# Claude Code integration test design

## Goal

Add a deterministic integration test that launches two real Claude Code CLI sessions and verifies DistributedCC message delivery, replies, and permission behavior without calling a real model.

The test must cover the complete path from one Claude Code harness through DistributedCC to another harness and back. A local fake Anthropic Messages API supplies scripted responses in place of model inference.

## Scope

The integration test covers:

- two persistent, named Claude Code processes;
- a local deterministic Anthropic-compatible backend;
- real DistributedCC broker, bridge, registry, Unix socket, and virtual-peer behavior;
- sender `SendMessage` tool execution;
- receiver inbound-message admission;
- scripted receiver answers;
- permission-mode behavior and propagation;
- failure diagnostics and process cleanup;
- supported and latest Claude Code compatibility in GitHub Actions.

The test does not evaluate model quality, contact Anthropic, use production credentials, or replace focused protocol and lifecycle tests already in the repository.

## Architecture

### Scripted Anthropic backend

`ScriptedAnthropicServer` exposes the endpoints Claude Code needs, including:

- `POST /v1/messages`;
- `POST /v1/messages/count_tokens`;
- any harmless readiness request observed from the pinned CLI.

The backend implements Anthropic Messages streaming with correctly ordered server-sent events. It can return text, tool-use blocks, tool-result continuations, and final responses.

Each Claude Code process receives a distinct dummy authentication token. The backend uses that token to select the sender or receiver scenario and records every request for later assertions. This avoids depending on unstable prompt text to identify a process.

The scenario engine is a deterministic state machine. Unexpected requests, states, tools, or event order fail immediately with a diagnostic describing the recorded request sequence.

### Claude Code process fixture

A `ClaudeProcess` fixture launches a persistent Claude Code session with:

- an explicit name and session ID;
- an isolated configuration directory and home directory;
- an explicit permission mode and session-scoped settings;
- `ANTHROPIC_BASE_URL` pointing to the fake backend;
- a process-specific dummy token supplied through an isolated `apiKeyHelper`;
- a restricted tool surface containing only tools required by the scenario;
- captured pseudo-terminal output for visible-result assertions and diagnostics;
- no dependence on user, project, plugin, hook, memory, or prior-session state.

The fixture verifies the CLI version before starting the scenario. It uses non-bare interactive PTY mode because Claude Code 2.1.296 omits `messagingSocketPath` in print mode and with `--bare`. It pre-seeds onboarding and workspace trust in the isolated configuration, strips inherited parent-session markers, and never resumes an ambient session.

### DistributedCC fixture

The integration fixture starts a real broker and two bridges on ephemeral ports with temporary registry and socket paths. It waits for each observable lifecycle condition:

1. Claude process registration;
2. bridge connection;
3. session publication;
4. remote peer creation;
5. message route;
6. receiver delivery;
7. reply route;
8. sender delivery.

Deadline-based polling replaces fixed sleeps. Each wait reports its last observed state on timeout.

### Message flow

```text
fake backend -> sender Claude -> SendMessage tool -> sender virtual peer
-> sender bridge -> broker -> receiver bridge -> receiver Claude
-> fake backend -> receiver answer -> reverse DistributedCC path
-> sender Claude -> recorded final response
```

The fake backend first instructs the sender harness to call `SendMessage`. DistributedCC transports the message to the receiver harness. The receiver's next Messages API request contains the inbound message, so the backend returns a scripted answer. The answer traverses DistributedCC back to the sender, whose next request proves that the harness received it.

## Permission scenarios

### Explicitly allowed

The sender uses prompting-class mode with an explicit `SendMessage` allow rule. The receiver uses session-scoped `crossSessionInbound: "accept"`. The round trip must succeed without an interactive prompt.

### Tool denied

The sender uses `dontAsk` with an explicit `SendMessage` disallow rule. Omitting an allow rule is insufficient because Claude Code treats native `SendMessage` as safe. The fake backend requests `SendMessage`; Claude Code must return an error tool result, and the broker must receive no route event.

### Bypass propagation

The sender uses bypass mode inside the isolated test environment. Its available tool surface remains restricted to `SendMessage`. The routed message must carry `source_mode="bypass"`, and the trusted receiver must accept and answer it.

### Receiver admission

A receiver without `crossSessionInbound: "accept"` must not pass a mismatched inbound message into its model conversation before user acceptance. The test proves non-delivery through a bounded deadline and recorded backend requests.

This test configuration follows `AI_Agent_Security_Cheat_Sheet#1-tool-security--least-privilege`: CI grants only the tool needed by each scenario and no repository write token. `crossSessionInbound` controls inbound admission but does not approve receiver-side tool calls.

## Assertions

The happy-path integration test asserts:

- both named sessions register and become remotely visible;
- sender Claude emits the expected `SendMessage` tool call;
- DistributedCC routes the expected body and source mode;
- receiver Claude sends the inbound body to the fake backend;
- receiver Claude produces the scripted answer;
- sender Claude sends the returned answer to the fake backend;
- all processes remain healthy until orderly shutdown;
- no request reaches an external network endpoint; a fixture-owned deny proxy records and blocks Claude Code's model-catalog probes to `downloads.claude.ai`.

Permission tests assert both the visible Claude Code result and the absence or presence of broker routing. Request-contract assertions use parsed JSON objects rather than serialized-string equality.

## Error handling and diagnostics

Every process and server starts under one cleanup stack. Cleanup runs after success, assertion failure, cancellation, or timeout. It first requests graceful shutdown, then terminates and finally kills any process that misses its deadline.

A failed test reports:

- normalized Claude Code PTY output;
- fake backend request and response state history;
- broker and bridge logs;
- registry snapshot;
- active process state and exit codes;
- last satisfied lifecycle condition;
- resolved Claude Code version.

Logs redact authentication tokens and avoid persisting message content beyond deterministic test fixtures. GitHub Actions uploads diagnostics only after failure.

## Test organization

Primary ownership remains in DistributedCC:

- `tests/test_claude_code_integration.py` contains scenarios;
- `tests/claude_code_process.py` contains PTY process, version, egress, waiting, and redaction helpers;
- `tests/claude_code_fake_api.py` contains SSE response and scripted Messages API helpers;
- `tests/claude_code_integration_support.py` contains broker, bridge, lifecycle, and diagnostics orchestration;
- `claude_code_integration` marks tests requiring the real CLI;
- normal test runs exclude that marker;
- the dedicated integration workflow runs only that marker.

The support module may reuse patterns from `claude-code-proxy`, but DistributedCC does not import test code from another repository. This keeps CI self-contained and avoids cross-repository package coupling.

## GitHub Actions

### Normal tests workflow

`.github/workflows/tests.yml` runs for pull-request `opened`, `reopened`, and `synchronize` events. It uses Python 3.14 to match the project container, installs test dependencies, then runs the complete normal suite excluding `claude_code_integration`, lint checks, and diff hygiene.

The workflow uses read-only repository permissions, dependency caching, concurrency cancellation for superseded revisions, and immutable action revisions.

### Claude Code integration workflow

`.github/workflows/claude-code-integration.yml` runs the integration marker on Linux for the same pull-request events. It also runs on a weekly schedule to detect Claude Code compatibility drift without waiting for repository activity.

The workflow contains two jobs:

1. **Supported** installs exact `@anthropic-ai/claude-code@2.1.296`. This job is deterministic and blocking.
2. **Latest** installs `@anthropic-ai/claude-code@latest`. This compatibility probe runs the same tests and is non-blocking with `continue-on-error: true`.

Each job prints the resolved `claude --version` into the job summary. A latest-version failure remains visible and uploads diagnostics but does not block unrelated pull requests. A passing latest job provides evidence for a future supported-version update.

The workflow uses dummy credentials, a local fake backend, `permissions: contents: read`, no repository secrets, job-level timeout, per-test deadlines, and concurrency cancellation.

## Compatibility policy

`2.1.296` remains the supported private-protocol baseline until a deliberate compatibility update changes the production version guard and fixtures together. The latest job never silently changes that baseline.

A latest-job failure should distinguish:

- registry version rejection;
- Messages API or streaming contract drift;
- session registry or Unix socket changes;
- native peer-envelope changes;
- permission or inbound-admission changes;
- CLI invocation or output-format changes.

## Acceptance criteria

The design is complete when implementation demonstrates:

- deterministic round-trip messaging between two real Claude Code sessions without a real model call;
- explicit allow, denial, bypass propagation, and receiver-admission scenarios;
- bounded waits and reliable cleanup;
- actionable failure diagnostics;
- separate normal and integration GitHub Actions workflows;
- blocking coverage against Claude Code `2.1.296`;
- non-blocking compatibility coverage against latest Claude Code;
- no external credentials, external inference, or write-capable CI token.
