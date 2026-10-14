from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

from claude_code_fake_api import (
    ScriptedAnthropicServer,
    ScriptedScenario,
    ScriptStep,
    peer_envelopes,
)
from claude_code_process import (
    ClaudeLaunchOptions,
    ClaudeProcess,
    DenyingProxy,
    EgressAttempt,
    redact,
    wait_until,
)
from websockets.asyncio.server import Server, serve

from distributedcc.bridge import BridgeConfig, run_bridge
from distributedcc.protocol import MAX_MESSAGE_BYTES, PeerEvent
from distributedcc.registry import LocalSession, discover_live_sessions
from distributedcc.server import (
    Broker,
    Connection,
    TokenAuthenticator,
    handle_websocket,
    make_process_request,
)

EXPECTED_EGRESS_BY_CLAUDE_VERSION = {
    "2.1.296": {EgressAttempt("CONNECT", "downloads.claude.ai:443")},
}


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


def prompting_sender(*, allow_send_message: bool) -> ClaudeLaunchOptions:
    return ClaudeLaunchOptions(
        name="dcc-sender",
        session_id="22222222-2222-4222-8222-222222222222",
        api_key="sender-test-token",
        base_url="",
        permission_mode="dontAsk",
        allow_send_message=allow_send_message,
        inbound_accept=True,
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
    )


def bypass_sender() -> ClaudeLaunchOptions:
    return dataclasses.replace(
        prompting_sender(allow_send_message=True),
        permission_mode="bypassPermissions",
    )


def bypass_receiver(*, inbound_accept: bool) -> ClaudeLaunchOptions:
    return dataclasses.replace(
        prompting_receiver(inbound_accept=inbound_accept),
        permission_mode="bypassPermissions",
    )


class IntegrationHarness:
    def __init__(
        self,
        tmp_path: Path,
        artifact_dir: Path,
        *,
        sender: ClaudeLaunchOptions,
        receiver: ClaudeLaunchOptions,
        sender_steps: list[ScriptStep],
        receiver_steps: list[ScriptStep],
    ) -> None:
        self.tmp_path = tmp_path
        self.artifact_dir = artifact_dir
        self.sender_options = sender
        self.receiver_options = receiver
        self.sender_scenario = ScriptedScenario("sender", sender_steps)
        self.receiver_scenario = ScriptedScenario("receiver", receiver_steps)
        self.fake_server = ScriptedAnthropicServer(
            {
                sender.api_key: self.sender_scenario,
                receiver.api_key: self.receiver_scenario,
            }
        )
        self.deny_proxy = DenyingProxy()
        self.allowed_egress = EXPECTED_EGRESS_BY_CLAUDE_VERSION.get(
            ClaudeProcess.resolve_version(),
            set(),
        )
        self.broker = RecordingBroker()
        self.broker_server: Server | None = None
        self.bridge_tasks: list[asyncio.Task[None]] = []
        self.sender: ClaudeProcess
        self.receiver: ClaudeProcess
        self.last_satisfied_condition = "not started"

    async def __aenter__(self) -> IntegrationHarness:
        await self.deny_proxy.__aenter__()
        await self.fake_server.__aenter__()
        self.sender = self._make_process("sender", self.sender_options)
        self.receiver = self._make_process("receiver", self.receiver_options)
        try:
            await self._start_broker()
            async with asyncio.TaskGroup() as group:
                group.create_task(self.sender.start())
                group.create_task(self.receiver.start())
            self._start_bridges()
        except BaseException:
            await self.cleanup()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        if exc is not None:
            self.write_diagnostics(exc)
        errors = await self.cleanup()
        if errors and exc is None:
            raise ExceptionGroup("integration cleanup failed", errors)

    async def wait_ready(self) -> None:
        await self._wait(
            "sender Claude registration",
            self.sender.session_record,
            self._record_has_socket,
        )
        await self._wait(
            "receiver Claude registration",
            self.receiver.session_record,
            self._record_has_socket,
        )
        await self._wait(
            "sender session publication",
            lambda: self.broker.remote_sessions("receiver-node"),
            lambda sessions: any(item["name"] == "dcc-sender" for item in sessions),
        )
        await self._wait(
            "receiver session publication",
            lambda: self.broker.remote_sessions("sender-node"),
            lambda sessions: any(item["name"] == "dcc-receiver" for item in sessions),
        )
        await self._wait(
            "sender remote peer creation",
            lambda: self.virtual_session_names("sender"),
            lambda names: names == {"dcc-receiver"},
        )
        await self._wait(
            "receiver remote peer creation",
            lambda: self.virtual_session_names("receiver"),
            lambda names: names == {"dcc-sender"},
        )

    async def wait_for_completed_round_trip(self) -> None:
        await self.wait_for_route_count(2)
        await self._wait(
            "sender receives reply",
            lambda: len(self.sender_scenario.history),
            lambda count: count >= 3,
        )
        await self._wait(
            "receiver sends reply",
            lambda: len(self.receiver_scenario.history),
            lambda count: count >= 2,
        )
        await self._wait(
            "sender renders completion",
            lambda: self.sender.has_result_text("Round trip complete."),
            bool,
        )
        await self._wait(
            "receiver renders completion",
            lambda: self.receiver.has_result_text("Receiver reply sent."),
            bool,
        )

    async def wait_for_route_count(self, count: int) -> None:
        await self._wait(
            f"broker route count {count}",
            lambda: len(self.broker.route_events),
            lambda observed: observed >= count,
        )

    async def wait_for_sender_continuation(self) -> dict[str, object]:
        record = await self._wait(
            "sender tool continuation",
            lambda: self.sender_scenario.history,
            lambda history: len(history) >= 2,
        )
        return record[-1].body

    async def assert_route_count_stable(self, count: int, *, duration: float) -> None:
        deadline = asyncio.get_running_loop().time() + duration
        while asyncio.get_running_loop().time() < deadline:
            self._raise_if_unhealthy()
            observed = len(self.broker.route_events)
            if observed != count:
                raise AssertionError(f"route count changed from {count} to {observed}")
            await asyncio.sleep(0.05)

    async def assert_receiver_request_count_stable(
        self,
        count: int,
        *,
        duration: float,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + duration
        while asyncio.get_running_loop().time() < deadline:
            self._raise_if_unhealthy()
            observed = len(self.receiver_scenario.history)
            if observed != count:
                raise AssertionError(
                    f"receiver request count changed from {count} to {observed}"
                )
            await asyncio.sleep(0.05)

    def sender_saw_reply(self, body: str) -> bool:
        return any(
            envelope.body == body
            for record in self.sender_scenario.history
            for envelope in peer_envelopes(record.body)
        )

    def receiver_saw_inbound(self, body: str) -> bool:
        return any(
            envelope.body == body
            for record in self.receiver_scenario.history
            for envelope in peer_envelopes(record.body)
        )

    def receiver_peer_request(self) -> dict[str, object]:
        for record in self.receiver_scenario.history:
            if peer_envelopes(record.body):
                return record.body
        raise AssertionError("receiver has no peer request")

    def assert_clean(self) -> None:
        self.fake_server.assert_clean(self.deny_proxy, self.allowed_egress)

    def assert_egress_policy(self) -> None:
        self.deny_proxy.assert_allowed_attempts(self.allowed_egress)

    def real_session_names(self, side: str) -> set[str]:
        process = self._process(side)
        return {
            name
            for name, session in discover_live_sessions(process.sessions_dir).items()
            if session.pid == process.process.pid
        }

    def virtual_session_names(self, side: str) -> set[str]:
        process = self._process(side)
        own_pid = process.process.pid
        return {
            name
            for name, session in discover_live_sessions(process.sessions_dir).items()
            if session.pid != own_pid
        }

    def real_session(self, side: str, name: str) -> LocalSession:
        process = self._process(side)
        session = discover_live_sessions(process.sessions_dir)[name]
        if session.pid != process.process.pid:
            raise AssertionError(f"{name} is not the real {side} session")
        return session

    def owned_registry_paths(self) -> list[Path]:
        directories = (self.sender.sessions_dir, self.receiver.sessions_dir)
        return [path for directory in directories for path in directory.glob("*.json")]

    def owned_socket_paths(self) -> list[Path]:
        paths = list((self.tmp_path / "sender-virtual-sockets").glob("*.sock"))
        paths.extend((self.tmp_path / "receiver-virtual-sockets").glob("*.sock"))
        for process in (self.sender, self.receiver):
            if process.process is not None:
                paths.append(Path(f"/run/user/{os.getuid()}/cc-socks/{process.process.pid}.sock"))
        return [path for path in paths if path.exists()]

    async def cleanup(self) -> list[BaseException]:
        errors: list[BaseException] = []
        for task in self.bridge_tasks:
            task.cancel()
        await self._collect_cleanup(
            errors,
            asyncio.gather(*self.bridge_tasks, return_exceptions=True),
        )
        processes = [
            process
            for process in (getattr(self, "sender", None), getattr(self, "receiver", None))
            if process is not None
        ]
        results = await asyncio.gather(
            *(process.stop() for process in processes),
            return_exceptions=True,
        )
        errors.extend(result for result in results if isinstance(result, BaseException))
        if self.broker_server is not None:
            self.broker_server.close()
            await self._collect_cleanup(errors, self.broker_server.wait_closed())
        await self._collect_cleanup(errors, self.fake_server.__aexit__(None, None, None))
        await self._collect_cleanup(errors, self.deny_proxy.__aexit__(None, None, None))
        return errors

    def write_diagnostics(self, error: BaseException) -> None:
        payload = {
            "error": repr(error),
            "claude_version": ClaudeProcess.resolve_version(),
            "last_satisfied_condition": self.last_satisfied_condition,
            "processes": {
                "sender": self._process_diagnostic(self.sender),
                "receiver": self._process_diagnostic(self.receiver),
            },
            "fake_api_failures": self.fake_server.failures,
            "fake_api_history": {
                "sender": [record.body for record in self.sender_scenario.history],
                "receiver": [record.body for record in self.receiver_scenario.history],
            },
            "broker_routes": [dataclasses.asdict(event) for event in self.broker.route_events],
            "deny_proxy_attempts": [
                dataclasses.asdict(attempt) for attempt in self.deny_proxy.attempts
            ],
        }
        path = self.artifact_dir / "integration-failure.json"
        path.write_text(json.dumps(redact(payload), indent=2))

    def _make_process(self, side: str, options: ClaudeLaunchOptions) -> ClaudeProcess:
        configured = dataclasses.replace(
            options,
            base_url=self.fake_server.base_url,
            deny_proxy_url=self.deny_proxy.url,
        )
        return ClaudeProcess(self.tmp_path / side, configured)

    async def _start_broker(self) -> None:
        authenticator = TokenAuthenticator(
            {
                "sender-node": "sender-broker-token",
                "receiver-node": "receiver-broker-token",
            }
        )
        self.broker_server = await serve(
            lambda websocket: handle_websocket(websocket, self.broker, authenticator),
            "127.0.0.1",
            0,
            process_request=make_process_request(authenticator),
            max_size=MAX_MESSAGE_BYTES,
            compression=None,
        )
        assert self.broker_server.sockets
        port = self.broker_server.sockets[0].getsockname()[1]
        self.broker_url = f"ws://127.0.0.1:{port}"

    def _start_bridges(self) -> None:
        sender_sockets = self.tmp_path / "sender-virtual-sockets"
        receiver_sockets = self.tmp_path / "receiver-virtual-sockets"
        sender_sockets.mkdir()
        receiver_sockets.mkdir()
        configs = (
            BridgeConfig(
                server_url=self.broker_url,
                token="sender-broker-token",
                publish_names=("dcc-sender",),
                sessions_dir=self.sender.sessions_dir,
                sockets_dir=sender_sockets,
            ),
            BridgeConfig(
                server_url=self.broker_url,
                token="receiver-broker-token",
                publish_names=("dcc-receiver",),
                sessions_dir=self.receiver.sessions_dir,
                sockets_dir=receiver_sockets,
            ),
        )
        self.bridge_tasks = [
            asyncio.create_task(run_bridge(config), name=f"integration-bridge-{index}")
            for index, config in enumerate(configs)
        ]

    async def _wait[T](
        self,
        label: str,
        observe: Callable[[], T | Awaitable[T]],
        satisfied: Callable[[T], bool],
        timeout: float = 15.0,
    ) -> T:
        async def healthy_observe() -> T:
            self._raise_if_unhealthy()
            value = observe()
            return await value if inspect.isawaitable(value) else value

        result = await wait_until(label, healthy_observe, satisfied, timeout=timeout)
        self.last_satisfied_condition = label
        return result

    def _raise_if_unhealthy(self) -> None:
        if self.fake_server.failures:
            raise AssertionError(f"fake API failed: {self.fake_server.failures}")
        for label, process in (("sender", self.sender), ("receiver", self.receiver)):
            if process.returncode is not None:
                raise AssertionError(
                    f"{label} Claude exited with {process.returncode}: "
                    f"{process.terminal_text()[-2000:]}"
                )
        for task in self.bridge_tasks:
            if task.done() and not task.cancelled():
                raise AssertionError(f"{task.get_name()} failed: {task.exception()!r}")

    def _process(self, side: str) -> ClaudeProcess:
        if side == "sender":
            return self.sender
        if side == "receiver":
            return self.receiver
        raise ValueError(f"unknown side: {side}")

    @staticmethod
    def _record_has_socket(record: dict[str, object] | None) -> bool:
        if record is None:
            return False
        socket_path = record.get("messagingSocketPath")
        return isinstance(socket_path, str) and Path(socket_path).is_socket()

    @staticmethod
    async def _collect_cleanup(
        errors: list[BaseException],
        awaitable: Awaitable[object],
    ) -> None:
        try:
            await awaitable
        except BaseException as error:
            errors.append(error)

    @staticmethod
    def _process_diagnostic(process: ClaudeProcess) -> dict[str, object]:
        return {
            "returncode": process.returncode,
            "record": process.session_record(),
            "terminal": process.terminal_text()[-10000:],
        }
