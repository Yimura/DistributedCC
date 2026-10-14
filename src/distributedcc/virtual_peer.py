"""Claude Code virtual-peer records and native frame handling."""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import re
import socket
import struct
import time
from dataclasses import dataclass
from pathlib import Path
from queue import Empty
from typing import Literal

from distributedcc.protocol import (
    ADDRESS_PATTERN,
    MAX_MESSAGE_BYTES,
    SESSION_PATTERN,
    ProtocolError,
)
from distributedcc.registry import (
    SUPPORTED_PEER_PROTOCOL,
    TESTED_CLAUDE_CODE_VERSION,
    discover_live_sessions,
)

NATIVE_ENVELOPE_RE = re.compile(
    rf'^<cross-session-message from="{ADDRESS_PATTERN}"'
    rf'(?: from-session="{SESSION_PATTERN}")?'
    r'(?: hop-chain="[0-9a-f]{24}(?:,[0-9a-f]{24}){0,31}")?'
    r'(?: from-name="[^"<>\n\r]+")?'
    r'(?: from-mode="(?:bypass|prompting)")?'
    r'(?: from-plugin="[^"<>\n\r]+")?>\n'
    r'(?P<body>[\s\S]*)\n</cross-session-message>$'
)


@dataclass(frozen=True, slots=True)
class RemoteSession:
    name: str
    session_id: str
    node: str
    permission_mode: Literal["bypass", "prompting"]


def build_session_record(
    *,
    remote: RemoteSession,
    pid: int,
    proc_start: str,
    socket_path: Path,
    now_ms: int,
) -> dict[str, object]:
    return {
        "pid": pid,
        "sessionId": remote.session_id,
        "cwd": f"/virtual/{remote.node}",
        "startedAt": now_ms,
        "procStart": proc_start,
        "version": TESTED_CLAUDE_CODE_VERSION,
        "peerProtocol": SUPPORTED_PEER_PROTOCOL,
        "peerFeatures": [],
        "kind": "interactive",
        "entrypoint": "cli",
        "messagingSocketPath": str(socket_path),
        "name": remote.name,
        "nameSource": "distributedcc",
        "nameSince": now_ms,
        "status": "idle",
        "updatedAt": now_ms,
        "statusUpdatedAt": now_ms,
    }


def decode_native_send(raw: str) -> str:
    if len(raw.encode()) > MAX_MESSAGE_BYTES:
        raise ProtocolError("native frame is too large")
    try:
        frame = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ProtocolError("native frame is not valid JSON") from error
    if not isinstance(frame, dict) or frame.get("type") != "user":
        raise ProtocolError("expected native user frame")
    message = frame.get("message")
    if not isinstance(message, dict) or set(message) != {"role", "content"}:
        raise ProtocolError("invalid native message object")
    if message["role"] != "user" or not isinstance(message["content"], str):
        raise ProtocolError("invalid native message object")
    envelope = NATIVE_ENVELOPE_RE.fullmatch(message["content"])
    if envelope is None:
        raise ProtocolError("peer envelope is not canonical")
    return envelope.group("body")


class VirtualPeerProcess:
    def __init__(
        self,
        *,
        remote: RemoteSession,
        sessions_dir: Path,
        sockets_dir: Path,
        outbound: object,
    ) -> None:
        self.remote = remote
        self.sessions_dir = sessions_dir
        self.sockets_dir = sockets_dir
        self.outbound = outbound
        self._stop = multiprocessing.Event()
        self._process: multiprocessing.Process | None = None

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process is not None else None

    @property
    def socket_path(self) -> Path | None:
        pid = self.pid
        return self.sockets_dir / f"{pid}.sock" if pid is not None else None

    def start(self) -> None:
        if self._process is not None:
            raise RuntimeError("virtual peer already started")
        self._process = multiprocessing.Process(
            target=_run_virtual_peer,
            args=(
                self.remote,
                self.sessions_dir,
                self.sockets_dir,
                self.outbound,
                self._stop,
            ),
            name=f"dcc-peer-{self.remote.name}",
        )
        self._process.start()

    def stop(self) -> None:
        process = self._process
        if process is None:
            return
        self._stop.set()
        process.join(timeout=3)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
        self._process = None


class VirtualPeerSupervisor:
    def __init__(self, sessions_dir: Path, sockets_dir: Path) -> None:
        self.sessions_dir = sessions_dir
        self.sockets_dir = sockets_dir
        self.outbound: multiprocessing.Queue[dict[str, str]] = multiprocessing.Queue()
        self._workers: dict[str, VirtualPeerProcess] = {}

    def reconcile(self, sessions: list[RemoteSession]) -> None:
        desired = {session.name: session for session in sessions}
        for name, worker in list(self._workers.items()):
            if desired.get(name) == worker.remote:
                continue
            worker.stop()
            del self._workers[name]
        for name, remote in desired.items():
            if name in self._workers:
                continue
            worker = VirtualPeerProcess(
                remote=remote,
                sessions_dir=self.sessions_dir,
                sockets_dir=self.sockets_dir,
                outbound=self.outbound,
            )
            worker.start()
            self._workers[name] = worker

    def address_for(self, session_name: str) -> str | None:
        worker = self._workers.get(session_name)
        socket_path = worker.socket_path if worker is not None else None
        return f"uds:{socket_path}" if socket_path is not None else None

    def next_outbound(self, timeout: float = 0.2) -> dict[str, str] | None:
        try:
            return self.outbound.get(timeout=timeout)
        except Empty:
            return None

    def stop(self) -> None:
        for worker in self._workers.values():
            worker.stop()
        self._workers.clear()
        self.outbound.close()


def _run_virtual_peer(
    remote: RemoteSession,
    sessions_dir: Path,
    sockets_dir: Path,
    outbound: object,
    stop: multiprocessing.synchronize.Event,
) -> None:
    pid = os.getpid()
    socket_path = sockets_dir / f"{pid}.sock"
    record_path = sessions_dir / f"{pid}.json"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        socket_path.unlink(missing_ok=True)
        listener.bind(str(socket_path))
        os.chmod(socket_path, 0o600)
        listener.listen(8)
        listener.settimeout(0.2)
        record = build_session_record(
            remote=remote,
            pid=pid,
            proc_start=_read_own_start_token(),
            socket_path=socket_path,
            now_ms=int(time.time() * 1000),
        )
        _atomic_write_json(record_path, record)
        while not stop.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            with connection:
                _handle_connection(connection, remote, sessions_dir, outbound)
    finally:
        listener.close()
        record_path.unlink(missing_ok=True)
        socket_path.unlink(missing_ok=True)


def _handle_connection(
    connection: socket.socket,
    remote: RemoteSession,
    sessions_dir: Path,
    outbound: object,
) -> None:
    peer_pid, peer_uid, _ = struct.unpack(
        "3i",
        connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")),
    )
    if peer_uid != os.getuid():
        logging.warning("virtual peer rejected uid %s", peer_uid)
        return
    source = next(
        (
            session
            for session in discover_live_sessions(sessions_dir).values()
            if session.pid == peer_pid
        ),
        None,
    )
    if source is None:
        logging.warning("virtual peer rejected unregistered pid %s", peer_pid)
        return
    raw = _read_line(connection)
    if raw is None:
        logging.debug("virtual peer probe closed without a frame from %s", source.name)
        return
    try:
        body = decode_native_send(raw)
    except ProtocolError as error:
        opening = _opening_envelope_tag(raw)
        logging.warning(
            "virtual peer rejected frame from %s: %s; envelope=%s",
            source.name,
            error,
            opening,
        )
        return
    outbound.put(
        {
            "source_session": source.name,
            "target_session": remote.name,
            "body": body,
        }
    )
    logging.info("virtual peer routed %s to %s", source.name, remote.name)


def _opening_envelope_tag(raw: str) -> str:
    try:
        frame = json.loads(raw)
        content = frame["message"]["content"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return "<unavailable>"
    if not isinstance(content, str):
        return "<unavailable>"
    return content.splitlines()[0][:500] if content else "<empty>"


def _read_line(connection: socket.socket) -> str | None:
    chunks = bytearray()
    while len(chunks) <= MAX_MESSAGE_BYTES:
        part = connection.recv(min(65536, MAX_MESSAGE_BYTES + 1 - len(chunks)))
        if not part:
            break
        chunks.extend(part)
        newline = chunks.find(b"\n")
        if newline >= 0:
            try:
                return chunks[:newline].decode()
            except UnicodeDecodeError:
                return None
    return None


def _read_own_start_token() -> str:
    stat = Path("/proc/self/stat").read_text()
    tail = stat.rsplit(") ", maxsplit=1)[1].split()
    return tail[19]


def _atomic_write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(value, separators=(",", ":")))
    os.replace(temporary, path)
