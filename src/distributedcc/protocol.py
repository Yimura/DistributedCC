"""Strict broker protocol and Claude Code peer-envelope codec."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal

MAX_MESSAGE_BYTES = 256 * 1024
MAX_BODY_BYTES = 240 * 1024
SUPERSEDED_CLOSE_CODE = 4001
SUPERSEDED_CLOSE_REASON = "replaced by newer connection"
ADDRESS_PATTERN = r"(?:bridge|uds|did):[A-Za-z0-9%:_/.\\-]{1,200}"
SESSION_PATTERN = r"[A-Za-z0-9_-]{1,80}"
NAME_PATTERN = r'[^"<>\n\r]{1,64}'
ENVELOPE_RE = re.compile(
    rf'^<cross-session-message from="(?P<address>{ADDRESS_PATTERN})" '
    rf'from-session="(?P<session>{SESSION_PATTERN})" '
    rf'from-name="(?P<name>{NAME_PATTERN})" '
    rf'from-mode="(?P<mode>bypass|prompting)">\n'
    rf"(?P<body>[\s\S]*)\n</cross-session-message>$"
)
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


class ProtocolError(ValueError):
    """Raised when a broker or peer message violates its wire contract."""


@dataclass(frozen=True, slots=True)
class PeerEnvelope:
    address: str
    session_id: str
    name: str
    mode: Literal["bypass", "prompting"]
    body: str


@dataclass(frozen=True, slots=True)
class SessionAdvertisement:
    name: str
    session_id: str
    permission_mode: Literal["bypass", "prompting"]

    def __post_init__(self) -> None:
        _validate_identifier("session name", self.name)
        if not SESSION_PATTERN_RE.fullmatch(self.session_id):
            raise ProtocolError("invalid session id")
        if self.permission_mode not in {"bypass", "prompting"}:
            raise ProtocolError("invalid permission mode")


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


SESSION_PATTERN_RE = re.compile(rf"^{SESSION_PATTERN}$")


def build_peer_envelope(
    *, address: str, session_id: str, name: str, mode: str, body: str
) -> str:
    if not re.fullmatch(ADDRESS_PATTERN, address):
        raise ProtocolError("invalid peer address")
    if not SESSION_PATTERN_RE.fullmatch(session_id):
        raise ProtocolError("invalid session id")
    if not re.fullmatch(NAME_PATTERN, name):
        raise ProtocolError("invalid peer name")
    if mode not in {"bypass", "prompting"}:
        raise ProtocolError("invalid permission mode")
    if not body or len(body.encode()) > MAX_BODY_BYTES:
        raise ProtocolError("invalid message body size")
    return (
        f'<cross-session-message from="{address}" '
        f'from-session="{session_id}" '
        f'from-name="{name}" '
        f'from-mode="{mode}">\n'
        f"{body}\n"
        "</cross-session-message>"
    )


def parse_peer_envelope(value: str) -> PeerEnvelope:
    match = ENVELOPE_RE.fullmatch(value)
    if match is None:
        raise ProtocolError("peer envelope is not canonical")
    fields = match.groupdict()
    canonical = build_peer_envelope(
        address=fields["address"],
        session_id=fields["session"],
        name=fields["name"],
        mode=fields["mode"],
        body=fields["body"],
    )
    if canonical != value:
        raise ProtocolError("peer envelope is not canonical")
    return PeerEnvelope(
        address=fields["address"],
        session_id=fields["session"],
        name=fields["name"],
        mode=fields["mode"],
        body=fields["body"],
    )


def decode_client_message(raw: str) -> PeerEvent:
    if len(raw.encode()) > MAX_MESSAGE_BYTES:
        raise ProtocolError("message is too large")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ProtocolError("message is not valid JSON") from error
    if not isinstance(value, dict):
        raise ProtocolError("message must be an object")
    if value.get("type") != "route":
        raise ProtocolError("unsupported message type")
    required = {
        "type",
        "id",
        "source_session",
        "source_mode",
        "target_session",
        "body",
    }
    unknown = set(value) - required
    missing = required - set(value)
    if unknown:
        raise ProtocolError(f"unknown fields: {', '.join(sorted(unknown))}")
    if missing:
        raise ProtocolError(f"missing fields: {', '.join(sorted(missing))}")
    if not all(isinstance(value[key], str) for key in required - {"type"}):
        raise ProtocolError("route fields must be strings")
    return PeerEvent(
        id=value["id"],
        source_session=value["source_session"],
        source_mode=value["source_mode"],
        target_session=value["target_session"],
        body=value["body"],
    )


def decode_publish_message(raw: str) -> list[SessionAdvertisement]:
    if len(raw.encode()) > MAX_MESSAGE_BYTES:
        raise ProtocolError("message is too large")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ProtocolError("message is not valid JSON") from error
    if not isinstance(value, dict) or set(value) != {"type", "sessions"}:
        raise ProtocolError("invalid publish message fields")
    if value["type"] != "publish" or not isinstance(value["sessions"], list):
        raise ProtocolError("invalid publish message")
    if len(value["sessions"]) > 64:
        raise ProtocolError("too many published sessions")

    advertisements = []
    for session in value["sessions"]:
        if not isinstance(session, dict) or set(session) != {"name", "session_id", "mode"}:
            raise ProtocolError("invalid published session fields")
        if not all(isinstance(item, str) for item in session.values()):
            raise ProtocolError("published session fields must be strings")
        advertisements.append(
            SessionAdvertisement(
                name=session["name"],
                session_id=session["session_id"],
                permission_mode=session["mode"],
            )
        )
    return advertisements


def _validate_identifier(label: str, value: str) -> None:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ProtocolError(f"invalid {label}")
