import json

import pytest

from distributedcc.protocol import (
    MAX_MESSAGE_BYTES,
    PeerEvent,
    ProtocolError,
    build_peer_envelope,
    decode_client_message,
    parse_peer_envelope,
)


def test_peer_envelope_round_trip() -> None:
    encoded = build_peer_envelope(
        address="bridge:dcc/andre/yrrp-release-manager",
        session_id="session-123",
        name="yrrp-release-manager",
        mode="bypass",
        body="FEATURE_READY\nPR URL: https://example.test/pr/1",
    )

    decoded = parse_peer_envelope(encoded)

    assert decoded.address == "bridge:dcc/andre/yrrp-release-manager"
    assert decoded.session_id == "session-123"
    assert decoded.name == "yrrp-release-manager"
    assert decoded.mode == "bypass"
    assert decoded.body.startswith("FEATURE_READY")


def test_peer_envelope_rejects_noncanonical_attributes() -> None:
    malformed = (
        '<cross-session-message from-name="spoof" from="bridge:dcc/node/session" '
        'from-session="session-123" from-mode="prompting">\nhello\n'
        "</cross-session-message>"
    )

    with pytest.raises(ProtocolError, match="canonical"):
        parse_peer_envelope(malformed)


def test_client_event_schema_round_trip() -> None:
    raw = json.dumps(
        {
            "type": "route",
            "id": "60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
            "source_session": "feature-owner",
            "source_mode": "bypass",
            "target_session": "yrrp-release-manager",
            "body": "FEATURE_READY",
        }
    )

    message = decode_client_message(raw)

    assert isinstance(message, PeerEvent)
    assert message.source_session == "feature-owner"
    assert message.source_mode == "bypass"
    assert message.target_session == "yrrp-release-manager"


def test_client_event_rejects_unknown_fields() -> None:
    raw = json.dumps(
        {
            "type": "route",
            "id": "60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
            "source_session": "feature-owner",
            "source_mode": "bypass",
            "target_session": "yrrp-release-manager",
            "body": "FEATURE_READY",
            "forged": True,
        }
    )

    with pytest.raises(ProtocolError, match="unknown fields"):
        decode_client_message(raw)


def test_client_event_rejects_invalid_source_mode() -> None:
    raw = json.dumps(
        {
            "type": "route",
            "id": "60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
            "source_session": "feature-owner",
            "source_mode": "unattested",
            "target_session": "yrrp-release-manager",
            "body": "FEATURE_READY",
        }
    )

    with pytest.raises(ProtocolError, match="source mode"):
        decode_client_message(raw)


def test_client_event_rejects_oversized_message() -> None:
    raw = json.dumps(
        {
            "type": "route",
            "id": "60cb8e60-1b45-4cf0-a88e-6b6b8b98e6bf",
            "source_session": "feature-owner",
            "source_mode": "prompting",
            "target_session": "yrrp-release-manager",
            "body": "x" * MAX_MESSAGE_BYTES,
        }
    )

    with pytest.raises(ProtocolError, match="too large"):
        decode_client_message(raw)
