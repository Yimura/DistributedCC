import json

import pytest

from distributedcc.protocol import ProtocolError, decode_publish_message
from distributedcc.server import TokenAuthenticator


def test_authenticator_maps_unique_secret_to_fixed_node() -> None:
    authenticator = TokenAuthenticator.from_json(
        json.dumps({"andre": "andre-secret", "friend": "friend-secret"})
    )

    assert authenticator.authenticate("Bearer friend-secret") == "friend"
    assert authenticator.authenticate("Bearer andre-secret") == "andre"


@pytest.mark.parametrize(
    "header",
    [None, "", "Basic friend-secret", "Bearer wrong", "Bearer"],
)
def test_authenticator_rejects_invalid_header(header: str | None) -> None:
    authenticator = TokenAuthenticator.from_json(json.dumps({"friend": "friend-secret"}))

    assert authenticator.authenticate(header) is None


def test_authenticator_rejects_duplicate_secret() -> None:
    with pytest.raises(ValueError, match="unique"):
        TokenAuthenticator.from_json(
            json.dumps({"andre": "shared-secret", "friend": "shared-secret"})
        )


def test_decodes_strict_publish_message() -> None:
    sessions = decode_publish_message(
        json.dumps(
            {
                "type": "publish",
                "sessions": [
                    {
                        "name": "yrrp-release-manager",
                        "session_id": "session-manager",
                        "mode": "bypass",
                    }
                ],
            }
        )
    )

    assert sessions[0].name == "yrrp-release-manager"
    assert sessions[0].permission_mode == "bypass"


def test_publish_rejects_extra_session_fields() -> None:
    raw = json.dumps(
        {
            "type": "publish",
            "sessions": [
                {
                    "name": "friend-feature",
                    "session_id": "session-feature",
                    "mode": "prompting",
                    "node": "andre",
                }
            ],
        }
    )

    with pytest.raises(ProtocolError, match="session fields"):
        decode_publish_message(raw)
