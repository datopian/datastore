"""`Decision` shape + `default_key_id` JWT/opaque handling."""

from __future__ import annotations

import base64
import hashlib

import jwt
import orjson
import pytest
from datastore.auth.base import Decision, default_key_id


def test_decision_defaults_are_all_none() -> None:
    d = Decision()
    assert d.subject is None
    assert d.claims is None
    assert d.resource is None
    assert d.package is None


def test_default_key_id_extracts_jti_from_jwt() -> None:
    token = jwt.encode({"sub": "u", "jti": "abc123"}, "k", algorithm="HS256")
    assert default_key_id(token) == "jti:abc123"


def test_default_key_id_falls_back_to_sha256_for_jwt_without_jti() -> None:
    token = jwt.encode({"sub": "u"}, "k", algorithm="HS256")
    expected = "h:" + hashlib.sha256(token.encode()).hexdigest()[:16]
    assert default_key_id(token) == expected


def test_default_key_id_falls_back_to_sha256_for_opaque_token() -> None:
    token = "opaque-token-no-dots"
    expected = "h:" + hashlib.sha256(token.encode()).hexdigest()[:16]
    assert default_key_id(token) == expected


def test_default_key_id_ignores_non_string_jti() -> None:
    # Hand-craft a JWT-shaped payload with a numeric `jti` — fall back to sha256.
    payload = base64.urlsafe_b64encode(orjson.dumps({"jti": 12345})).rstrip(b"=").decode()
    token = f"hdr.{payload}.sig"
    assert default_key_id(token).startswith("h:")


def test_default_key_id_handles_malformed_jwt_segment() -> None:
    # Three-segment string but middle segment is not valid base64/json.
    assert default_key_id("hdr.@@@.sig").startswith("h:")


@pytest.mark.parametrize("token", ["", "a", "a.b", "a.b.c.d"])
def test_default_key_id_for_non_three_segment_inputs(token: str) -> None:
    # Anything that isn't a 3-part JWT → hashed.
    assert default_key_id(token).startswith("h:")
