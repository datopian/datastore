"""`Decision` shape + `default_key_id` always-sha256 behaviour."""

from __future__ import annotations

import hashlib

import jwt
import pytest
from datastore.auth.base import Decision, default_key_id


def test_decision_defaults_are_all_none() -> None:
    d = Decision()
    assert d.subject is None
    assert d.claims is None
    assert d.resource is None
    assert d.package is None


def test_default_key_id_hashes_full_credential_even_for_jwt() -> None:
    # Security: never derive cache identity from unverified JWT claims
    # (a forged `jti` could collide with a verified user's cache entry).
    # The full credential bytes always go through sha256.
    token = jwt.encode({"sub": "u", "jti": "abc123"}, "k", algorithm="HS256")
    expected = "h:" + hashlib.sha256(token.encode()).hexdigest()[:16]
    assert default_key_id(token) == expected


def test_default_key_id_hashes_opaque_token() -> None:
    token = "opaque-token-no-dots"
    expected = "h:" + hashlib.sha256(token.encode()).hexdigest()[:16]
    assert default_key_id(token) == expected


def test_two_different_jwts_with_same_jti_get_different_cache_keys() -> None:
    # The whole point of dropping the jti optimisation: A and B both
    # claim `jti=shared` but were signed differently. Their cache keys
    # must NOT collide.
    a = jwt.encode({"sub": "a", "jti": "shared"}, "key-1", algorithm="HS256")
    b = jwt.encode({"sub": "b", "jti": "shared"}, "key-2", algorithm="HS256")
    assert default_key_id(a) != default_key_id(b)


@pytest.mark.parametrize("token", ["", "a", "a.b", "a.b.c", "a.b.c.d"])
def test_default_key_id_is_sha256_for_any_input_shape(token: str) -> None:
    expected = "h:" + hashlib.sha256(token.encode()).hexdigest()[:16]
    assert default_key_id(token) == expected
