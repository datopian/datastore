"""JWT provider — verifies signature + optional `aud` / `iss` claims.

PyJWT does the heavy lifting; tests pin the provider's wrapping:
  - what becomes `Decision.subject` / `Decision.claims`,
  - which failure modes translate to `AuthorizationError`,
  - HS* vs RS* key wiring at construction time.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import jwt
import pytest
from datastore.auth.jwt import Provider as JWTAuthProvider
from datastore.core.config import Config
from datastore.core.exceptions import AuthorizationError

SECRET = "topsecret"


def _hs256_config(**overrides: Any) -> Config:
    base = {
        "AUTH_TYPE": "jwt",
        "JWT_ALGORITHM": "HS256",
        "JWT_SECRET": SECRET,
        "CKAN_URL": "",
    }
    base.update(overrides)
    return Config(**base)


def _provider(**overrides: Any) -> JWTAuthProvider:
    return JWTAuthProvider(config=_hs256_config(**overrides))


def _authorize(provider: JWTAuthProvider, token: str | None):
    return asyncio.run(provider.authorize(
        credential=token,
        resource_id="r",
        package_id=None,
        permission="read",
    ))


def test_valid_token_returns_decision_with_subject_and_claims() -> None:
    token = jwt.encode({"sub": "user-1", "role": "admin"}, SECRET, algorithm="HS256")
    decision = _authorize(_provider(), token)
    assert decision.subject == "user-1"
    assert decision.claims == {"sub": "user-1", "role": "admin"}


def test_missing_credential_is_rejected_before_jwt_decode() -> None:
    with pytest.raises(AuthorizationError, match="JWT token required"):
        _authorize(_provider(), None)


def test_invalid_signature_raises_authorization_error() -> None:
    token = jwt.encode({"sub": "u"}, "wrong-secret", algorithm="HS256")
    with pytest.raises(AuthorizationError, match="invalid JWT"):
        _authorize(_provider(), token)


def test_expired_token_raises_authorization_error() -> None:
    past = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(seconds=5)
    token = jwt.encode({"sub": "u", "exp": past}, SECRET, algorithm="HS256")
    with pytest.raises(AuthorizationError, match="invalid JWT"):
        _authorize(_provider(), token)


def test_audience_mismatch_raises_authorization_error() -> None:
    provider = _provider(JWT_AUDIENCE="expected-aud")
    token = jwt.encode({"sub": "u", "aud": "other-aud"}, SECRET, algorithm="HS256")
    with pytest.raises(AuthorizationError, match="invalid JWT"):
        _authorize(provider, token)


def test_audience_match_passes() -> None:
    provider = _provider(JWT_AUDIENCE="expected-aud")
    token = jwt.encode(
        {"sub": "u", "aud": "expected-aud"}, SECRET, algorithm="HS256"
    )
    decision = _authorize(provider, token)
    assert decision.subject == "u"


def test_issuer_mismatch_raises_authorization_error() -> None:
    provider = _provider(JWT_ISSUER="expected-iss")
    token = jwt.encode({"sub": "u", "iss": "other-iss"}, SECRET, algorithm="HS256")
    with pytest.raises(AuthorizationError, match="invalid JWT"):
        _authorize(provider, token)


def test_missing_sub_claim_yields_subject_none() -> None:
    token = jwt.encode({"role": "guest"}, SECRET, algorithm="HS256")
    decision = _authorize(_provider(), token)
    assert decision.subject is None
    assert decision.claims == {"role": "guest"}


def test_garbled_token_raises_authorization_error() -> None:
    with pytest.raises(AuthorizationError, match="invalid JWT"):
        _authorize(_provider(), "not.a.real.jwt")


def test_key_id_uses_jti_for_caching() -> None:
    provider = _provider()
    token = jwt.encode({"sub": "u", "jti": "tok-1"}, SECRET, algorithm="HS256")
    assert provider.key_id(token) == "jti:tok-1"


def test_provider_name_is_jwt() -> None:
    assert _provider().name == "jwt"
