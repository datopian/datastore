"""Registry dispatch — `AUTH_TYPE` selects the right provider class.

Verifies the importlib-based factory: each provider package's `Provider`
symbol is what gets returned, kwargs are forwarded, and constructor
errors propagate (so the lifespan can surface them at startup).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from datastore.auth.anonymous import Provider as AnonymousProvider
from datastore.auth.ckan import Provider as CKANProvider
from datastore.auth.jwt import Provider as JWTProvider
from datastore.auth.registry import get_auth_provider
from datastore.core.config import Config
from datastore.infrastructure.cache import InMemoryCache


def test_anonymous_type_returns_anonymous_provider() -> None:
    cfg = Config(AUTH_TYPE="anonymous", CKAN_URL="")
    provider = get_auth_provider(cfg)
    assert isinstance(provider, AnonymousProvider)
    assert provider.name == "anonymous"


def test_ckan_type_returns_ckan_provider_and_forwards_kwargs() -> None:
    cfg = Config(AUTH_TYPE="ckan", CKAN_URL="http://ckan.test")
    ckan = MagicMock()
    provider = get_auth_provider(
        cfg, ckan=ckan, cache=InMemoryCache(), cache_ttl=60,
    )
    assert isinstance(provider, CKANProvider)
    assert provider.name == "ckan"


def test_jwt_type_returns_jwt_provider_with_hs_secret() -> None:
    cfg = Config(AUTH_TYPE="jwt", JWT_SECRET="topsecret", CKAN_URL="")
    provider = get_auth_provider(cfg)
    assert isinstance(provider, JWTProvider)
    assert provider.name == "jwt"


def test_each_call_returns_a_fresh_instance() -> None:
    # The factory doesn't memoize — instance reuse is the caller's job
    # (the lifespan builds once and stashes on app.state).
    cfg = Config(AUTH_TYPE="anonymous", CKAN_URL="")
    assert get_auth_provider(cfg) is not get_auth_provider(cfg)


def test_unknown_auth_type_rejected_at_config_validation() -> None:
    # The Config validator checks against the directories on disk —
    # nothing exotic; just verify the boundary fails fast.
    with pytest.raises(ValueError, match="AUTH_TYPE"):
        Config(AUTH_TYPE="does_not_exist", CKAN_URL="")


def test_jwt_provider_raises_when_hs_secret_missing() -> None:
    # Constructor errors must propagate so the lifespan surfaces them
    # at startup rather than on the first request.
    cfg = Config(AUTH_TYPE="jwt", JWT_ALGORITHM="HS256", JWT_SECRET="", CKAN_URL="")
    with pytest.raises(ValueError, match="JWT_SECRET"):
        get_auth_provider(cfg)


def test_jwt_provider_raises_when_rs_public_key_missing() -> None:
    cfg = Config(
        AUTH_TYPE="jwt", JWT_ALGORITHM="RS256", JWT_PUBLIC_KEY="", CKAN_URL=""
    )
    with pytest.raises(ValueError, match="JWT_PUBLIC_KEY"):
        get_auth_provider(cfg)
