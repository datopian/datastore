"""CKAN provider — binds api_key per call, maps result to Decision, caches.

The provider holds an unbound `CKANClient` + a TTL cache; each `authorize()`
call clones the client with the caller's credential and wraps the round
trip in the cache. Tests use a small fake CKAN to pin both the binding /
mapping and the cache hit/miss/fail-open behaviour.
"""

from __future__ import annotations

import asyncio
from typing import Any

import jwt
import orjson
import pytest
from datastore.auth.ckan import Provider as CKANAuthProvider
from datastore.core.exceptions import AuthorizationError
from datastore.infrastructure.cache import InMemoryCache


class FakeCKAN:
    """Minimal stand-in for `CKANClient` — records the bound key + call args."""

    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self._bound_key: str | None = None
        self._result = result or {
            "package": {"id": "pkg-1"},
            "resource": {"id": "res-1", "package_id": "pkg-1"},
        }
        self.calls: list[dict[str, Any]] = []
        self.raise_on_authorize: Exception | None = None

    def bind(self, api_key: str | None) -> "FakeCKAN":
        clone = FakeCKAN(self._result)
        clone._bound_key = api_key
        clone.calls = self.calls  # share so test sees calls regardless of clone
        clone.raise_on_authorize = self.raise_on_authorize
        return clone

    async def datastore_authorize(
        self,
        *,
        resource_id: str | None,
        package_id: str | None,
        permission: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "bound_key": self._bound_key,
                "resource_id": resource_id,
                "package_id": package_id,
                "permission": permission,
            }
        )
        if self.raise_on_authorize is not None:
            raise self.raise_on_authorize
        return self._result


class ExplodingCache:
    """CachePort stand-in — every op raises. Verifies fail-open behaviour."""

    async def get(self, key: str) -> bytes | None:
        raise RuntimeError("cache down")

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        raise RuntimeError("cache down")


def _provider(
    ckan: FakeCKAN | None = None,
    cache: InMemoryCache | ExplodingCache | None = None,
    cache_ttl: int = 60,
) -> CKANAuthProvider:
    return CKANAuthProvider(
        ckan=ckan or FakeCKAN(),
        cache=cache or InMemoryCache(),
        cache_ttl=cache_ttl,
    )


# --- mapping + binding ------------------------------------------------------


def test_authorize_binds_credential_and_maps_response_to_decision() -> None:
    ckan = FakeCKAN()
    provider = _provider(ckan=ckan)

    decision = asyncio.run(provider.authorize(
        credential="token-xyz",
        resource_id="res-1",
        package_id=None,
        permission="read",
    ))

    assert ckan.calls == [
        {
            "bound_key": "token-xyz",
            "resource_id": "res-1",
            "package_id": None,
            "permission": "read",
        }
    ]
    # `subject` carries a hash of the credential (raw key never leaves
    # this provider). Same shape as `key_id`.
    assert decision.subject == provider.key_id("token-xyz")
    assert decision.resource == {"id": "res-1", "package_id": "pkg-1"}
    assert decision.package == {"id": "pkg-1"}
    assert decision.claims is None


def test_authorize_propagates_ckan_authorization_error() -> None:
    ckan = FakeCKAN()
    ckan.raise_on_authorize = AuthorizationError("denied")
    provider = _provider(ckan=ckan)

    with pytest.raises(AuthorizationError, match="denied"):
        asyncio.run(provider.authorize(
            credential="t", resource_id="r", package_id=None, permission="read",
        ))


def test_authorize_handles_missing_metadata_fields() -> None:
    # CKAN's package-scoped flow returns no `resource` dict; mapping
    # must tolerate that (Decision.resource just stays None).
    ckan = FakeCKAN(result={"package": {"id": "pkg-1"}})
    provider = _provider(ckan=ckan)

    decision = asyncio.run(provider.authorize(
        credential="t", resource_id=None, package_id="pkg-1", permission="create",
    ))
    assert decision.package == {"id": "pkg-1"}
    assert decision.resource is None


# --- cache hit / miss / errors ----------------------------------------------


def test_cache_hit_skips_ckan_on_second_call() -> None:
    ckan = FakeCKAN()
    cache = InMemoryCache()
    provider = _provider(ckan=ckan, cache=cache)

    asyncio.run(provider.authorize(
        credential="tok", resource_id="res-1", package_id=None, permission="read",
    ))
    asyncio.run(provider.authorize(
        credential="tok", resource_id="res-1", package_id=None, permission="read",
    ))

    # CKAN called exactly once across both authorizations.
    assert len(ckan.calls) == 1


def test_cache_key_uses_anon_marker_when_no_credential() -> None:
    ckan = FakeCKAN()
    cache = InMemoryCache()
    provider = _provider(ckan=ckan, cache=cache)

    asyncio.run(provider.authorize(
        credential=None, resource_id="res-1", package_id=None, permission="read",
    ))

    # Verify by hitting again with the same shape — second call must be cached.
    asyncio.run(provider.authorize(
        credential=None, resource_id="res-1", package_id=None, permission="read",
    ))
    assert len(ckan.calls) == 1


def test_separate_credentials_get_separate_cache_entries() -> None:
    ckan = FakeCKAN()
    cache = InMemoryCache()
    provider = _provider(ckan=ckan, cache=cache)

    asyncio.run(provider.authorize(
        credential="user-a", resource_id="r", package_id=None, permission="read",
    ))
    asyncio.run(provider.authorize(
        credential="user-b", resource_id="r", package_id=None, permission="read",
    ))

    # Two distinct cache entries → two CKAN calls.
    assert len(ckan.calls) == 2


def test_package_scoped_call_uses_pkg_cache_namespace() -> None:
    ckan = FakeCKAN()
    cache = InMemoryCache()
    provider = _provider(ckan=ckan, cache=cache)

    # res-scoped and pkg-scoped calls share neither key nor cache entry.
    asyncio.run(provider.authorize(
        credential="tok", resource_id="x", package_id=None, permission="read",
    ))
    asyncio.run(provider.authorize(
        credential="tok", resource_id=None, package_id="x", permission="create",
    ))
    assert len(ckan.calls) == 2


def test_cache_failure_falls_through_to_ckan() -> None:
    ckan = FakeCKAN()
    provider = _provider(ckan=ckan, cache=ExplodingCache())

    # Fail-open: a broken cache must not break the request.
    decision = asyncio.run(provider.authorize(
        credential="tok", resource_id="res-1", package_id=None, permission="read",
    ))

    assert decision.resource == {"id": "res-1", "package_id": "pkg-1"}
    assert len(ckan.calls) == 1


def test_malformed_cache_entry_falls_through_to_ckan() -> None:
    # A poisoned cache value (not a JSON dict) is treated as a miss.
    # Blocking auth on a corrupt cache entry would be a self-inflicted
    # outage; we log + re-query CKAN instead.
    ckan = FakeCKAN()
    cache = InMemoryCache()
    provider = _provider(ckan=ckan, cache=cache)
    cache_key = (
        f"auth:ckan:{provider.key_id('tok')}:res:res-1:read"
    )
    asyncio.run(cache.set(cache_key, orjson.dumps("not-a-dict"), 60))

    decision = asyncio.run(provider.authorize(
        credential="tok", resource_id="res-1", package_id=None, permission="read",
    ))

    # Fell back to CKAN and got the canned decision.
    assert decision.resource == {"id": "res-1", "package_id": "pkg-1"}
    assert len(ckan.calls) == 1


def test_subject_in_cached_decision_is_hashed_not_raw_credential() -> None:
    # Security: the raw credential must never end up in the cache.
    # `Decision.subject` is what gets serialised — store the hash.
    ckan = FakeCKAN()
    provider = _provider(ckan=ckan, cache=InMemoryCache())

    decision = asyncio.run(provider.authorize(
        credential="raw-api-key-do-not-leak",
        resource_id="res-1", package_id=None, permission="read",
    ))

    assert decision.subject is not None
    assert "raw-api-key-do-not-leak" not in decision.subject
    assert decision.subject.startswith("h:")


# --- key derivation + name --------------------------------------------------


def test_key_id_hashes_credentials_regardless_of_jwt_shape() -> None:
    # JWTs and opaque tokens both go through sha256 — never trust an
    # unverified JWT claim for cache identity.
    jwt_tok = jwt.encode({"sub": "u", "jti": "tok-42"}, "k", algorithm="HS256")
    assert _provider().key_id(jwt_tok).startswith("h:")
    assert _provider().key_id("opaque-api-key").startswith("h:")


def test_provider_name_is_ckan() -> None:
    assert _provider().name == "ckan"
