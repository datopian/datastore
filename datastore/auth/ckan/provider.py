"""CKAN provider — defers to `/api/3/action/datastore_authorize`, with TTL cache.

Caching is scoped to this provider: CKAN's `datastore_authorize` is a
network round-trip on every call, so wrapping it with a TTL cache cuts
duplicate work. Other providers (JWT signature check, anonymous no-op)
are local and cheap — they don't need caching, so the cache lives here
rather than in the orchestration layer.
"""

from __future__ import annotations

import logging
from typing import Any

import orjson

from datastore.auth.base import Decision, default_key_id
from datastore.core.exceptions import AuthorizationError
from datastore.infrastructure.cache import CachePort
from datastore.infrastructure.ckan_client import CKANClient

log = logging.getLogger(__name__)


class CKANAuthProvider:
    name = "ckan"

    def __init__(
        self,
        *,
        ckan: CKANClient,
        cache: CachePort,
        cache_ttl: int,
        **_: object,
    ) -> None:
        self._ckan = ckan
        self._cache = cache
        self._cache_ttl = cache_ttl

    async def authorize(
        self,
        *,
        credential: str | None,
        resource_id: str | None,
        package_id: str | None,
        permission: str | None,
    ) -> Decision:
        scope, target = ("res", resource_id) if resource_id else ("pkg", package_id)
        assert target is not None  # orchestration validates one-of upstream
        cache_key = self._cache_key(credential, scope, target, permission)

        cached = await _safe_get(self._cache, cache_key)
        if cached is not None:
            try:
                decision = _decision_from_bytes(cached)
                log.debug(
                    "ckan auth cache HIT  scope=%s target=%s perm=%s",
                    scope, target, permission,
                )
                return decision
            except (AuthorizationError, ValueError, TypeError) as e:
                # Treat a corrupt cache entry as a miss — fall through
                # to CKAN. Blocking auth on a poisoned cache would be a
                # self-inflicted outage.
                log.warning(
                    "ckan auth cache entry malformed for scope=%s target=%s: "
                    "%s — falling back to CKAN",
                    scope, target, e,
                )

        log.debug(
            "ckan auth cache MISS scope=%s target=%s perm=%s -> CKAN",
            scope, target, permission,
        )
        ckan = self._ckan.bind(credential)
        result = await ckan.datastore_authorize(
            resource_id=resource_id,
            package_id=package_id,
            permission=permission,
        )
        # `subject` rides through the cache (orjson-serialised). Never
        # store the raw credential there — use the same hash we already
        # derive for the cache key.
        decision = Decision(
            subject=self.key_id(credential) if credential else None,
            resource=result.get("resource"),
            package=result.get("package"),
        )
        await _safe_set(
            self._cache, cache_key, _decision_to_bytes(decision), self._cache_ttl,
        )
        return decision

    def key_id(self, credential: str) -> str:
        return default_key_id(credential)

    def _cache_key(
        self,
        credential: str | None,
        scope: str,
        target: str,
        permission: str | None,
    ) -> str:
        key_id = self.key_id(credential) if credential else "anon"
        return f"auth:ckan:{key_id}:{scope}:{target}:{permission}"


# --- cache plumbing ----------------------------------------------------------
# Fail-open: cache failures must not block the request. We log and fall
# through to CKAN (a slow request is better than a wrong one).


async def _safe_get(cache: CachePort, key: str) -> bytes | None:
    try:
        return await cache.get(key)
    except Exception:  # noqa: BLE001
        log.warning("ckan auth cache GET failed; falling back to CKAN", exc_info=True)
        return None


async def _safe_set(cache: CachePort, key: str, value: bytes, ttl: int) -> None:
    try:
        await cache.set(key, value, ttl)
    except Exception:  # noqa: BLE001
        log.warning("ckan auth cache SET failed; skipping cache", exc_info=True)


def _decision_to_bytes(d: Decision) -> bytes:
    return orjson.dumps(
        {"subject": d.subject, "claims": d.claims,
         "resource": d.resource, "package": d.package},
    )


def _decision_from_bytes(value: bytes) -> Decision:
    parsed: Any = orjson.loads(value)
    if not isinstance(parsed, dict):
        raise AuthorizationError("cached auth entry is malformed")
    return Decision(
        subject=parsed.get("subject"),
        claims=parsed.get("claims"),
        resource=parsed.get("resource"),
        package=parsed.get("package"),
    )
