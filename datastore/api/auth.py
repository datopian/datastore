"""CKAN authorization — pure async functions. No state, no FastAPI.

`AuthContext` (in `app/api/context.py`) wraps these into a per-request
object: it holds the state (api_key, cache, ttl, enabled) and exposes
methods that delegate here.
"""

from __future__ import annotations

import base64
import hashlib
import logging
from typing import Any, Literal, get_args

import orjson

from datastore.core.exceptions import AuthorizationError
from datastore.infrastructure.cache import CachePort
from datastore.infrastructure.ckan_client import CKANClient

log = logging.getLogger(__name__)

Permission = Literal["read", "create", "update", "delete", "patch"]
ALLOWED_PERMISSIONS: frozenset[str] = frozenset(get_args(Permission))


# --- public ------------------------------------------------------------------
async def authorize(
    *,
    api_key: str | None,
    cache: CachePort,
    cache_ttl: int,
    enabled: bool,
    ckan: CKANClient,
    resource_id: str | None,
    package_id: str | None,
    permission: Permission | None = None,
) -> dict[str, Any]:
    """CKAN `datastore_authorize` with TTL cache.

    """
    if bool(resource_id) == bool(package_id):
        raise ValueError("exactly one of resource_id or package_id required")

    if permission is not None and permission not in ALLOWED_PERMISSIONS:
        raise ValueError(f"permission must be one of {sorted(ALLOWED_PERMISSIONS)}")

    if not enabled:
        log.debug("auth disabled; returning stub for resource_id=%s package_id=%s",
                  resource_id, package_id)
        return _disabled_stub(resource_id, package_id)

    if not api_key:
        raise AuthorizationError("Access denied: Action requires an authenticated user")

    # Adapter enforces TTL: `cache.set(..., ttl=cache_ttl)` writes an entry
    # that expires `cache_ttl` seconds after this write.
    scope, target = ("res", resource_id) if resource_id else ("pkg", package_id)
    assert target is not None  # narrowed by the validation above
    cache_key = _cache_key(api_key, scope, target, permission)

    cached = await _safe_get(cache, cache_key)
    if cached is not None:
        log.debug("auth cache HIT  scope=%s target=%s perm=%s", scope, target, permission)
        return _decode(cached)

    log.debug("auth cache MISS scope=%s target=%s perm=%s -> CKAN", scope, target, permission)
    result = await ckan.datastore_authorize(
        resource_id=resource_id,
        package_id=package_id,
        permission=permission,
    )
    await _safe_set(cache, cache_key, orjson.dumps(result), cache_ttl)
    log.debug("auth cache STORE scope=%s target=%s perm=%s ttl=%ds",
              scope, target, permission, cache_ttl)
    return result


# --- cache helpers -----------------------------------------------------------

def _cache_key(
    api_key: str,
    scope: str,
    identifier: str,
    permission: str | None,
) -> str:
    return f"auth:{_key_id(api_key)}:{scope}:{identifier}:{permission}"


async def _safe_get(cache: CachePort, key: str) -> bytes | None:
    try:
        return await cache.get(key)
    except Exception:  # noqa: BLE001 — cache failure must not block requests
        log.warning("auth cache GET failed; falling back to CKAN", exc_info=True)
        return None


async def _safe_set(cache: CachePort, key: str, value: bytes, ttl: int) -> None:
    try:
        await cache.set(key, value, ttl)
    except Exception:  # noqa: BLE001 — same fail-open policy on writes
        log.warning("auth cache SET failed; skipping cache", exc_info=True)


# --- pure helpers ------------------------------------------------------------


def _key_id(api_key: str) -> str:
    """Stable, non-reversible id for the api_key.

    JWT tokens use their `jti` claim; opaque tokens use a sha256 prefix.
    The raw key never reaches the cache.
    """
    jti = _jwt_jti(api_key)
    if jti:
        return f"jti:{jti}"
    return "h:" + hashlib.sha256(api_key.encode()).hexdigest()[:16]


def _jwt_jti(token: str) -> str | None:
    """Extract the `jti` claim from an unverified JWT, or None if not a JWT."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        segment = parts[1]
        padded = segment + "=" * (-len(segment) % 4)
        payload = orjson.loads(base64.urlsafe_b64decode(padded))
    except (ValueError, TypeError, orjson.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    jti = payload.get("jti")
    return jti if isinstance(jti, str) and jti else None


def _disabled_stub(
    resource_id: str | None, package_id: str | None
) -> dict[str, Any]:
    """Decision returned when `AUTH_ENABLED=false` (local dev / CI without CKAN)."""
    if resource_id is not None:
        return {
            "package": {"id": None, "_auth_disabled": True},
            "resource": {"id": resource_id, "_auth_disabled": True},
        }
    return {
        "package": {"id": package_id, "_auth_disabled": True},
        "resource": {"package_id": package_id, "_auth_disabled": True},
    }


def _decode(value: bytes) -> dict[str, Any]:
    parsed = orjson.loads(value)
    if not isinstance(parsed, dict):
        raise AuthorizationError("cached auth entry is malformed")
    return parsed
