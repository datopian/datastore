from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any

from fastapi import Depends, Header
from starlette.requests import Request

from datastore.api import auth as auth_fns
from datastore.api.auth import Permission
from datastore.core.config import Config, get_config
from datastore.core.helper import parse_authorization_header
from datastore.infrastructure.cache import CachePort
from datastore.infrastructure.ckan_client import CKANClient

# --- FastAPI dependency seams ------------------------------------------------
ConfigDep = Annotated[Config, Depends(get_config)]


def get_cache(request: Request) -> CachePort:
    """Cache adapter installed by the app lifespan in `request.app.state.cache`."""
    cache = getattr(request.app.state, "cache", None)
    if cache is None:
        raise RuntimeError("cache is not initialised; check the lifespan wiring")
    return cache  # type: ignore[no-any-return]


def get_ckan_client(request: Request) -> CKANClient:
    """CKAN client installed by the app lifespan in `request.app.state.ckan`."""
    ckan = getattr(request.app.state, "ckan", None)
    if ckan is None:
        raise RuntimeError("ckan client is not initialised; check the lifespan wiring")
    return ckan  # type: ignore[no-any-return]


# --- AuthContext -------------------------------------------------------------
@dataclass(slots=True)
class AuthContext:
    """Per-request auth state. Delegates the real work to `app.api.auth`."""

    api_key: str | None = field(repr=False)
    cache: CachePort
    cache_ttl: int
    enabled: bool
    ckan: CKANClient

    async def authorize(
        self,
        resource_id: str | None = None,
        package_id: str | None = None,
        permission: Permission | None = None,
    ) -> dict[str, Any]:
        return await auth_fns.authorize(
            api_key=self.api_key,
            cache=self.cache,
            cache_ttl=self.cache_ttl,
            enabled=self.enabled,
            ckan=self.ckan,
            resource_id=resource_id,
            package_id=package_id,
            permission=permission,
        )


# --- RequestContext ----------------------------------------------------------
@dataclass(slots=True)
class RequestContext:
    """Per-request facade — the one dep an endpoint takes.

    Usage:
        async def handler(payload: ..., context: Context):
            decision = await ctx.auth.authorize(resource_id=..., permission=...)
            created  = await ctx.ckan.resource_create(resource=...)

    Add new sub-contexts here as the app grows (e.g. `engine`, `events`).
    """

    config: Config
    auth: AuthContext
    ckan: CKANClient


def get_context(
    config: ConfigDep,
    cache: Annotated[CachePort, Depends(get_cache)],
    ckan: Annotated[CKANClient, Depends(get_ckan_client)],
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> RequestContext:
    api_key = parse_authorization_header(authorization)
    bound_ckan = ckan.bind(api_key)
    auth = AuthContext(
        api_key=api_key,
        cache=cache,
        cache_ttl=config.AUTH_CACHE_TTL,
        enabled=config.AUTH_ENABLED,
        ckan=bound_ckan,
    )
    return RequestContext(config=config, auth=auth, ckan=bound_ckan)


Context = Annotated[RequestContext, Depends(get_context)]
