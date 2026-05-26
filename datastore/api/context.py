from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any

from fastapi import Depends, Header
from starlette.requests import Request

from datastore.api import auth as auth_fns
from datastore.api.auth import Permission
from datastore.auth.base import AuthProvider
from datastore.core.config import Config, get_config
from datastore.core.helper import parse_authorization_header
from datastore.infrastructure.ckan_client import CKANClient

ConfigDep = Annotated[Config, Depends(get_config)]


def get_ckan_client(request: Request) -> CKANClient | None:
    """CKAN client installed by the app lifespan in `request.app.state.ckan`.

    `None` under non-CKAN auth (the lifespan skips construction when
    `AUTH_TYPE != "ckan"` — the datastore runs standalone).
    """
    return getattr(request.app.state, "ckan", None)


def get_auth_provider(request: Request) -> AuthProvider:
    """Auth provider installed by the app lifespan."""
    provider = getattr(request.app.state, "auth_provider", None)
    if provider is None:
        raise RuntimeError(
            "auth provider is not initialised; check the lifespan wiring"
        )
    return provider  # type: ignore[no-any-return]


@dataclass(slots=True)
class RequestContext:
    """Per-request facade — the one dep an endpoint takes.

    `ckan` is None under non-CKAN auth (the datastore runs standalone).
    Code paths that need CKAN — today only `datastore_create`'s `resource`
    dict branch — must guard for that.

    Usage:
        async def handler(payload: ..., context: Context):
            data_dict = await ctx.authorize(resource_id=..., permission=...)
            if ctx.ckan is not None:
                created = await ctx.ckan.resource_create(resource=...)
    """

    config: Config
    api_key: str | None = field(repr=False)
    auth_provider: AuthProvider
    ckan: CKANClient | None

    async def authorize(
        self,
        resource_id: str | None = None,
        package_id: str | None = None,
        permission: Permission | None = None,
    ) -> dict[str, Any]:
        return await auth_fns.authorize(
            api_key=self.api_key,
            provider=self.auth_provider,
            resource_id=resource_id,
            package_id=package_id,
            permission=permission,
        )


def get_context(
    config: ConfigDep,
    ckan: Annotated[CKANClient | None, Depends(get_ckan_client)],
    provider: Annotated[AuthProvider, Depends(get_auth_provider)],
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> RequestContext:
    api_key = parse_authorization_header(authorization)
    return RequestContext(
        config=config,
        api_key=api_key,
        auth_provider=provider,
        ckan=ckan.bind(api_key) if ckan is not None else None,
    )


Context = Annotated[RequestContext, Depends(get_context)]
