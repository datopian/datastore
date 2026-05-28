"""Auth orchestration — boundary validation + anonymous-read policy.

Provider-agnostic. Owns only the pieces that apply to every provider:
  - the anonymous-read policy (some permissions skip the credential check),
  - validation of `permission` and the `resource_id` XOR `package_id` rule.

Caching is a provider concern (network-bound providers cache; local ones
don't). Today only the CKAN provider caches — see `auth/ckan/provider.py`.

`RequestContext.authorize(...)` (in `api/context.py`) is the public seam
endpoints use; it delegates here.
"""

from __future__ import annotations

from typing import Any, Literal, get_args

from datastore.auth.base import AuthProvider
from datastore.core.exceptions import AuthorizationError, ValidationError

Permission = Literal["read", "create", "update", "delete", "patch"]
ALLOWED_PERMISSIONS: frozenset[str] = frozenset(get_args(Permission))

# Permissions an unauthenticated caller is allowed to attempt. For these
# we forward to the provider with `credential=None`; the provider decides
# (e.g. CKAN checks resource visibility). Anything outside this set
# hard-fails on missing credentials before the provider is called.
ANONYMOUS_PERMISSIONS: frozenset[str] = frozenset({"read"})


async def authorize(
    *,
    api_key: str | None,
    provider: AuthProvider,
    resource_id: str | None,
    package_id: str | None,
    permission: Permission | None = None,
) -> dict[str, Any]:
    """Run policy checks, delegate to the provider, return endpoint data_dict.

    Endpoints merge the returned dict into their `data_dict`:
      `{"resource": <dict or {}>, "package": <dict or {}>}`
    """
    if bool(resource_id) == bool(package_id):
        raise ValidationError("exactly one of resource_id or package_id required")

    if permission is not None and permission not in ALLOWED_PERMISSIONS:
        raise ValidationError(
            f"permission must be one of {sorted(ALLOWED_PERMISSIONS)}"
        )

    if not api_key and permission not in ANONYMOUS_PERMISSIONS:
        raise AuthorizationError(
            "Access denied: Action requires an authenticated user"
        )

    decision = await provider.authorize(
        credential=api_key,
        resource_id=resource_id,
        package_id=package_id,
        permission=permission,
    )
    return {"resource": decision.resource or {}, "package": decision.package or {}}


def ensure_resource_writable(
    resource: dict[str, Any], *, force: bool, auth_type: str
) -> None:
    """Block writes to a CKAN resource the datastore doesn't own, unless
    `force` is set.

    CKAN tags resources whose data the datastore owns with
    `url_type="datastore"`. Calling `datastore_create` / `_upsert` /
    `_delete` on a resource whose `url_type` is anything else (upload,
    link, …) would clobber externally-managed data, so we require
    `force=true` for that case — mirroring CKAN's own datastore_create
    check.

    Only applies under `AUTH_TYPE="ckan"` (other providers carry no CKAN
    resource record). Also skipped when `url_type` is absent — that
    means there's no existing CKAN resource yet (e.g. the dict-form of
    `datastore_create`, which materialises the resource on the fly).
    """
    if auth_type != "ckan":
        return
    url_type = resource.get("url_type")
    if url_type is None:
        return
    if not force and url_type != "datastore":
        raise ValidationError(
            'Cannot update a read-only resource. Use "force" to force update.'
        )
