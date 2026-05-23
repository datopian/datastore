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
