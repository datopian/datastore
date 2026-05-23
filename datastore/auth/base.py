"""Auth provider contract — `AuthProvider` Protocol + `Decision` dataclass.

A provider answers: is this credential allowed to do `permission` on
this `resource_id` / `package_id`?

Providers RAISE `AuthorizationError` to deny; a returned `Decision`
always means allowed. `subject` and `claims` carry caller identity (when
known); `resource` and `package` carry CKAN-style metadata (CKAN
provider only — generic providers leave them None).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True, frozen=True)
class Decision:
    subject: str | None = None
    claims: dict[str, Any] | None = None
    resource: dict[str, Any] | None = None
    package: dict[str, Any] | None = None


class AuthProvider(Protocol):
    """Auth provider interface. One instance per app, built in lifespan."""

    name: str

    async def authorize(
        self,
        *,
        credential: str | None,
        resource_id: str | None,
        package_id: str | None,
        permission: str | None,
    ) -> Decision: ...

    def key_id(self, credential: str) -> str:
        """Stable, non-reversible id for cache keys. Raw credential never stored."""
        ...


def default_key_id(credential: str) -> str:
    """sha256 prefix of the full credential string.

    Security note: deliberately ignores any embedded JWT `jti` claim. An
    unverified `jti` from the token's payload can be forged to collide
    with a cached authorization decision for a different (verified)
    token — the cache lookup is keyed before signature verification, so
    a forged `jti:<value>` lookup would return the cached decision for
    the legitimate user with the same `jti`. Hashing the whole
    credential keeps the cache identity tied to bytes-on-the-wire and
    makes any collision strictly equivalent to a sha256 collision.
    """
    return "h:" + hashlib.sha256(credential.encode()).hexdigest()[:16]
