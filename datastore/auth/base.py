"""Auth provider contract — `AuthProvider` Protocol + `Decision` dataclass.

A provider answers: is this credential allowed to do `permission` on
this `resource_id` / `package_id`?

Providers RAISE `AuthorizationError` to deny; a returned `Decision`
always means allowed. `subject` and `claims` carry caller identity (when
known); `resource` and `package` carry CKAN-style metadata (CKAN
provider only — generic providers leave them None).
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from typing import Any, Protocol

import orjson


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
    """JWT `jti` if the credential is a JWT; sha256 prefix otherwise.

    Shared by providers that accept either opaque or JWT tokens.
    """
    parts = credential.split(".")
    if len(parts) == 3:
        try:
            segment = parts[1]
            padded = segment + "=" * (-len(segment) % 4)
            payload = orjson.loads(base64.urlsafe_b64decode(padded))
            if isinstance(payload, dict):
                jti = payload.get("jti")
                if isinstance(jti, str) and jti:
                    return f"jti:{jti}"
        except (ValueError, TypeError, orjson.JSONDecodeError):
            pass
    return "h:" + hashlib.sha256(credential.encode()).hexdigest()[:16]
