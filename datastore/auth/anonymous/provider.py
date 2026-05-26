"""Anonymous provider — always allows, no identity.

Use for local dev or CI without a real auth backend. Every call returns
an empty `Decision`; no signature, no claims, no resource metadata.
"""

from __future__ import annotations

from datastore.auth.base import Decision


class AnonymousAuthProvider:
    name = "anonymous"

    def __init__(self, **_: object) -> None:
        pass

    async def authorize(
        self,
        *,
        credential: str | None,
        resource_id: str | None,
        package_id: str | None,
        permission: str | None,
    ) -> Decision:
        return Decision()

    def key_id(self, credential: str) -> str:
        return "anon"
