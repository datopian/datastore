"""`api/auth.py` orchestration — validation + anonymous-read policy.

Provider behaviour is tested per-provider in `tests/auth/<name>/`.
Caching is provider-specific (only CKAN caches) and lives in
`tests/auth/ckan/test_provider.py`. Here we pin only the cross-cutting
pieces that apply to every provider:
  - permission whitelist + resource_id XOR package_id validation;
  - anonymous-read policy (read with no api_key forwards to provider);
  - non-read with no api_key hard-fails before any provider call;
  - the dict shape returned to endpoints.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from datastore.api.auth import authorize
from datastore.auth.base import Decision
from datastore.core.exceptions import AuthorizationError, ValidationError


class FakeProvider:
    """Records calls and returns a canned Decision (or raises)."""

    name = "fake"

    def __init__(
        self,
        decision: Decision | None = None,
        raises: Exception | None = None,
    ) -> None:
        self._decision = decision or Decision(
            resource={"id": "res-1"}, package={"id": "pkg-1"}
        )
        self._raises = raises
        self.calls: list[dict[str, Any]] = []

    async def authorize(self, **kwargs: Any) -> Decision:
        self.calls.append(kwargs)
        if self._raises is not None:
            raise self._raises
        return self._decision

    def key_id(self, credential: str) -> str:
        return f"k:{credential}"


# --- happy path -------------------------------------------------------------


def test_provider_decision_is_returned_as_endpoint_data_dict_shape() -> None:
    provider = FakeProvider()
    result = asyncio.run(authorize(
        api_key="tok",
        provider=provider,
        resource_id="res-1",
        package_id=None,
        permission="read",
    ))

    assert result == {"resource": {"id": "res-1"}, "package": {"id": "pkg-1"}}
    assert provider.calls == [
        {
            "credential": "tok",
            "resource_id": "res-1",
            "package_id": None,
            "permission": "read",
        }
    ]


def test_decision_without_metadata_yields_empty_dicts() -> None:
    # Anonymous / JWT providers return Decision() with no resource/package;
    # endpoint code reads from the dict so we must substitute empty dicts.
    result = asyncio.run(authorize(
        api_key="tok",
        provider=FakeProvider(decision=Decision()),
        resource_id="res-1",
        package_id=None,
        permission="read",
    ))
    assert result == {"resource": {}, "package": {}}


# --- anonymous-read policy --------------------------------------------------


def test_anonymous_caller_for_read_passes_through_to_provider() -> None:
    provider = FakeProvider(decision=Decision())
    asyncio.run(authorize(
        api_key=None, provider=provider,
        resource_id="res-1", package_id=None, permission="read",
    ))
    assert provider.calls[0]["credential"] is None


@pytest.mark.parametrize("permission", ["create", "update", "delete", "patch"])
def test_anonymous_caller_rejected_for_non_read_permissions(permission: str) -> None:
    provider = FakeProvider()
    with pytest.raises(AuthorizationError, match="authenticated user"):
        asyncio.run(authorize(
            api_key=None, provider=provider,
            resource_id="res-1", package_id=None, permission=permission,  # type: ignore[arg-type]
        ))
    # Provider never reached — policy short-circuits first.
    assert provider.calls == []


# --- input validation -------------------------------------------------------


def test_must_supply_exactly_one_of_resource_or_package_id() -> None:
    provider = FakeProvider()
    with pytest.raises(ValidationError, match="resource_id or package_id"):
        asyncio.run(authorize(
            api_key="tok", provider=provider,
            resource_id="res-1", package_id="pkg-1", permission="read",
        ))
    with pytest.raises(ValidationError, match="resource_id or package_id"):
        asyncio.run(authorize(
            api_key="tok", provider=provider,
            resource_id=None, package_id=None, permission="read",
        ))


def test_invalid_permission_rejected_at_boundary() -> None:
    provider = FakeProvider()
    with pytest.raises(ValidationError, match="permission must be one of"):
        asyncio.run(authorize(
            api_key="tok", provider=provider,
            resource_id="res-1", package_id=None, permission="execute",  # type: ignore[arg-type]
        ))
    assert provider.calls == []


# --- failure modes ----------------------------------------------------------


def test_provider_authorization_error_propagates() -> None:
    provider = FakeProvider(raises=AuthorizationError("nope"))
    with pytest.raises(AuthorizationError, match="nope"):
        asyncio.run(authorize(
            api_key="tok", provider=provider,
            resource_id="res-1", package_id=None, permission="read",
        ))
