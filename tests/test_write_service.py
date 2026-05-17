"""Unit tests for the write service.

`create_datastore` is exercised directly with a fake context — no HTTP,
no FastAPI. Faster than the TestClient suite and isolates orchestration
from request plumbing.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from datastore.core.config import Config
from datastore.services.write import create_datastore


class _FakeCKAN:
    """Just enough surface for `create_datastore`'s new-resource branch."""

    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []

    async def resource_create(self, *, resource: dict[str, Any]) -> dict[str, Any]:
        self.created.append(dict(resource))
        return {**resource, "id": resource.get("id") or "new-res-id"}


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(config=Config(), ckan=_FakeCKAN())


def test_existing_resource_skips_resource_create() -> None:
    ctx = _ctx()
    data_dict = {
        "package": {"id": "pkg-1"},
        "resource": "existing-resource-id",  # str → existing flow
        "fields": [{"id": "a", "type": "int4"}],
        "primary_key": ["a"],
        "records": [{"a": 1}, {"a": 2}],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.resource_id == "existing-resource-id"
    assert result.package_id == "pkg-1"
    assert result.primary_key == ["a"]
    assert result.records_inserted == 2
    assert ctx.ckan.created == []  # no CKAN call


def test_new_resource_creates_via_ckan() -> None:
    ctx = _ctx()
    data_dict = {
        "package": {"id": "pkg-1"},
        "resource": {"package_id": "pkg-1", "name": "foo"},  # dict → new flow
        "fields": [{"id": "a"}],
        "primary_key": ["a"],
        "records": [],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.resource_id == "new-res-id"
    assert result.package_id == "pkg-1"
    assert result.records_inserted == 0
    assert len(ctx.ckan.created) == 1
    assert ctx.ckan.created[0]["package_id"] == "pkg-1"


def test_missing_records_counts_zero() -> None:
    """`records` may be omitted entirely — service should default to []."""
    ctx = _ctx()
    data_dict = {
        "package": {"id": "pkg-x"},
        "resource": "res-x",
        "fields": [{"id": "a"}],
        "primary_key": ["a"],
        # records absent
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.records_inserted == 0


def test_primary_key_defaults_to_empty_list() -> None:
    ctx = _ctx()
    data_dict = {
        "package": {"id": "pkg-x"},
        "resource": "res-x",
        "fields": [{"id": "a"}],
        # primary_key absent
        "records": [],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.primary_key == []


def test_missing_package_returns_none_package_id() -> None:
    """If auth somehow returns no package block, the response still serializes."""
    ctx = _ctx()
    data_dict = {
        # no "package" key
        "resource": "res-x",
        "fields": [{"id": "a"}],
        "primary_key": ["a"],
        "records": [{"a": 1}],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.package_id is None
    assert result.records_inserted == 1
