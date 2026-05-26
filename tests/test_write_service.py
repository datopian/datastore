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
from datastore.services.write import create_datastore, upsert_datastore


class _FakeCKAN:
    """Just enough surface for `create_datastore`'s new-resource branch."""

    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []

    async def resource_create(self, *, resource: dict[str, Any]) -> dict[str, Any]:
        self.created.append(dict(resource))
        return {**resource, "id": resource.get("id") or "new-res-id"}


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(config=Config(), ckan=_FakeCKAN())


def _schema(primary_key: list[str] | None = None) -> dict[str, Any]:
    """Minimal canonical frictionless schema for service-level tests.

    The request validator folds legacy `fields`/`primary_key` into this
    shape; tests that bypass the boundary build it directly.
    """
    schema: dict[str, Any] = {"fields": [{"name": "a", "type": "integer"}]}
    if primary_key:
        schema["primaryKey"] = primary_key
    return schema


def test_existing_resource_skips_resource_create() -> None:
    ctx = _ctx()
    data_dict = {
        "package": {"id": "pkg-1"},
        "resource": "existing-resource-id",  # str → existing flow
        "schema": _schema(primary_key=["a"]),
        "records": [{"a": 1}, {"a": 2}],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.resource_id == "existing-resource-id"
    assert result.package_id == "pkg-1"
    # Top-level `primary_key` and `schema.primaryKey` carry the same value.
    assert result.primary_key == ["a"]
    assert result.schema["primaryKey"] == ["a"]
    assert ctx.ckan.created == []  # no CKAN call


def test_new_resource_creates_via_ckan() -> None:
    ctx = _ctx()
    data_dict = {
        "package": {"id": "pkg-1"},
        "resource": {"package_id": "pkg-1", "name": "foo"},  # dict → new flow
        "schema": _schema(primary_key=["a"]),
        "records": [],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.resource_id == "new-res-id"
    assert result.package_id == "pkg-1"
    assert len(ctx.ckan.created) == 1
    assert ctx.ckan.created[0]["package_id"] == "pkg-1"


def test_missing_records_is_handled() -> None:
    """`records` may be omitted entirely — service should default to [] and not echo."""
    ctx = _ctx()
    data_dict = {
        "package": {"id": "pkg-x"},
        "resource": "res-x",
        "schema": _schema(primary_key=["a"]),
        # records absent
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.records is None  # include_records defaults to False


def test_schema_without_primary_key_is_accepted() -> None:
    """A schema with no `primaryKey` is valid — the response just omits it."""
    ctx = _ctx()
    data_dict = {
        "package": {"id": "pkg-x"},
        "resource": "res-x",
        "schema": _schema(),  # no primaryKey
        "records": [],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert "primaryKey" not in result.schema
    assert result.primary_key == []


def test_missing_package_returns_none_package_id() -> None:
    """If auth somehow returns no package block, the response still serializes."""
    ctx = _ctx()
    data_dict = {
        # no "package" key
        "resource": "res-x",
        "schema": _schema(primary_key=["a"]),
        "records": [{"a": 1}],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.package_id is None


# --- upsert_datastore -------------------------------------------------------


def test_upsert_returns_typed_result() -> None:
    ctx = _ctx()
    data_dict = {
        "resource_id": "res-1",
        "records": [{"a": 1}, {"a": 2}],
        "method": "upsert",
    }

    result = asyncio.run(upsert_datastore(ctx, data_dict))

    assert result.resource_id == "res-1"
    assert result.method == "upsert"
    # Optional fields stay None when their flags default to False — the
    # exclude_none serializer drops them from the wire body.
    assert result.records is None
    assert result.total is None


def test_upsert_default_method_is_upsert() -> None:
    """`method` is optional; absence resolves to 'upsert' inside the service."""
    ctx = _ctx()
    result = asyncio.run(
        upsert_datastore(
            ctx,
            {
                "resource_id": "res-1",
                "records": [{"a": 1}],
                # method absent
            },
        )
    )

    assert result.method == "upsert"


def test_upsert_echoes_records_when_include_records() -> None:
    ctx = _ctx()
    records = [{"a": 1}, {"a": 2}]
    result = asyncio.run(
        upsert_datastore(
            ctx,
            {
                "resource_id": "res-1",
                "records": records,
                "method": "upsert",
                "include_records": True,
            },
        )
    )

    assert result.records == records


def test_upsert_returns_total_when_include_total() -> None:
    """BigQuery placeholder returns `total=len(records)`; the service lifts it."""
    ctx = _ctx()
    result = asyncio.run(
        upsert_datastore(
            ctx,
            {
                "resource_id": "res-1",
                "records": [{"a": 1}, {"a": 2}],
                "method": "upsert",
                "include_total": True,
            },
        )
    )

    assert result.total == 2


def test_upsert_omits_total_when_include_total_false() -> None:
    """Even if the engine populates total, the service gates on the request flag."""
    ctx = _ctx()
    result = asyncio.run(
        upsert_datastore(
            ctx,
            {
                "resource_id": "res-1",
                "records": [{"a": 1}, {"a": 2}],
                "method": "upsert",
                "include_total": False,
            },
        )
    )

    assert result.total is None


def test_upsert_records_optional() -> None:
    """`records` may be omitted — service defaults to [] and doesn't crash."""
    ctx = _ctx()
    result = asyncio.run(
        upsert_datastore(
            ctx,
            {
                "resource_id": "res-1",
                "method": "upsert",
                # records absent
            },
        )
    )

    assert result.resource_id == "res-1"
    assert result.records is None  # include_records defaults to False
