"""Unit tests for the write service.

`create_datastore` is exercised directly with a fake context — no HTTP,
no FastAPI. Faster than the TestClient suite and isolates orchestration
from request plumbing.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from datastore.core.config import Config
from datastore.services.write import (
    create_datastore,
    delete_datastore,
    upsert_datastore,
)


class _FakeCKAN:
    """Just enough surface for `create_datastore`'s CKAN sync.

    Records `resource_create` calls and `resource_patch` calls so tests can
    assert the schema (and timestamp) are mirrored onto the resource.
    """

    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.patched: list[tuple[str, dict[str, Any]]] = []

    async def resource_create(self, *, resource: dict[str, Any]) -> dict[str, Any]:
        self.created.append(dict(resource))
        return {**resource, "id": resource.get("id") or "new-res-id"}

    async def resource_patch(
        self, *, resource_id: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        self.patched.append((resource_id, dict(patch)))
        return {"id": resource_id, **patch}


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


def test_create_syncs_schema_to_ckan_resource() -> None:
    """After the table write, the resource is patched with the schema and a
    refreshed `last_modified`, so CKAN metadata stays in sync with the
    datastore table."""
    ctx = _ctx()
    schema = _schema(primary_key=["a"])
    data_dict = {
        "package": {"id": "pkg-1"},
        "resource": "res-1",
        "schema": schema,
        "records": [],
    }

    asyncio.run(create_datastore(ctx, data_dict))

    assert len(ctx.ckan.patched) == 1
    rid, patch = ctx.ckan.patched[0]
    assert rid == "res-1"
    assert patch["schema"] == schema
    assert isinstance(patch["last_modified"], str) and patch["last_modified"]


def test_create_new_resource_also_syncs_schema() -> None:
    """The dict (new-resource) path patches the freshly-created resource id
    with the schema too."""
    ctx = _ctx()
    schema = _schema(primary_key=["a"])
    data_dict = {
        "package": {"id": "pkg-1"},
        "resource": {"package_id": "pkg-1", "name": "foo"},
        "schema": schema,
        "records": [],
    }

    asyncio.run(create_datastore(ctx, data_dict))

    rid, patch = ctx.ckan.patched[0]
    assert rid == "new-res-id"  # id returned by resource_create
    assert patch["schema"] == schema


def test_create_without_ckan_skips_schema_sync() -> None:
    """Standalone auth (no CKAN) — there's no resource to patch; create
    still succeeds without touching CKAN."""
    ctx = SimpleNamespace(config=Config(), ckan=None)
    data_dict = {
        "package": {"id": "pkg-1"},
        "resource": "res-1",
        "schema": _schema(),
        "records": [],
    }

    result = asyncio.run(create_datastore(ctx, data_dict))

    assert result.resource_id == "res-1"  # no error, no CKAN call


def test_upsert_syncs_timestamp_to_ckan() -> None:
    """Upsert changes data, not columns — it refreshes the resource
    timestamp (and activity log) but must NOT touch the schema."""
    ctx = _ctx()
    data_dict = {"resource_id": "res-1", "records": [{"a": 1}], "method": "upsert"}

    asyncio.run(upsert_datastore(ctx, data_dict))

    assert len(ctx.ckan.patched) == 1
    rid, patch = ctx.ckan.patched[0]
    assert rid == "res-1"
    assert patch["last_modified"]
    assert "schema" not in patch  # data-only op leaves the schema alone
    assert "url_type" not in patch


def test_delete_rows_syncs_timestamp_only() -> None:
    """Row delete (placeholder yields no schema) → timestamp + activity,
    no schema patch."""
    ctx = _ctx()
    data_dict = {"resource_id": "res-1", "filters": {"a": 1}}

    asyncio.run(delete_datastore(ctx, data_dict))

    rid, patch = ctx.ckan.patched[0]
    assert rid == "res-1"
    assert patch["last_modified"]
    assert "schema" not in patch


def test_delete_columns_syncs_new_schema_to_ckan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A column drop changes the schema — the reduced schema returned by
    the engine is mirrored to CKAN so the two stay consistent."""
    from datastore.infrastructure.engines.bigquery import BigQueryBackend

    new_schema = {"fields": [{"name": "a", "type": "integer"}]}

    def fake_delete(self: Any, *, resource_id: str, filters: Any, fields: Any) -> Any:
        return SimpleNamespace(schema=new_schema)

    monkeypatch.setattr(BigQueryBackend, "delete", fake_delete)

    ctx = _ctx()
    data_dict = {"resource_id": "res-1", "fields": ["b"]}

    asyncio.run(delete_datastore(ctx, data_dict))

    rid, patch = ctx.ckan.patched[0]
    assert rid == "res-1"
    assert patch["schema"] == new_schema
    assert patch["last_modified"]


def test_delete_whole_table_clears_schema_on_ckan() -> None:
    """Dropping the whole table (no filters, no fields) removes the
    BigQuery table — so the resource's schema is dropped too, keeping the
    two consistent."""
    ctx = _ctx()
    data_dict = {"resource_id": "res-1"}  # no filters, no fields → drop table

    asyncio.run(delete_datastore(ctx, data_dict))

    rid, patch = ctx.ckan.patched[0]
    assert rid == "res-1"
    assert patch["schema"] is None  # schema cleared on the resource
    assert patch["last_modified"]


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
