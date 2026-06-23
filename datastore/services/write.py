from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.responses import (
    DatastoreCreateResponse,
    DatastoreDeleteResponse,
    DatastoreUpsertResponse,
)
from datastore.schemas.validators import frictionless_schema_to_fields

if TYPE_CHECKING:  # type-only — no runtime import from api/
    from datastore.api.context import RequestContext


def _utc_now_iso() -> str:
    """Naive UTC ISO timestamp, matching CKAN's stored datetime format."""
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


async def _sync_resource_to_ckan(
    context: RequestContext,
    resource_id: str,
    *,
    schema: dict[str, Any] | None = None,
    clear_schema: bool = False,
) -> None:
    """Keep the CKAN resource metadata in sync with the datastore table.

    Always refreshes `last_modified` — and, because `resource_patch` runs
    CKAN's `resource_update`, records an entry in the resource's activity
    log. The schema is touched per the operation:

    - `schema=<dict>` — an op that (re)defines columns (`datastore_create`,
      or a `datastore_delete` column drop): mirror it so CKAN matches the
      BigQuery table.
    - `clear_schema=True` — a whole-table drop: the table (and its schema)
      is gone, so drop the schema from the resource (`schema=null`).
    - neither (default) — a data-only op (upsert, row delete): leave the
      already-consistent schema untouched, timestamp + activity only.

    No-op under non-CKAN auth (`context.ckan is None`) or without a
    resource id — standalone deployments carry no CKAN resource record.
    """
    if context.ckan is None or not resource_id:
        return
    patch: dict[str, Any] = {"last_modified": _utc_now_iso()}
    if clear_schema:
        patch["schema"] = None
    elif schema is not None:
        patch["schema"] = schema
    await context.ckan.resource_patch(resource_id=resource_id, patch=patch)


async def create_datastore(
    context: RequestContext, data_dict: dict[str, Any]
) -> DatastoreCreateResponse.Result:
    package = data_dict.get("package") or {}
    resource = data_dict.get("resource") or {}
    schema = data_dict["schema"]
    records = data_dict.get("records") or []
    include_records = bool(data_dict.get("include_records", False))
    include_total = bool(data_dict.get("include_total", False))

    fields, primary_key = frictionless_schema_to_fields(schema)

    if isinstance(resource, dict):
        # Endpoint gates this branch on AUTH_TYPE=ckan, so context.ckan is
        # non-None here in practice; the assert keeps the type checker honest.
        assert context.ckan is not None, (
            "datastore_create `resource` dict path requires AUTH_TYPE=ckan"
        )
        # Tag the new resource as datastore-managed so CKAN (and our own
        # read-only guard on subsequent writes) knows the datastore owns
        # its data. Caller-supplied url_type is overridden on purpose.
        resource = await context.ckan.resource_create(
            resource={**resource, "url_type": "datastore"}
        )
        resource_id = resource["id"]
    else:
        resource_id = resource

    engine = get_datastore_engine(context, mode="rw")
    # Off the event loop — BigQuery's sync client would otherwise block
    # every other request on this worker for the duration of the call.
    write_result = await asyncio.to_thread(
        engine.create,
        resource_id=resource_id,
        schema=schema,
        records=records,
        include_total=include_total,
    )

    await _sync_resource_to_ckan(context, resource_id, schema=schema)

    return DatastoreCreateResponse.Result(
        resource_id=resource_id,
        package_id=package.get("id"),
        fields=fields,
        schema=schema,
        primary_key=primary_key,
        records=records if include_records else None,
        total=write_result.get("total") if include_total else None,
    )


async def upsert_datastore(
    context: RequestContext, data_dict: dict[str, Any]
) -> DatastoreUpsertResponse.Result:
    """Run the `datastore_upsert` action."""
    resource_id = data_dict["resource_id"]
    records = data_dict.get("records") or []
    method = data_dict.get("method") or "upsert"
    include_records = bool(data_dict.get("include_records", False))
    include_total = bool(data_dict.get("include_total", False))

    engine = get_datastore_engine(context, mode="rw")
    write_result = await asyncio.to_thread(
        engine.upsert,
        resource_id=resource_id,
        records=records,
        method=method,
        include_total=include_total,
    )


    await _sync_resource_to_ckan(context, resource_id)

    return DatastoreUpsertResponse.Result(
        resource_id=resource_id,
        method=method,
        records=records if include_records else None,
        total=write_result.get("total") if include_total else None,
    )


async def delete_datastore(
    context: RequestContext, data_dict: dict[str, Any]
) -> DatastoreDeleteResponse.Result:
    """Drop the table, delete rows, or drop columns. `filters` and
    `fields` are passed through verbatim — schema layer enforces
    mutual exclusivity."""
    resource_id = data_dict["resource_id"]
    filters = data_dict.get("filters")
    fields = data_dict.get("fields")

    engine = get_datastore_engine(context, mode="rw")
    result = await asyncio.to_thread(
        engine.delete, resource_id=resource_id, filters=filters, fields=fields,
    )

    # Sync CKAN per the delete variant (mirrors the engine's branching):
    #   column drop  → mirror the reduced schema
    #   whole-table drop (no filters, no fields) → drop the schema too
    #   row delete   → data only (timestamp + activity)
    if fields is not None:
        await _sync_resource_to_ckan(context, resource_id, schema=result.schema)
    elif filters is None:
        await _sync_resource_to_ckan(context, resource_id, clear_schema=True)
    else:
        await _sync_resource_to_ckan(context, resource_id)

    return DatastoreDeleteResponse.Result(
        resource_id=resource_id,
        filters=filters,
        fields=fields,
        # Populated only on the column-drop path: the table's schema
        # after the listed columns were removed.
        schema=result.schema,
    )
