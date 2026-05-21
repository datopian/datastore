from __future__ import annotations

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

    is_new_resource = isinstance(resource, dict)
    if is_new_resource:
        resource = await context.ckan.resource_create(resource=resource)
        resource_id = resource["id"]
    else:
        resource_id = resource

    # TODO: placeholder engine call — replace once the real backend lands.
    engine = get_datastore_engine(context, mode="rw")
    write_result = engine.create(
        resource_id=resource_id,
        schema=schema,
        records=records,
        include_total=include_total,
    )

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

    # TODO: placeholder engine call — replace once the real backend lands.
    engine = get_datastore_engine(context, mode="rw")
    write_result = engine.upsert(
        resource_id=resource_id,
        records=records,
        method=method,
        include_total=include_total,
    )

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
    engine.delete(resource_id=resource_id, filters=filters, fields=fields)

    return DatastoreDeleteResponse.Result(
        resource_id=resource_id,
        filters=filters,
        fields=fields,
    )
