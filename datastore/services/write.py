from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.responses import (
    DatastoreCreateResponse,
    DatastoreDeleteResponse,
    DatastoreUpsertResponse,
)

if TYPE_CHECKING:  # type-only — no runtime import from api/
    from datastore.api.context import RequestContext


async def create_datastore(
    context: RequestContext, data_dict: dict[str, Any]
) -> DatastoreCreateResponse.Result:
    package = data_dict.get("package") or {}
    resource = data_dict.get("resource") or {}
    fields = data_dict.get("fields") or []
    records = data_dict.get("records") or []
    primary_key = data_dict.get("primary_key") or []
    include_records = bool(data_dict.get("include_records", False))
    include_total = bool(data_dict.get("include_total", False))

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
        fields=fields,
        unique_keys=primary_key,
        records=records,
        include_total=include_total,
    )

    return DatastoreCreateResponse.Result(
        resource_id=resource_id,
        package_id=package.get("id"),
        fields=fields,
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
    """Delete rows matching `filters`, or drop the whole table.
    """
    resource_id = data_dict["resource_id"]
    filters = data_dict.get("filters") or None

    engine = get_datastore_engine(context, mode="rw")
    engine.delete(resource_id=resource_id, filters=filters)

    return DatastoreDeleteResponse.Result(
        resource_id=resource_id,
        filters=filters,
    )
