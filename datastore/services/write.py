from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.responses import (
    DatastoreCreateResponse,
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

    is_new_resource = isinstance(resource, dict)
    if is_new_resource:
        resource = await context.ckan.resource_create(resource=resource)
        resource_id = resource["id"]
    else:
        resource_id = resource

    # TODO: placeholder engine call — replace once the real backend lands.
    engine = get_datastore_engine(context, mode="rw")
    engine.create(
        resource_id=resource_id,
        fields=fields,
        unique_keys=primary_key,
        records=records,
    )

    return DatastoreCreateResponse.Result(
        resource_id=resource_id,
        package_id=package.get("id"),
        fields=fields,
        primary_key=primary_key,
        records_inserted=len(records),
    )


async def upsert_datastore(
    context: RequestContext, data_dict: dict[str, Any]
) -> DatastoreUpsertResponse.Result:
    """Run the `datastore_upsert` action.
    """
    resource_id = data_dict["resource_id"]
    records = data_dict.get("records") or []
    method = data_dict.get("method") or "upsert"
    include_records = bool(data_dict.get("include_records", False))
    include_total = bool(data_dict.get("include_total", False))

    # TODO: placeholder engine call — replace once the real backend lands.
    engine = get_datastore_engine(context, mode="rw")
    engine.upsert(
        resource_id=resource_id,
        records=records,
        method=method,
        include_total=include_total,
    )

    return DatastoreUpsertResponse.Result(
        resource_id=resource_id,
        method=method,
        records_affected=len(records),
        records=records if include_records else None,
        record_count=None,
    )
