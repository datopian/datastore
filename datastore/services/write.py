from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.responses import DatastoreCreateResponse

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
