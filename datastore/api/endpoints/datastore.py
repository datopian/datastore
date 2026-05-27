from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from starlette.requests import Request
from starlette.responses import StreamingResponse

from datastore.api.context import Context
from datastore.api.responses import (
    ERROR_RESPONSES,
    _deprecation_warnings,
    _success_response,
)
from datastore.core.exceptions import ValidationError
from datastore.schemas.request import (
    DatastoreCreateRequest,
    DatastoreDeleteRequest,
    DatastoreInfoRequest,
    DatastoreSearchRequest,
    DatastoreSearchSQLRequest,
    DatastoreUpsertRequest,
)
from datastore.schemas.responses import (
    DatastoreCreateResponse,
    DatastoreDeleteResponse,
    DatastoreInfoResponse,
    DatastoreSearchResponse,
    DatastoreUpsertResponse,
)
from datastore.services.read import (
    info_datastore,
    search_datastore,
    search_sql_datastore,
)
from datastore.services.write import (
    create_datastore,
    delete_datastore,
    upsert_datastore,
)

router = APIRouter(tags=["Datastore"], responses=ERROR_RESPONSES)


@router.post(
    "/datastore_create",
    response_model=DatastoreCreateResponse,
    summary="Create a datastore table and optionally insert rows",
)
async def datastore_create(
    request: Request,
    payload: DatastoreCreateRequest,
    context: Context,
):
    """`POST /api/3/datastore_create` — authorize, then run the create flow."""

    if payload.resource is not None and context.config.AUTH_TYPE != "ckan":
        raise ValidationError(
            "`resource` dict is only supported for ckan auth; for other auth types,"
            "use `resource_id` instead"
        )

    if payload.resource_id:
        data_dict = await context.authorize(
            resource_id=payload.resource_id,
            permission="create",
        )
    else:
        data_dict = await context.authorize(
            package_id=payload.resource.get("package_id"),
            permission="create",
        )

    data_dict.update(
        {
            "resource": payload.resource_id or payload.resource,
            "schema": payload.schema,
            "records": payload.records,
            "include_records": payload.include_records,
            "include_total": payload.include_total,
        }
    )

    result = await create_datastore(context, data_dict)
    warnings = _deprecation_warnings(payload)

    return _success_response(request, result, warnings=warnings or None)


@router.post(
    "/datastore_upsert",
    response_model=DatastoreUpsertResponse,
    summary="Insert / update / upsert records in a datastore table",
)
async def datastore_upsert(
    request: Request,
    payload: DatastoreUpsertRequest,
    context: Context,
):
    """`POST /api/3/datastore_upsert` — authorize, then upsert / insert / update rows."""
    data_dict = await context.authorize(
        resource_id=payload.resource_id,
        permission="update",
    )
    data_dict.update(payload.model_dump())
    result = await upsert_datastore(context, data_dict)
    return _success_response(request, result)


@router.post(
    "/datastore_delete",
    response_model=DatastoreDeleteResponse,
    summary="Delete rows, drop columns, or drop the datastore table",
)
async def datastore_delete(
    request: Request,
    payload: DatastoreDeleteRequest,
    context: Context,
):
    """`POST /api/3/datastore_delete` — delete rows or drop the table.

    Body:
      `resource_id` / `id` (one required) — table to delete from.
      `filters` (optional dict) — only rows matching every key/value
         pair are deleted. Omit → whole table is dropped.
      `force` (optional bool) — required to delete from a CKAN
         read-only resource.

    Returns the original `filters` echoed back (CKAN convention) so the
    caller can confirm what the server actually applied.
    """
    await context.authorize(resource_id=payload.resource_id, permission="delete")
    result = await delete_datastore(context, payload.model_dump())
    return _success_response(request, result)


@router.get(
    "/datastore_search",
    response_model=DatastoreSearchResponse,
    summary="Search a datastore table (filters, full-text, sort, paging)",
)
async def datastore_search(
    request: Request,
    context: Context,
    params: Annotated[DatastoreSearchRequest, Query()],
):
    """`GET /api/3/datastore_search` — authorize, then stream rows.

    All of the search business logic — engine call, pagination link
    building, format dispatch — lives in `services.read.search_datastore`.
    Every format emits the same JSON envelope, so this endpoint just
    authorizes, assembles `data_dict`, and wraps the service's body
    iterator in a `StreamingResponse` with a fixed `application/json`
    media type.
    """
    data_dict = await context.authorize(
        resource_id=params.resource_id,
        permission="read",
    )
    data_dict.update(params.model_dump())
    body_iter = await search_datastore(context, data_dict, request_url=str(request.url))
    return StreamingResponse(body_iter, media_type="application/json")


@router.get(
    "/datastore_search_sql",
    response_model=DatastoreSearchResponse,
    summary="Query datastore tables with a read-only SQL SELECT",
)
async def datastore_search_sql(
    request: Request,
    context: Context,
    params: Annotated[DatastoreSearchSQLRequest, Query()],
):
    """`GET /api/3/datastore_search_sql` — execute a raw SQL SELECT and stream.
    Accepts a single `sql` query parameter;
    """
    for resource_id in params.resource_ids:
        await context.authorize(resource_id=resource_id, permission="read")

    data_dict = params.model_dump() | {
        "function_names": params.function_names,
        "limit": params.limit,
        "offset": params.offset,
    }

    body_iter = await search_sql_datastore(context, data_dict, request_url=str(request.url))
    return StreamingResponse(body_iter, media_type="application/json")


@router.get(
    "/datastore_info",
    response_model=DatastoreInfoResponse,
    summary="Get a resource's schema and metadata",
)
async def datastore_info(
    request: Request,
    context: Context,
    params: Annotated[DatastoreInfoRequest, Query()],
):
    """`GET /api/3/datastore_info` — return table metadata.

    Authorizes the caller on `resource_id` (same gate as `datastore_search`),
    then asks the read-only engine for its `InfoResult`. The response is
    small enough to skip streaming; we go through the standard
    `_success_response` envelope.

    Body shape:
        result.fields  — column schema, list of {"id", "type", ...}
        result.meta    — free-form dict (engine-specific extras)
    """
    await context.authorize(resource_id=params.resource_id, permission="read")
    result = await info_datastore(context, params.model_dump())
    return _success_response(request, result)

