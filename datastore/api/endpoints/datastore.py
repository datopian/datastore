from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from starlette.requests import Request
from starlette.responses import StreamingResponse

from datastore.api.context import Context
from datastore.api.responses import ORJSONResponse, _success_response
from datastore.schemas.request import (
    DatastoreCreateRequest,
    DatastoreSearchRequest,
    DatastoreUpsertRequest,
)
from datastore.schemas.responses import (
    DatastoreCreateResponse,
    DatastoreSearchResponse,
    DatastoreUpsertResponse,
)
from datastore.services.read import search_datastore
from datastore.services.write import create_datastore, upsert_datastore

router = APIRouter(tags=["datastore"])


@router.post("/datastore_create", response_model=DatastoreCreateResponse)
async def datastore_create(
    request: Request,
    payload: DatastoreCreateRequest,
    context: Context,
):
    """`POST /api/3/datastore_create` — authorize, then run the create flow."""

    if payload.resource_id:
        data_dict = await context.auth.authorize(
            resource_id=payload.resource_id,
            permission="create",
        )
    else:
        data_dict = await context.auth.authorize(
            package_id=payload.resource.get("package_id"),
            permission="create",
        )
        
    data_dict.update(
        {
            "resource": payload.resource_id or payload.resource,
            "fields": payload.fields,
            "records": payload.records,
            "primary_key": payload.primary_key,
            "include_records": payload.include_records,
            "include_total": payload.include_total,
        }
    )

    result = await create_datastore(context, data_dict)
    return _success_response(request, result)


@router.post("/datastore_upsert", response_model=DatastoreUpsertResponse)
async def datastore_upsert(
    request: Request,
    payload: DatastoreUpsertRequest,
    context: Context,
):
    """`POST /api/3/datastore_upsert` — authorize, then upsert / insert / update rows."""
    data_dict = await context.auth.authorize(
        resource_id=payload.resource_id,
        permission="update",
    )
    data_dict.update(payload.model_dump())
    result = await upsert_datastore(context, data_dict)
    return _success_response(request, result)


@router.post("/datastore_delete")
def datastore_delete() -> ORJSONResponse:
    raise HTTPException(status_code=501, detail="datastore_delete is not implemented")


@router.get("/datastore_search", response_model=DatastoreSearchResponse)
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
    data_dict = await context.auth.authorize(
        resource_id=params.resource_id,
        permission="read",
    )
    data_dict.update(params.model_dump())
    body_iter = await search_datastore(
        context, data_dict, request_url=str(request.url)
    )
    return StreamingResponse(body_iter, media_type="application/json")


@router.get("/datastore_search_sql")
def datastore_search_sql() -> ORJSONResponse:
    raise HTTPException(status_code=501, detail="datastore_search_sql is not implemented")


@router.get("/datastore_info")
def datastore_info() -> ORJSONResponse:
    raise HTTPException(status_code=501, detail="datastore_info is not implemented")
