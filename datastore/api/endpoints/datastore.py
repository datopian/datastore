from __future__ import annotations

from fastapi import APIRouter, HTTPException
from starlette.requests import Request

from datastore.api.context import Context
from datastore.api.responses import ORJSONResponse, ckan_success
from datastore.schemas.datastore import DatastoreCreateRequest, DatastoreUpsertRequest
from datastore.schemas.responses import (
    DatastoreCreateResponse,
    DatastoreUpsertResponse,
)
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
    return ckan_success(request, result)


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
    return ckan_success(request, result)


@router.post("/datastore_delete")
def datastore_delete() -> ORJSONResponse:
    raise HTTPException(status_code=501, detail="datastore_delete is not implemented")


@router.get("/datastore_search")
def datastore_search() -> ORJSONResponse:
    raise HTTPException(status_code=501, detail="datastore_search is not implemented")


@router.get("/datastore_search_sql")
def datastore_search_sql() -> ORJSONResponse:
    raise HTTPException(status_code=501, detail="datastore_search_sql is not implemented")


@router.get("/datastore_info")
def datastore_info() -> ORJSONResponse:
    raise HTTPException(status_code=501, detail="datastore_info is not implemented")
