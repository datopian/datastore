"""HTTP routes for the datastore API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, Request, Response
from fastapi.responses import StreamingResponse

from datastore.api.responses import json_response, streaming_json, success_envelope
from datastore.config import Settings, get_settings

router = APIRouter()
datastore_router = APIRouter()


@router.get("/")
def welcome(settings: Settings = Depends(get_settings)) -> dict[str, str]:
    return {"message": settings.APP_MESSAGE}


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}


@datastore_router.post("/datastore_create")
def datastore_create(request: Request, payload: dict[str, Any] = Body(...)) -> Response:
    return json_response(
        success_envelope(
            request,
            {
                "resource_id": payload.get("resource_id"),
                "fields": payload.get("fields", []),
                "stub": True,
            },
        )
    )


@datastore_router.get("/datastore_search")
def datastore_search(
    request: Request,
    resource_id: str | None = None,
    q: str | None = None,
) -> StreamingResponse:
    return streaming_json(
        success_envelope(
            request,
            {
                "resource_id": resource_id,
                "fields": [],
                "records": [],
                "stub": True,
            },
        )
    )


@datastore_router.post("/datastore_upsert")
def datastore_upsert(request: Request, payload: dict[str, Any] = Body(...)) -> Response:
    return json_response(
        success_envelope(
            request,
            {
                "resource_id": payload.get("resource_id"),
                "rows_written": 0,
                "stub": True,
            },
        )
    )


@datastore_router.get("/datastore_search_sql")
def datastore_search_sql(
    request: Request,
    sql: str | None = None,
) -> StreamingResponse:
    return streaming_json(
        success_envelope(
            request,
            {
                "fields": [],
                "records": [],
                "stub": True,
            },
        )
    )


@datastore_router.post("/datastore_delete")
def datastore_delete(request: Request, payload: dict[str, Any] = Body(...)) -> Response:
    return json_response(
        success_envelope(
            request,
            {
                "resource_id": payload.get("resource_id"),
                "stub": True,
            },
        )
    )


@datastore_router.get("/datastore_info")
def datastore_info(
    request: Request,
    resource_id: str | None = None,
) -> Response:
    return json_response(
        success_envelope(
            request,
            {
                "resource_id": resource_id,
                "fields": [],
                "stub": True,
            },
        )
    )
