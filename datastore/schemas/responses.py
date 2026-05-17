"""Pydantic response models.

Declared on route decorators via `response_model=...` so OpenAPI
documents the actual contract, and used as the service-layer return
type so mypy catches drift between service and route.

CKAN's response shape is fixed: `{help, success, result: {...}}`.
`CKANResponse` carries `help` + `success`; each endpoint subclasses
it and adds an inner `Result` class plus a `result: Result` field.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from datastore.schemas.validators import FieldSpec


class ResponseModel(BaseModel):
    """Base envelope: `help` + `success`. Subclass and add `result: <Inner>`."""

    help: str
    success: bool = True


# --- health -----------------------------------------------------------------


class WelcomeResponse(ResponseModel):
    """Response for `GET /`."""

    class Result(BaseModel):
        message: str

    result: Result


class StatusResponse(ResponseModel):
    """Response for `GET /health` and `GET /ready`."""

    class Result(BaseModel):
        status: str

    result: Result


# --- datastore --------------------------------------------------------------

class DatastoreCreateResponse(ResponseModel):
    """Response for `POST /api/3/datastore_create`."""

    class Result(BaseModel):
        resource_id: str
        package_id: str | None = None
        fields: list[FieldSpec]
        primary_key: list[str] = Field(default_factory=list)
        records_inserted: int

    result: Result
