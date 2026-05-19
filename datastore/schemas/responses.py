"""Pydantic response models.

Declared on route decorators via `response_model=...` so OpenAPI
documents the actual contract, and used as the service-layer return
type so mypy catches drift between service and route.

CKAN's response shape is fixed: `{help, success, result: {...}}`.
`CKANResponse` carries `help` + `success`; each endpoint subclasses
it and adds an inner `Result` class plus a `result: Result` field.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

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
    """Response for `POST /api/3/datastore_create`.

    Returns both column shapes so clients on either side of the migration
    see the form they expect:
      - `fields` is the legacy `{id, type, info}` shape.
      - `schema` is the  Frictionless Table Schema (`{fields,
        primaryKey, ...}`).
    Both describe the same columns; they're derived from whichever the
    caller supplied. Legacy `fields` will be removed once callers move
    over to `schema`.
    """

    class Result(BaseModel):
        resource_id: str
        package_id: str | None = None
        fields: Annotated[
            list[FieldSpec],
            Field(deprecated="use 'schema' (Frictionless Table Schema) instead"),
        ]
        schema: dict[str, Any]
        primary_key: Annotated[
            list[str],
            Field(deprecated="use 'schema.primaryKey' (Frictionless Table Schema) instead"),
        ]
        # Echoed input rows when the request set `include_records=True`.
        records: list[dict[str, Any]] | None = None
        # Total row count after the write — set only when `include_total=True`.
        total: int | None = None

    result: Result


class DatastoreUpsertResponse(ResponseModel):
    """Response for `POST /api/3/datastore_upsert`."""

    class Result(BaseModel):
        resource_id: str
        method: str
        records: list[dict[str, Any]] | None = None
        total: int | None = None

    result: Result


class DatastoreDeleteResponse(ResponseModel):
    """Response for `POST /api/3/datastore_delete`."""

    class Result(BaseModel):
        resource_id: str
        filters: dict[str, Any] | None = None

    result: Result


class DatastoreSearchResponse(ResponseModel):
    """Response for `GET /api/3/datastore_search`."""

    class Result(BaseModel):
        # `_links` starts with an underscore, which pydantic treats as a
        # private attribute by default — alias it onto a regular field.
        model_config = ConfigDict(populate_by_name=True)

        resource_id: str
        fields: list[dict[str, Any]]
        records: list[dict[str, Any]]
        limit: int
        offset: int
        total: int | None = None
        links: dict[str, str] = Field(alias="_links", default_factory=dict)

    result: Result


class DatastoreInfoResponse(ResponseModel):
    """Response for `GET /api/3/datastore_info`.

    Returns column metadata in both shapes so clients on either side of
    the migration see what they expect:
      - `schema` is the canonical Frictionless Table Schema.
      - `fields` is the legacy `{id, type, info}` list (marked
        `deprecated`).
    `meta` is a free-form dict that engines populate with whatever extras
    they expose (row count, table size, last-modified, …) — piped
    through verbatim so adding a new key doesn't need a schema change.
    """

    class Result(BaseModel):
        meta: dict[str, Any]
        schema: dict[str, Any]
        fields: Annotated[
            list[dict[str, Any]],
            Field(deprecated="use 'schema' (Frictionless Table Schema) instead"),
        ]

    result: Result
