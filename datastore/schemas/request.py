from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datastore.schemas.validators import (
    FieldSpec,
    StringOrList,
    to_json_object,
    to_str_or_json_object,
)

UpsertMethod = Literal["upsert", "insert", "update"]
RecordsFormat = Literal["objects", "lists", "csv", "tsv"]


class DatastoreCreateRequest(BaseModel):
    """Request body for `POST /api/3/datastore_create`.
    """

    model_config = ConfigDict(extra="forbid")

    resource_id: str | None = None
    resource: dict[str, Any] | None = None
    fields: list[FieldSpec] = Field(min_length=1)
    primary_key: StringOrList = None
    records: list[dict[str, Any]] | None = None
    include_records: bool = False
    include_total: bool = False
    force: bool | None = None

    @model_validator(mode="after")
    def _require_resource_id_or_resource(self) -> DatastoreCreateRequest:
        if self.resource_id is None and self.resource is None:
            raise ValueError("either 'resource_id' or 'resource' is required")
        if self.resource_id is not None and self.resource is not None:
            raise ValueError("provide either 'resource_id' or 'resource', not both")
        return self


class DatastoreUpsertRequest(BaseModel):
    """Request body for `POST /api/3/datastore_upsert`.
    """

    model_config = ConfigDict(extra="forbid")

    resource_id: str
    records: list[dict[str, Any]] | None = None
    method: UpsertMethod = "upsert"
    include_records: bool = False
    include_total: bool = False
    force: bool = False


class DatastoreSearchRequest(BaseModel):
    """Query parameters for `GET /api/3/datastore_search`.

    Fields are declared as *URL-side* types (all scalars) so FastAPI's
    `Annotated[Model, Query()]` can introspect them. The complex CKAN
    encodings live in their raw string form on this model:

    - `filters` — JSON-encoded object, e.g. ``{"col": value}``.
    - `q` — full-text query. Plain string scans every column; a value
      starting with ``{`` is a per-column ``{column: term}`` object.
    - `fields` — comma-separated column names.

    Parseability is checked at validation time (via `field_validator`s
    below); the *parsed* dict / list values are produced at the service
    boundary by re-running the helpers in `schemas.validators`.

    Bounds: ``limit ∈ [0, 32000]``, ``offset >= 0``.
    """

    model_config = ConfigDict(extra="forbid")

    resource_id: str
    filters: str | None = None
    q: str | None = None
    distinct: bool = False
    plain: bool = True
    language: str = "english"
    limit: int = Field(default=100, ge=0, le=32000)
    offset: int = Field(default=0, ge=0)
    fields: str | None = None
    sort: str | None = None
    include_total: bool = True
    records_format: RecordsFormat = "objects"

    @field_validator("filters")
    @classmethod
    def _check_filters(cls, v: str | None) -> str | None:
        if v:
            to_json_object(v)  # raises ValueError when not a JSON object
        return v

    @field_validator("q")
    @classmethod
    def _check_q(cls, v: str | None) -> str | None:
        if v:
            to_str_or_json_object(v)  # raises if it looks like JSON but isn't
        return v

