from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from datastore.schemas.validators import FieldSpec, StringOrList

UpsertMethod = Literal["upsert", "insert", "update"]


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

