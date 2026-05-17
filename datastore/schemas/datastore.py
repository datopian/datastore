from __future__ import annotations

from typing import Any

from datastore.schemas.validators import FieldSpec, StringOrList
from pydantic import BaseModel, ConfigDict, Field, model_validator


class DatastoreCreateRequest(BaseModel):
    """Request body for `POST /api/3/datastore_create`.
    """

    model_config = ConfigDict(extra="forbid")

    resource_id: str | None = None
    resource: dict[str, Any] | None = None
    fields: list[FieldSpec] = Field(min_length=1)
    primary_key: StringOrList = None
    records: list[dict[str, Any]] | None = None
    force: bool | None = None 

    @model_validator(mode="after")
    def _require_resource_id_or_resource(self) -> DatastoreCreateRequest:
        if self.resource_id is None and self.resource is None:
            raise ValueError("either 'resource_id' or 'resource' is required")
        if self.resource_id is not None and self.resource is not None:
            raise ValueError("provide either 'resource_id' or 'resource', not both")
        return self
