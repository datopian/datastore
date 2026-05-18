from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)

from datastore.schemas.validators import (
    FieldSpec,
    StringOrList,
    parse_sql_references,
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


# Strip leading SQL comments (line `-- ...` and block `/* ... */`) before
# checking the first keyword. The check is coarse — real safety comes from
# the engine running under read-only credentials (e.g. BigQuery IAM).
_SQL_COMMENT_RE = re.compile(r"^\s*(--[^\n]*\n|/\*.*?\*/)+", re.DOTALL)
_SQL_LEAD_RE = re.compile(r"^(select|with)\b", re.IGNORECASE)


class DatastoreSearchSQLRequest(BaseModel):
    """Query parameters for `GET /api/3/datastore_search_sql`.

    The caller supplies a raw SQL string and only that. Pagination /
    row-limit are the caller's responsibility (put `LIMIT` / `OFFSET` /
    a `WHERE` cursor in the SQL itself); the response shape matches
    `datastore_search` so existing clients can reuse the same parser.

    Validation also extracts table names and function names from the SQL
    (via sqlglot) and exposes them as `resource_ids` / `function_names`
    so the endpoint can authorize per-resource and gate functions against
    the allow-list — both without re-parsing. Table names in CKAN's
    datastore map 1:1 to resource_ids, hence the field name.
    """

    model_config = ConfigDict(extra="forbid")

    sql: str

    # Set by `_extract_sql_references` after sql validates. Private so
    # they're not user-settable from the URL and don't show in OpenAPI;
    # the read-only properties below give callers a clean attribute.
    _resource_ids: list[str] = PrivateAttr(default_factory=list)
    _function_names: list[str] = PrivateAttr(default_factory=list)

    @property
    def resource_ids(self) -> list[str]:
        """Table names referenced by the SQL — each authorized as a
        CKAN resource_id."""
        return self._resource_ids

    @property
    def function_names(self) -> list[str]:
        """SQL function calls in the query, lowercased — checked
        against `core.constants.ALLOWED_SQL_FUNCTIONS`."""
        return self._function_names

    @field_validator("sql")
    @classmethod
    def _check_sql_is_select(cls, v: str) -> str:
        """Reject anything that isn't a single SELECT / WITH statement
        AND fails to parse as valid SQL.

        Two checks, both client-side fail-fast:
          - regex SELECT / WITH lead + no semicolons (cheap, friendly errors)
          - sqlglot parse (catches malformed SQL — `SELECT FROM`, stray
            tokens, etc.). `_extract_sql_references` re-parses to extract
            tables + functions; the doubled parse cost is negligible
            (microseconds) and keeps the error attached to the `sql`
            field instead of `(root)`.

        Real safety still lives at the engine layer — `mode="ro"` selects
        credentials with only SELECT privileges, so even if this check is
        bypassed the database refuses the write.
        """
        stripped = v.strip()
        if not stripped:
            raise ValueError("sql must not be empty")

        head = _SQL_COMMENT_RE.sub("", stripped).strip()
        if not _SQL_LEAD_RE.match(head):
            raise ValueError(
                "only SELECT / WITH statements are allowed"
            )

        cleaned = stripped.rstrip(";").rstrip()
        if ";" in cleaned:
            raise ValueError(
                "multiple statements are not allowed "
                "(strip all `;` except an optional trailing one)"
            )
        parse_sql_references(v)
        return v

    @model_validator(mode="after")
    def _extract_sql_references(self) -> DatastoreSearchSQLRequest:
        """Parse `sql` via sqlglot and stash table + function names.

        Runs after `_check_sql_is_select`, so we know we have a single
        SELECT / WITH. CTE aliases are excluded from `_resource_ids`
        (they're defined inline, not external tables).
        """
        self._resource_ids, self._function_names = parse_sql_references(self.sql)
        return self


class DatastoreInfoRequest(BaseModel):
    """Query parameters for `GET /api/3/datastore_info`.

    Accepts either `resource_id` or `id` — they're aliases for the same
    thing (CKAN's `id` is historical; `resource_id` is what the rest of
    this API uses). Exactly one must be provided. The model_validator
    normalises `id` → `resource_id` so downstream code only reads one
    field. `extra="forbid"` so unknown params surface as 400s.
    """

    model_config = ConfigDict(extra="forbid")

    resource_id: str | None = None
    id: str | None = None

    @model_validator(mode="after")
    def _require_resource_id_or_id(self) -> DatastoreInfoRequest:
        if self.resource_id is None and self.id is None:
            raise ValueError("either 'resource_id' or 'id' is required")
        if self.resource_id is None:
            self.resource_id = self.id
        return self


class DatastoreDeleteRequest(BaseModel):
    """Request body for `POST /api/3/datastore_delete`.

    Deletes rows matching `filters`, or drops the whole table when
    `filters` is omitted. Accepts either `resource_id` or `id` (same
    aliasing as `datastore_info`); model_validator normalises `id` →
    `resource_id`.

    `force=True` is required to delete from a CKAN resource marked
    read-only — the engine layer enforces this; the schema just carries
    the flag.
    """

    model_config = ConfigDict(extra="forbid")

    resource_id: str | None = None
    id: str | None = None
    filters: dict[str, Any] | None = None
    force: bool = False

    @model_validator(mode="after")
    def _require_resource_id_or_id(self) -> DatastoreDeleteRequest:
        if self.resource_id is None and self.id is None:
            raise ValueError("either 'resource_id' or 'id' is required")
        if self.resource_id is None:
            self.resource_id = self.id
        return self
