from __future__ import annotations

import re
from typing import Annotated, Any, Literal

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
    fields_to_frictionless_schema,
    parse_sql_pagination,
    parse_sql_references,
    to_json_object,
    to_str_or_json_object,
    validate_frictionless_schema,
)

UpsertMethod = Literal["upsert", "insert", "update"]
RecordsFormat = Literal["objects", "lists", "csv", "tsv"]


class DatastoreCreateRequest(BaseModel):
    """Request body for `POST /api/3/datastore_create`.

    Column definitions: provide either the legacy `fields` shape (a list of
    `FieldSpec` objects) **or** a Frictionless Table Schema via `schema`,
    never both. The Frictionless form is the native shape; `fields` is kept
    as a back-compat input and will be deprecated.

    When `schema` is supplied, `primary_key` must not be — the schema's
    `primaryKey` is the single source of truth for the unique key.
    """

    model_config = ConfigDict(extra="forbid")

    resource_id: str | None = None
    resource: dict[str, Any] | None = None
    # `deprecated=` must ride on `Annotated` metadata, not as a `Field()`
    # default — Pydantic silently drops it on union- / Annotated-aliased
    # fields when supplied via `Field(default=..., deprecated=...)`.
    fields: Annotated[
        list[FieldSpec] | None,
        Field(deprecated="use 'schema' (Frictionless Table Schema) instead"),
    ] = None
    schema: dict[str, Any] | None = None
    primary_key: Annotated[
        StringOrList,
        Field(deprecated="use 'schema.primaryKey' instead"),
    ] = None
    records: list[dict[str, Any]] | None = None
    include_records: bool = False
    include_total: bool = False
    force: bool | None = None

    _check_schema = field_validator("schema")(validate_frictionless_schema)

    @model_validator(mode="after")
    def _require_resource_id_or_resource(self) -> DatastoreCreateRequest:
        if self.resource_id is None and self.resource is None:
            raise ValueError("either 'resource_id' or 'resource' is required")
        if self.resource_id is not None and self.resource is not None:
            raise ValueError("provide either 'resource_id' or 'resource', not both")
        return self

    @model_validator(mode="after")
    def _require_fields_or_schema(self) -> DatastoreCreateRequest:
        # Read deprecated fields via __dict__ so we don't trip our own
        # `Field(deprecated=...)` DeprecationWarning during validation.
        fields_val = self.__dict__.get("fields")
        primary_key_val = self.__dict__.get("primary_key")
        has_fields = fields_val is not None
        has_schema = self.schema is not None
        if has_fields and has_schema:
            raise ValueError(
                "provide either 'fields' (legacy) or 'schema' (frictionless), not both"
            )
        if not has_fields and not has_schema:
            raise ValueError("either 'fields' or 'schema' is required")
        if has_fields and len(fields_val or []) == 0:
            raise ValueError("'fields' must not be empty")
        if has_schema and primary_key_val:
            raise ValueError(
                "'primary_key' is not allowed with 'schema'; use the schema's 'primaryKey' instead"
            )
        return self

    @model_validator(mode="after")
    def _build_canonical_schema(self) -> DatastoreCreateRequest:
        """Fold legacy `fields` + `primary_key` into the canonical `schema`.

        After this validator `self.schema` is always populated, so the
        endpoint / service only ever read the Frictionless shape — the
        legacy inputs exist purely as a boundary back-compat surface.
        Runs after `_require_fields_or_schema`, so we know exactly one
        of {fields, schema} is set.
        """
        if self.schema is not None:
            return self
        fields_val = self.__dict__.get("fields") or []
        primary_key_val = self.__dict__.get("primary_key") or []
        self.__dict__["schema"] = fields_to_frictionless_schema(fields_val, primary_key_val)
        return self


class DatastoreUpsertRequest(BaseModel):
    """Request body for `POST /api/3/datastore_upsert`."""

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
    # Engine enforces `Config.SEARCH_RESULT_ROWS_MAX` (default 32000).
    # No `le` here so ops can lift the cap via env without a schema change.
    limit: int = Field(default=100, ge=0)
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
    _limit: int = PrivateAttr(default=0)
    _offset: int = PrivateAttr(default=0)

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

    @property
    def limit(self) -> int:
        """`LIMIT` literal parsed from the SQL — required."""
        return self._limit

    @property
    def offset(self) -> int:
        """`OFFSET` literal parsed from the SQL (0 when absent)."""
        return self._offset

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
            raise ValueError("only SELECT / WITH statements are allowed")

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
        """Parse `sql` via sqlglot and stash table + function names +
        the LIMIT/OFFSET literals.

        Runs after `_check_sql_is_select`, so we know we have a single
        SELECT / WITH. CTE aliases are excluded from `_resource_ids`
        (they're defined inline, not external tables). LIMIT is
        required — the service uses it to build pagination links and
        to cap the streaming response; missing LIMIT raises a clean
        ValidationError up front.
        """
        self._resource_ids, self._function_names = parse_sql_references(self.sql)
        self._limit, self._offset = parse_sql_pagination(self.sql)
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
        if (
            self.resource_id is not None
            and self.id is not None
            and self.resource_id != self.id
        ):
            raise ValueError(
                "'resource_id' and 'id' both provided with different "
                "values; send exactly one"
            )
        if self.resource_id is None:
            self.resource_id = self.id
        return self


class DatastoreDeleteRequest(BaseModel):
    """Request body for `POST /api/3/datastore_delete`. Drops the
    whole table when both `filters` and `fields` are omitted; row
    delete when `filters` is set; column drop when `fields` is set.
    `filters` and `fields` are mutually exclusive."""

    model_config = ConfigDict(extra="forbid")

    resource_id: str | None = None
    id: str | None = None
    filters: dict[str, Any] | None = None
    fields: list[str] | None = None
    force: bool = False

    @model_validator(mode="after")
    def _require_resource_id_or_id(self) -> DatastoreDeleteRequest:
        if self.resource_id is None and self.id is None:
            raise ValueError("either 'resource_id' or 'id' is required")
        if (
            self.resource_id is not None
            and self.id is not None
            and self.resource_id != self.id
        ):
            raise ValueError(
                "'resource_id' and 'id' both provided with different "
                "values; send exactly one"
            )
        if self.resource_id is None:
            self.resource_id = self.id
        if self.filters is not None and self.fields:
            raise ValueError(
                "'filters' and 'fields' are mutually exclusive — "
                "rows and columns are separate delete operations"
            )
        if self.fields is not None and not self.fields:
            raise ValueError("'fields' must list at least one column")
        return self
