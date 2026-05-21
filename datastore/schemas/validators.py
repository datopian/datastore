"""Reusable Pydantic validators and schema parts."""

from __future__ import annotations

import json
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, field_validator

from datastore.core.constants import (
    FRICTIONLESS_TO_POSTGRES,
    POSTGRES_TO_FRICTIONLESS,
    POSTGRES_TYPES,
)

# --- validator functions -----------------------------------------------------


def to_list(value: Any) -> list[str] | None:
    """Coerce `None | str | list[str]` to `list[str] | None`.
    Equivalent to CKAN's `list_of_strings_or_string`.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return list(value)
    raise ValueError("must be a string or list of strings")


def check_postgres_type(value: Any) -> str | None:
    """Pass through `None`; otherwise resolve to a canonical PostgreSQL type.

    Raises ValueError when `value` isn't a string or isn't a recognised
    Postgres type / alias in `POSTGRES_TYPES`.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("type must be a string")
    key = " ".join(value.strip().lower().split())
    canonical = POSTGRES_TYPES.get(key)
    if canonical is None:
        canonicals = sorted(set(POSTGRES_TYPES.values()))
        raise ValueError(
            f"unknown field type '{value}'; expected one of {canonicals} or a PostgreSQL alias"
        )
    return canonical


def to_json_object(value: Any) -> dict[str, Any] | None:
    """Decode a query-string JSON object. Pass-through dicts; reject the rest."""
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise ValueError("must be a JSON object")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("must be a JSON object")
    return parsed


def to_str_or_json_object(value: Any) -> str | dict[str, Any] | None:
    """`q`-style param: plain string, or JSON object when the value starts with `{`."""
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        if value.lstrip().startswith("{"):
            return to_json_object(value)
        return value
    raise ValueError("must be a string or JSON object")


def to_csv_list(value: Any) -> list[str] | None:
    """Coerce a comma-separated string to a list; pass-through lists."""
    if value is None or value == "":
        return None
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        parts = [token.strip() for token in value.split(",")]
        return [p for p in parts if p]
    raise ValueError("must be a comma-separated string or list of strings")


def fields_to_frictionless_schema(
    fields: list[Any], primary_key: list[str] | None = None
) -> dict[str, Any]:
    """Convert the legacy `fields` + `primary_key` shape into a Frictionless
    Table Schema descriptor.

    Each `FieldSpec` becomes a Frictionless field:
      - `id`   → `name`
      - `type` (Postgres canonical) → `type` (Frictionless), via
        `POSTGRES_TO_FRICTIONLESS`. Unknown types fall through to `string`.
      - `info` is unpacked: recognised keys (`title`, `description`) move
        to top-level Frictionless properties; the rest stays nested
        under a custom `info` key so `frictionless_schema_to_fields`
        can round-trip the data dictionary intact (previously these
        extras were spread onto the field and silently lost on the
        reverse path).

    `primary_key` becomes the schema's `primaryKey` (Frictionless naming).
    """
    fr_fields: list[dict[str, Any]] = []
    for f in fields:
        spec = f.model_dump(exclude_none=True) if hasattr(f, "model_dump") else dict(f)
        fr: dict[str, Any] = {"name": spec["id"]}
        pg_type = spec.get("type")
        if pg_type:
            fr["type"] = POSTGRES_TO_FRICTIONLESS.get(pg_type, "string")
        info = spec.get("info") or {}
        extra: dict[str, Any] = {}
        for k, v in info.items():
            # `info.type` is treated as a hint and dropped — the outer
            # canonical type already lives on `fr["type"]`, and letting an
            # info-side `type` ride along would either shadow or conflict
            # with it after the merge below.
            if k == "type":
                continue
            if k in ("title", "description") and isinstance(v, str):
                fr[k] = v
            else:
                extra[k] = v
        if extra:
            fr = {**fr, **extra}
        fr_fields.append(fr)

    schema: dict[str, Any] = {"fields": fr_fields}
    if primary_key:
        schema["primaryKey"] = list(primary_key)
    return schema


def frictionless_schema_to_fields(
    schema: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Inverse of `fields_to_frictionless_schema`.

    Returns `(fields, primary_key)` where `fields` matches the legacy
    `{id, type, info}` shape. Frictionless `name` → `id`; the field's
    Frictionless type is mapped back to Postgres via
    `FRICTIONLESS_TO_POSTGRES` (defaults to `text`). `title` /
    `description` on the field move into `info`; any extras saved
    under `info` are merged back in.

    `primaryKey` may be a string or list of strings in Frictionless;
    normalised to `list[str]`.
    """
    fields_out: list[dict[str, Any]] = []
    for fr in schema.get("fields", []):
        name = fr.get("name")
        if not name:
            continue
        out: dict[str, Any] = {"id": name}
        fr_type = fr.get("type")
        if fr_type:
            out["type"] = FRICTIONLESS_TO_POSTGRES.get(fr_type, "text")
        info: dict[str, Any] = {}
        for k in ("title", "description"):
            v = fr.get(k)
            if isinstance(v, str):
                info[k] = v
        extra = fr.get("info")
        if isinstance(extra, dict):
            info.update(extra)
        if info:
            out["info"] = info
        fields_out.append(out)

    pk = schema.get("primaryKey")
    if isinstance(pk, str):
        primary_key = [pk]
    elif isinstance(pk, list):
        primary_key = [str(x) for x in pk]
    else:
        primary_key = []
    return fields_out, primary_key


def validate_frictionless_schema(value: Any) -> dict[str, Any] | None:
    """Validate a Frictionless Table Schema descriptor against this
    repo's stricter contract.

    Pass-through `None`. Otherwise:
      1. `frictionless.Schema.from_descriptor` validates the descriptor
         shape (raises on missing `fields`, unknown field type, etc.).
      2. Field types must be in `ALLOWED_FRICTIONLESS_TYPES` — wider
         Frictionless vocabulary (e.g. `duration`, `year`, `yearmonth`)
         is rejected here so storage layout stays predictable and the
         engine type maps don't grow ad-hoc.
      3. Field names must not collide with engine-reserved system
         columns (`_id`, `_updated_at`). Silently dropping them would
         leave the response advertising a column the engine won't
         populate.

    Any failure raises `ValueError` so Pydantic surfaces it through
    the standard CKAN error envelope.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("schema must be a JSON object")

    from frictionless import Schema
    from frictionless.exception import FrictionlessException

    from datastore.core.constants import (
        ALLOWED_FRICTIONLESS_TYPES,
        RESERVED_SYSTEM_COLUMN_NAMES,
    )

    try:
        Schema.from_descriptor(value)
    except FrictionlessException as exc:
        raise ValueError(str(exc)) from exc

    for f in value.get("fields", []) or []:
        name = f.get("name")
        if name in RESERVED_SYSTEM_COLUMN_NAMES:
            raise ValueError(
                f"field name {name!r} is reserved for engine-managed "
                "system columns; rename the field"
            )
        ftype = f.get("type")
        if ftype is not None and ftype not in ALLOWED_FRICTIONLESS_TYPES:
            raise ValueError(
                f"field {name!r} has unsupported type {ftype!r}; "
                f"allowed: {sorted(ALLOWED_FRICTIONLESS_TYPES)}"
            )
    return value


def parse_sql_references(sql: str, *, dialect: str = "postgres") -> tuple[list[str], list[str]]:
    """Parse `sql` and return (table_names, function_names).

    Used by `datastore_search_sql` to:
      - authorize every referenced table (each table name maps to a CKAN
        resource_id),
      - check every called function against the allow-list in
        `core.constants.ALLOWED_SQL_FUNCTIONS`.

    Names are deduplicated, lower-cased, and sorted. Function names are
    taken from the dialect-rendered form so that, e.g., `DATE_TRUNC` stays
    `date_trunc` (sqlglot's internal AST key would normalise it to
    `timestamptrunc`, which wouldn't match a human-readable allow-list).
    `CASE WHEN` / `CAST` / similar syntactic constructs are filtered out:
    they parse as `exp.Func` subclasses but their rendered head contains
    whitespace, not a function identifier.

    Raises `ValueError` if sqlglot can't parse the SQL — the schema's
    SELECT-only regex check runs first, so we should only reach here with
    a parseable statement.
    """
    import sqlglot
    from sqlglot import expressions as exp

    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as e:
        raise ValueError(f"could not parse SQL: {e}") from e

    # CTE aliases (e.g. `WITH t AS (...) SELECT * FROM t`) parse as
    # `exp.Table` nodes even though they're defined inline — exclude them
    # so auth isn't called for non-external table refs.
    cte_aliases = {cte.alias_or_name for cte in tree.find_all(exp.CTE) if cte.alias_or_name}
    tables = {t.name for t in tree.find_all(exp.Table) if t.name and t.name not in cte_aliases}

    functions: set[str] = set()
    for f in tree.find_all(exp.Func):
        if isinstance(f, exp.Anonymous):
            if f.name:
                functions.add(f.name.lower())
            continue
        head = f.sql(dialect=dialect).split("(", 1)[0].strip()
        # Skip syntactic constructs (`CASE WHEN ... END`, etc.) — they
        # parse as Func subclasses but the head isn't a function name.
        if not head or " " in head:
            continue
        functions.add(head.lower())

    return sorted(tables), sorted(functions)


# --- reusable Annotated types ------------------------------------------------
# The parser functions above (`to_json_object`, `to_str_or_json_object`,
# `to_csv_list`) are invoked directly at the service boundary; they don't
# need an Annotated wrapper because FastAPI's `Annotated[Model, Query()]`
# only accepts scalar fields on the model — see DatastoreSearchRequest.
StringOrList = Annotated[list[str] | None, BeforeValidator(to_list)]
PostgresType = Annotated[str | None, BeforeValidator(check_postgres_type)]


# --- reusable schema parts ---------------------------------------------------
class FieldSpec(BaseModel):
    """A single column definition.

    - `id`   — SQL-safe identifier (required).
    - `type` — optional PostgreSQL type; aliases (`integer`, `bigint`, `varchar`,
      …) are normalised to their canonical Postgres form.
    - `info` — optional free-form data dictionary (title, description, unit, …).
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    type: PostgresType = None
    info: dict[str, Any] | None = None

    @field_validator("id")
    @classmethod
    def _check_not_reserved(cls, v: str) -> str:
        from datastore.core.constants import RESERVED_SYSTEM_COLUMN_NAMES
        if v in RESERVED_SYSTEM_COLUMN_NAMES:
            raise ValueError(
                f"field id {v!r} is reserved for engine-managed system "
                "columns; rename the field"
            )
        return v
