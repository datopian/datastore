"""SQL builders for `datastore_search` on the BigQuery backend.

Pure helpers — no I/O. Each builder returns the SQL string and a
``QueryJobConfig`` carrying the parameter bindings; the backend submits
both via ``client.query`` and wraps errors. Splitting filters / sort /
projection / count into this module keeps ``backend.py`` focused on
orchestration and makes the builders trivially unit-testable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datastore.infrastructure.engines.bigquery.lib import (
    JSON_FRICTIONLESS_TYPES,
    SYSTEM_COLUMN_NAMES,
    format_select_column,
)
from datastore.infrastructure.engines.bigquery.types import bigquery_type

if TYPE_CHECKING:
    from google.cloud import bigquery

# Frictionless type → BigQuery scalar parameter type. JSON/array/geojson
# deliberately absent — filter equality against those is rejected (see
# `build_search`). Anything else falls back to STRING.
_PARAM_TYPE: dict[str, str] = {
    "integer":  "INT64",
    "number":   "FLOAT64",
    "boolean":  "BOOL",
    "string":   "STRING",
    "date":     "DATE",
    "datetime": "TIMESTAMP",
    "time":     "TIME",
    "any":      "STRING",
}

# System columns are always available for projection / filter / sort.
_SYSTEM_FIELD_DEFS: dict[str, dict] = {
    "_id":         {"name": "_id",         "type": "integer"},
    "_updated_at": {"name": "_updated_at", "type": "datetime"},
}


def _column_type_map(schema: dict, *, include_updated_at: bool) -> dict[str, str]:
    """Map column name → Frictionless type, including system columns."""
    out: dict[str, str] = {}
    for f in schema.get("fields", []):
        name = f.get("name")
        if name and name not in SYSTEM_COLUMN_NAMES:
            out[name] = f.get("type") or "string"
    out["_id"] = "integer"
    if include_updated_at:
        out["_updated_at"] = "datetime"
    return out


def _ordered_columns(schema: dict, *, include_updated_at: bool) -> list[str]:
    """Default projection order: `_id`, user fields, then `_updated_at`.

    `_id` first matches CKAN convention (it's the row identifier).
    `_updated_at` trails so user columns stay together in tabular UIs.
    """
    user = [
        f["name"]
        for f in schema.get("fields", [])
        if f.get("name") and f["name"] not in SYSTEM_COLUMN_NAMES
    ]
    cols = ["_id", *user]
    if include_updated_at:
        cols.append("_updated_at")
    return cols


def parse_sort(sort_str: str, allowed: set[str]) -> list[tuple[str, str]]:
    """Parse a CKAN-style sort string into validated `(col, dir)` pairs.

    Format: ``"col1 asc, col2 desc"`` — direction defaults to ``ASC``
    when omitted. Validates every column against `allowed`; raises
    `ValueError` on unknown columns or non-`asc`/`desc` direction
    tokens. Validation is what makes it safe to inline the column
    name into the generated SQL (no parameter binding for identifiers).
    """
    out: list[tuple[str, str]] = []
    for part in sort_str.split(","):
        part = part.strip()
        if not part:
            continue
        tokens = part.split()
        if len(tokens) > 2:
            raise ValueError(
                f"sort entry {part!r} has too many tokens; expected "
                "'<column>' or '<column> asc|desc'"
            )
        col = tokens[0]
        direction = (tokens[1].upper() if len(tokens) == 2 else "ASC")
        if col not in allowed:
            raise ValueError(
                f"sort references unknown column {col!r}"
            )
        if direction not in ("ASC", "DESC"):
            raise ValueError(
                f"sort direction for {col!r} must be ASC or DESC, "
                f"got {direction!r}"
            )
        out.append((col, direction))
    return out


def project_schema(schema: dict, projected_cols: list[str]) -> dict:
    """Filter the table schema to just the projected columns, preserving
    order. System columns get synthesised entries when projected.
    """
    by_name = {
        f["name"]: f
        for f in schema.get("fields", [])
        if f.get("name")
    }
    out_fields: list[dict] = []
    for col in projected_cols:
        if col in by_name:
            out_fields.append(by_name[col])
        elif col in _SYSTEM_FIELD_DEFS:
            out_fields.append(_SYSTEM_FIELD_DEFS[col])
    out = {"fields": out_fields}
    if "primaryKey" in schema:
        out["primaryKey"] = schema["primaryKey"]
    return out


def _make_param(name: str, fr_type: str, value: Any) -> "bigquery.ScalarQueryParameter":
    from google.cloud import bigquery
    bq_type = _PARAM_TYPE.get(fr_type, "STRING")
    return bigquery.ScalarQueryParameter(name, bq_type, value)


def _make_array_param(name: str, fr_type: str, values: list[Any]) -> "bigquery.ArrayQueryParameter":
    from google.cloud import bigquery
    bq_type = _PARAM_TYPE.get(fr_type, "STRING")
    return bigquery.ArrayQueryParameter(name, bq_type, values)


def _build_where(
    *,
    filters: dict | None,
    q: str | dict | None,
    type_map: dict[str, str],
    table_alias: str,
    params: list,
) -> str:
    """Render the `WHERE` clause and append parameters in place.

    Filter values bind with the column's typed Frictionless → BigQuery
    parameter type so INTEGER columns get `INT64` params, etc. Lists
    become `IN UNNEST(@p)`. Full-text uses BigQuery's native `SEARCH()`
    (tokenised, leverages search indexes when present); string `q`
    searches the whole row, dict `q` searches per column.

    Raises `ValueError` for unknown columns or filters against
    JSON/array/geojson columns (no clean equality semantics in BQ).
    The backend converts this to `ValidationError`.
    """
    clauses: list[str] = []

    if filters:
        for col, value in filters.items():
            if col not in type_map:
                raise ValueError(
                    f"filters references unknown column {col!r}"
                )
            ftype = type_map[col]
            if ftype in JSON_FRICTIONLESS_TYPES:
                raise ValueError(
                    f"filters cannot target JSON/array/geojson column "
                    f"{col!r}; use datastore_search_sql for structural "
                    "matches"
                )
            name = f"f{len(params)}"
            if isinstance(value, list):
                params.append(_make_array_param(name, ftype, value))
                clauses.append(f"`{col}` IN UNNEST(@{name})")
            elif value is None:
                clauses.append(f"`{col}` IS NULL")
            else:
                params.append(_make_param(name, ftype, value))
                clauses.append(f"`{col}` = @{name}")

    if isinstance(q, str):
        name = f"f{len(params)}"
        params.append(_make_param(name, "string", q))
        # `SEARCH(<alias>, @q)` matches against every searchable column
        # of the row — BigQuery's native full-text. Honours search
        # indexes when defined; falls back to a tokenised scan otherwise.
        clauses.append(f"SEARCH({table_alias}, @{name})")
    elif isinstance(q, dict):
        for col, term in q.items():
            if col not in type_map:
                raise ValueError(
                    f"q references unknown column {col!r}"
                )
            name = f"f{len(params)}"
            params.append(_make_param(name, "string", str(term)))
            clauses.append(f"SEARCH(`{col}`, @{name})")

    return " AND ".join(clauses)


def _project_column(col: str, type_map: dict[str, str]) -> str:
    """Render a projected column for `datastore_search` / `build_count`.

    Translates the Frictionless type to BigQuery's name (via
    `bigquery_type`) and delegates to `lib.format_select_column` — the
    same helper `datastore_dump` uses for `EXPORT DATA`. Net effect:
    TIMESTAMP / DATETIME columns come back as the fixed ISO 8601 string
    `2026-06-08T00:00:00` (UTC implicit) in both endpoints; other
    columns pass through as the native type.
    """
    return format_select_column(col, bigquery_type(type_map.get(col)))


def build_search(
    *,
    table_ref: str,
    schema: dict,
    include_updated_at: bool,
    fields: list[str] | None,
    filters: dict | None,
    q: str | dict | None,
    distinct: bool,
    sort: str | None,
    limit: int,
    offset: int,
) -> tuple[str, list, dict]:
    """Build a parameterised SELECT.

    Returns `(sql, parameters, result_schema)` where `result_schema` is
    the Frictionless schema of the projected columns (used by the
    streaming writer for column ordering + types). Parameters are a
    list of `ScalarQueryParameter` / `ArrayQueryParameter` ready to
    drop into a `QueryJobConfig`.

    Layout: ``SELECT [DISTINCT] cols FROM target AS t [WHERE ...]
    [ORDER BY ...] LIMIT N OFFSET M``. Sort defaults to `_id ASC` when
    `_id` is projected (CKAN's row-id ordering convention); otherwise
    no default sort — caller must specify or accept BigQuery's order
    (undefined).

    Raises `ValueError` for unknown columns in `fields` / `sort` /
    `filters` / `q`. The backend converts to `ValidationError` so the
    caller sees a clean 400 instead of a 500.
    """
    type_map = _column_type_map(schema, include_updated_at=include_updated_at)
    all_cols = set(type_map)
    default_cols = _ordered_columns(schema, include_updated_at=include_updated_at)

    if fields is None:
        projected = list(default_cols)
    else:
        for f in fields:
            if f not in all_cols:
                raise ValueError(
                    f"fields references unknown column {f!r}"
                )
        projected = list(fields)
    if not projected:
        raise ValueError("`fields` must select at least one column")

    sort_pairs: list[tuple[str, str]]
    if sort:
        sort_pairs = parse_sort(sort, all_cols)
    elif "_id" in projected:
        sort_pairs = [("_id", "ASC")]
    else:
        sort_pairs = []

    params: list = []
    where = _build_where(
        filters=filters, q=q, type_map=type_map,
        table_alias="t", params=params,
    )

    parts: list[str] = []
    projection = ", ".join(_project_column(c, type_map) for c in projected)
    parts.append(
        f"SELECT {'DISTINCT ' if distinct else ''}{projection} "
        f"FROM {table_ref} AS t"
    )
    if where:
        parts.append(f"WHERE {where}")
    if sort_pairs:
        # Sort against the underlying table column (`t.<col>`) rather than
        # the projected alias — the datetime projection rewrites
        # `col` → `FORMAT_TIMESTAMP(...) AS col`, and we want ORDER BY to
        # operate on the native TIMESTAMP, not the formatted string. (The
        # ISO format happens to sort lexicographically the same way, but
        # the explicit reference future-proofs us if the format changes.)
        parts.append(
            "ORDER BY " + ", ".join(f"t.`{c}` {d}" for c, d in sort_pairs)
        )
    parts.append(f"LIMIT {int(limit)} OFFSET {int(offset)}")

    sql = " ".join(parts)
    return sql, params, project_schema(schema, projected)


def build_count(
    *,
    table_ref: str,
    schema: dict,
    include_updated_at: bool,
    fields: list[str] | None,
    filters: dict | None,
    q: str | dict | None,
    distinct: bool,
) -> tuple[str, list]:
    """Build a parameterised `COUNT(*)` for the same row set.

    Wraps a `SELECT [DISTINCT] cols FROM target WHERE ...` so the
    count matches the projection / dedup of the data query. Doesn't
    apply LIMIT/OFFSET — total is independent of paging.

    Raises `ValueError` on the same conditions as `build_search` (the
    backend should run validation once via `build_search` first; this
    builder re-runs it as defense in depth).
    """
    type_map = _column_type_map(schema, include_updated_at=include_updated_at)
    all_cols = set(type_map)
    default_cols = _ordered_columns(schema, include_updated_at=include_updated_at)

    if fields is None:
        projected = list(default_cols)
    else:
        for f in fields:
            if f not in all_cols:
                raise ValueError(
                    f"fields references unknown column {f!r}"
                )
        projected = list(fields)

    params: list = []
    where = _build_where(
        filters=filters, q=q, type_map=type_map,
        table_alias="t", params=params,
    )

    # Same projection rewrite as `build_search` — datetime columns are
    # FORMAT_TIMESTAMP-wrapped. Matters when `distinct=True`: the COUNT
    # then dedupes on the formatted (second-precision) string, matching
    # what the user sees in the data response.
    projection = ", ".join(_project_column(c, type_map) for c in projected)
    inner_parts = [
        f"SELECT {'DISTINCT ' if distinct else ''}{projection} "
        f"FROM {table_ref} AS t"
    ]
    if where:
        inner_parts.append(f"WHERE {where}")
    inner = " ".join(inner_parts)

    sql = f"SELECT COUNT(*) AS n FROM ({inner})"
    return sql, params


def needs_count_query(
    *,
    filters: dict | None,
    q: str | dict | None,
    distinct: bool,
) -> bool:
    """`True` when total must come from a real COUNT — anything that
    narrows / dedupes the result set. `False` lets the backend take
    the cheap `__TABLES__.row_count` path."""
    return bool(filters) or bool(q) or distinct
