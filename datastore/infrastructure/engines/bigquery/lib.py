"""Side-effect-free helpers for the BigQuery backend.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from datastore.core.exceptions import ConflictError
from datastore.infrastructure.engines.bigquery.types import (
    bigquery_type,
    can_widen,
    frictionless_type_from_bigquery,
)

if TYPE_CHECKING:
    from google.cloud import bigquery

log = logging.getLogger(__name__)

# Frictionless types that map to BigQuery `JSON`.
JSON_FRICTIONLESS_TYPES = frozenset({"object", "array", "geojson"})

# Engine-managed columns. `_id` always present; `_updated_at` opt-in
# via `Config.INCLUDE_UPDATED_AT`. Same-named user fields are dropped.
SYSTEM_COLUMN_NAMES: frozenset[str] = frozenset({"_id", "_updated_at"})

# Frictionless type → BigQuery scalar parameter type for filter values.
# JSON / array / geojson absent — equality on those is rejected.
_FILTER_PARAM_TYPE: dict[str, str] = {
    "integer":  "INT64",
    "number":   "FLOAT64",
    "boolean":  "BOOL",
    "string":   "STRING",
    "date":     "DATE",
    "datetime": "TIMESTAMP",
    "time":     "TIME",
    "any":      "STRING",
}

# Native-metadata: sentinel under which the engine namespaces its own
# metadata in the table-level `description`. NOTE: the description is
# engine-owned — each write (`set_table_options_sql`) rewrites it wholesale
# from the schema, so any human-authored description text or non-datastore
# labels set in the BQ console are NOT preserved across a refresh.
DATASTORE_KEY = "datastore"

# Bumped when the JSON shape under `DATASTORE_KEY` changes in a way
# that needs explicit translation on read.
SCHEMA_VERSION = 1


# ── 3. system columns ───────────────────────────────────────────────────────


def _system_col_defs(include_updated_at: bool) -> tuple[str, ...]:
    return (
        ("`_id` INT64", "`_updated_at` TIMESTAMP")
        if include_updated_at
        else ("`_id` INT64",)
    )


def _system_col_insert_list(include_updated_at: bool) -> str:
    return "`_id`, `_updated_at`" if include_updated_at else "`_id`"


def format_select_column(name: str, bq_type: str | None) -> str:
    """Render a SELECT-list expression for one column, casting TIMESTAMP /
    DATETIME values to a fixed-shape ISO 8601 string
    (`YYYY-MM-DDTHH:MM:SS` — UTC implicit, no offset, no fractional).
    Other types pass through unchanged.

    Shared by `datastore_search` (engine-built projection) and the
    `datastore_dump` `EXPORT DATA` SELECT, so both read paths emit the
    same timestamp shape from BigQuery's side. TIMESTAMP is rendered in
    UTC; the resulting string carries no offset, so consumers must treat
    any timestamp value as UTC.
    """
    bq = (bq_type or "").upper()
    if bq == "TIMESTAMP":
        return (
            f"FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', `{name}`, 'UTC') AS `{name}`"
        )
    if bq == "DATETIME":
        return (
            f"FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', `{name}`) AS `{name}`"
        )
    return f"`{name}`"


def normalize_pk(schema: dict) -> list[str]:
    """`schema.primaryKey` as a list (str → 1-elem, missing → empty)."""
    pk = schema.get("primaryKey")
    if isinstance(pk, str):
        return [pk]
    return list(pk or [])


def _user_fields(schema: dict) -> list[dict]:
    """Declared fields minus engine-managed system columns."""
    return [
        f for f in schema.get("fields", [])
        if f.get("name") and f["name"] not in SYSTEM_COLUMN_NAMES
    ]


# ── 4. DDL builders (CREATE / ALTER / DROP) ────────────────────────────────


def column_defs(schema: dict, *, include_updated_at: bool = True) -> list[str]:
    """Render `schema.fields` as ``\\`name\\` TYPE`` column DDL.

    Per-field metadata (`info`, `title`, …) is NOT emitted per column
    — the full schema lives in the table-level OPTIONS instead so
    reads need one slot, not N. System columns are prepended;
    user fields that collide with them are dropped.
    """
    cols: list[str] = list(_system_col_defs(include_updated_at))
    for f in schema.get("fields", []):
        name = f.get("name")
        if not name or name in SYSTEM_COLUMN_NAMES:
            continue
        cols.append(f"`{name}` {bigquery_type(f.get('type'))}")
    return cols


def schema_diff(
    old_schema: dict, new_schema: dict
) -> tuple[list[str], list[tuple[str, str | None, str | None]], list[str]]:
    """Return `(added, type_changes, removed)` between two schemas.
    Types are raw Frictionless values; dialect mapping is the caller's job."""
    old_by_name = {
        f["name"]: f for f in old_schema.get("fields", []) if f.get("name")
    }
    new_by_name = {
        f["name"]: f for f in new_schema.get("fields", []) if f.get("name")
    }

    added = [n for n in new_by_name if n not in old_by_name]
    type_changes = [
        (n, old_by_name[n].get("type"), new_by_name[n].get("type"))
        for n in new_by_name
        if n in old_by_name
        and old_by_name[n].get("type") != new_by_name[n].get("type")
    ]
    removed = [n for n in old_by_name if n not in new_by_name]
    return added, type_changes, removed


def reject_unsupported_type_changes(
    type_changes: list[tuple[str, str | None, str | None]],
) -> None:
    """Raise `ConflictError` if any transition isn't a BigQuery widening."""
    unsupported = [
        f"'{name}' ({old_t} → {new_t})"
        for name, old_t, new_t in type_changes
        if not can_widen(bigquery_type(old_t), bigquery_type(new_t))
    ]
    if not unsupported:
        return
    head = (
        f"Cannot change column type for {unsupported[0]}"
        if len(unsupported) == 1
        else f"Cannot change column types: {', '.join(unsupported)}"
    )
    raise ConflictError(
        f"{head}. BigQuery does not support this conversion in place. "
        "To apply, recreate the resource with the new schema."
    )


def alter_clauses(
    added: list[str],
    type_changes: list[tuple[str, str | None, str | None]],
    new_schema: dict,
) -> list[str]:
    """Per-column clauses for a single `ALTER TABLE` statement."""
    new_by_name = {
        f["name"]: f for f in new_schema.get("fields", []) if f.get("name")
    }
    clauses: list[str] = []
    for name in added:
        field = new_by_name[name]
        clauses.append(
            f"ADD COLUMN IF NOT EXISTS `{name}` "
            f"{bigquery_type(field.get('type'))}"
        )
    for name, _, new_t in type_changes:
        clauses.append(
            f"ALTER COLUMN `{name}` SET DATA TYPE {bigquery_type(new_t)}"
        )
    return clauses


def drop_columns_sql(table_ref: str, columns: list[str]) -> str:
    """Render ``ALTER TABLE <target> DROP COLUMN …``.
    Caller validates column names — identifiers can't be parameterised."""
    if not columns:
        raise ValueError("drop_columns_sql requires at least one column")
    clauses = ", ".join(f"DROP COLUMN `{c}`" for c in columns)
    return f"ALTER TABLE {table_ref} {clauses}"


# ── 5. DML builders (INSERT / MERGE / UPDATE / DELETE) ─────────────────────


def insert_sql(
    table_ref: str, schema: dict, *, include_updated_at: bool = True
) -> str:
    """Render `INSERT INTO … SELECT FROM UNNEST(JSON_QUERY_ARRAY(@rows))`.

    `_id` is inlined as `(SELECT IFNULL(MAX(_id), 0) FROM tbl) +
    ROW_NUMBER() OVER ()` — one MAX baseline per batch, no separate
    probe.
    """
    fields = _user_fields(schema)
    if not fields:
        raise ValueError("schema has no user fields; cannot INSERT")

    data_cols = ", ".join(f"`{f['name']}`" for f in fields)
    data_extractors = ", ".join(_json_extract(f) for f in fields)
    sys_cols = _system_col_insert_list(include_updated_at)
    id_expr = (
        f"(SELECT IFNULL(MAX(`_id`), 0) FROM {table_ref}) "
        f"+ ROW_NUMBER() OVER ()"
    )
    sys_vals = (
        f"{id_expr}, CURRENT_TIMESTAMP()"
        if include_updated_at
        else id_expr
    )
    return (
        f"INSERT INTO {table_ref} ({sys_cols}, {data_cols}) "
        f"SELECT {sys_vals}, {data_extractors} "
        f"FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS r"
    )


def merge_sql(
    table_ref: str, schema: dict, *, include_updated_at: bool = True
) -> str:
    """Render `MERGE` keyed by `schema.primaryKey`.

    Matched rows update only when a non-PK column differs (so
    `_updated_at` advances only on real changes). Unmatched rows
    insert with `_id` continuing from `MAX(_id) + _rn`.
    """
    fields = _user_fields(schema)
    pk = normalize_pk(schema)
    if not pk:
        raise ValueError(
            "schema has no 'primaryKey'; upsert requires one to "
            "match existing rows"
        )

    pk_set = set(pk)
    non_pk = [f for f in fields if f["name"] not in pk_set]

    using_cols = ", ".join(
        f"{_json_extract(f)} AS `{f['name']}`" for f in fields
    )
    on_clause = " AND ".join(f"T.`{n}` = S.`{n}`" for n in pk)
    insert_cols = ", ".join(f"`{f['name']}`" for f in fields)
    insert_vals = ", ".join(f"S.`{f['name']}`" for f in fields)

    parts = [
        f"MERGE {table_ref} T",
        f"USING (SELECT {using_cols}, ROW_NUMBER() OVER () AS _rn "
        f"FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS r) S",
        f"ON {on_clause}",
    ]
    if non_pk:
        diff_predicate = " OR ".join(_diff_expr(f) for f in non_pk)
        matched_assignments = [
            f"T.`{f['name']}` = S.`{f['name']}`" for f in non_pk
        ]
        if include_updated_at:
            matched_assignments.append(
                "T.`_updated_at` = CURRENT_TIMESTAMP()"
            )
        parts.append(
            f"WHEN MATCHED AND ({diff_predicate}) "
            f"THEN UPDATE SET {', '.join(matched_assignments)}"
        )

    sys_cols = _system_col_insert_list(include_updated_at)
    id_value = f"(SELECT IFNULL(MAX(`_id`), 0) FROM {table_ref}) + S._rn"
    sys_vals = (
        f"{id_value}, CURRENT_TIMESTAMP()"
        if include_updated_at
        else id_value
    )
    parts.append(
        f"WHEN NOT MATCHED THEN INSERT "
        f"({sys_cols}, {insert_cols}) "
        f"VALUES ({sys_vals}, {insert_vals})"
    )
    return " ".join(parts)


def update_sql(
    table_ref: str, schema: dict, *, include_updated_at: bool = True
) -> str:
    """Render `UPDATE T SET … FROM (UNNEST(@rows)) S WHERE <pk match>`.

    Caller must compare affected rows to input size and raise
    `NotFoundError` for unmatched keys — DML UPDATE silently no-ops
    on misses.
    """
    fields = _user_fields(schema)
    pk = normalize_pk(schema)
    if not pk:
        raise ValueError(
            "schema has no 'primaryKey'; update requires one to "
            "match existing rows"
        )

    pk_set = set(pk)
    non_pk = [f for f in fields if f["name"] not in pk_set]
    if not non_pk and not include_updated_at:
        raise ValueError(
            "schema has no non-key columns to update and the "
            "`_updated_at` system column is disabled; nothing to SET"
        )

    using_cols = ", ".join(
        f"{_json_extract(f)} AS `{f['name']}`" for f in fields
    )
    set_parts = [
        f"T.`{f['name']}` = S.`{f['name']}`" for f in non_pk
    ]
    if include_updated_at:
        set_parts.append("T.`_updated_at` = CURRENT_TIMESTAMP()")
    set_clause = ", ".join(set_parts)
    where_clause = " AND ".join(f"T.`{n}` = S.`{n}`" for n in pk)

    return (
        f"UPDATE {table_ref} T "
        f"SET {set_clause} "
        f"FROM (SELECT {using_cols} "
        f"FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS r) S "
        f"WHERE {where_clause}"
    )


def delete_sql(
    table_ref: str,
    schema: dict,
    filters: dict,
) -> tuple[str, list]:
    """Render parameterised ``DELETE FROM <target> WHERE …``.

    Empty `filters` yields `WHERE TRUE` (BQ requires a WHERE on every
    DELETE). Returns `(sql, query_parameters)`.
    """
    # Lazy import keeps `google-cloud-bigquery` engine-private.
    from google.cloud import bigquery

    if filters is None or not isinstance(filters, dict):
        raise ValueError(
            "delete filters must be a dict; use the DROP path when no "
            "filter is intended"
        )

    type_map: dict[str, str] = {}
    for f in schema.get("fields", []):
        name = f.get("name")
        if name and name not in SYSTEM_COLUMN_NAMES:
            type_map[name] = f.get("type") or "string"
    # System columns are always filterable.
    type_map["_id"] = "integer"
    type_map["_updated_at"] = "datetime"

    params: list = []
    clauses: list[str] = []
    for col, value in filters.items():
        if col not in type_map:
            raise ValueError(
                f"filters references unknown column {col!r}"
            )
        ftype = type_map[col]
        if ftype in JSON_FRICTIONLESS_TYPES:
            raise ValueError(
                f"filters cannot target JSON/array/geojson column "
                f"{col!r}; use datastore_search_sql for structural matches"
            )
        bq_type = _FILTER_PARAM_TYPE.get(ftype, "STRING")
        name = f"f{len(params)}"
        if isinstance(value, list):
            params.append(
                bigquery.ArrayQueryParameter(name, bq_type, value)
            )
            clauses.append(f"`{col}` IN UNNEST(@{name})")
        elif value is None:
            clauses.append(f"`{col}` IS NULL")
        else:
            params.append(
                bigquery.ScalarQueryParameter(name, bq_type, value)
            )
            clauses.append(f"`{col}` = @{name}")

    where = " AND ".join(clauses) if clauses else "TRUE"
    return f"DELETE FROM {table_ref} WHERE {where}", params


def _diff_expr(field: dict) -> str:
    """NULL-safe inequality between `T.<col>` and `S.<col>`.
    JSON columns are canonicalised via `TO_JSON_STRING` first."""
    name = field["name"]
    if field.get("type") in JSON_FRICTIONLESS_TYPES:
        return (
            f"TO_JSON_STRING(T.`{name}`) IS DISTINCT FROM "
            f"TO_JSON_STRING(S.`{name}`)"
        )
    return f"T.`{name}` IS DISTINCT FROM S.`{name}`"


def _json_extract(field: dict) -> str:
    """Typed extraction of a field from JSON row variable `r`."""
    name = field["name"]
    fr_type = field.get("type")
    bq_type = bigquery_type(fr_type)
    path = f"'$.{name}'"
    if fr_type in JSON_FRICTIONLESS_TYPES:
        return f"PARSE_JSON(JSON_QUERY(r, {path}))"
    if bq_type == "STRING":
        return f"JSON_VALUE(r, {path})"
    return f"CAST(JSON_VALUE(r, {path}) AS {bq_type})"


# ── 6. SQL parsing utilities (sqlglot) ──────────────────────────────────────


def unfiltered_table_name(
    sql: str, *, dialect: str = "bigquery",
) -> str | None:
    """Return the source table name when `sql` is a plain
    `SELECT cols FROM <table> [LIMIT/OFFSET]` — i.e. result row count
    = source row count. `None` if any clause could change row count
    (WHERE, GROUP BY, JOIN, DISTINCT, aggregates, set ops, subqueries).

    Lets `datastore_search_sql` route the unfiltered total through
    free `INFORMATION_SCHEMA.TABLE_STORAGE` instead of a full COUNT(*).
    """
    # Lazy import — sqlglot is heavy.
    import sqlglot
    from sqlglot import expressions as exp

    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        return None

    if not isinstance(tree, exp.Select):
        return None

    blockers = ("where", "group", "having", "joins", "distinct", "qualify")
    if any(tree.args.get(k) for k in blockers):
        return None

    # Nested SELECTs (subqueries) can reduce / expand rows in ways
    # the surrounding clauses don't reveal — bail.
    if sum(1 for _ in tree.find_all(exp.Select)) > 1:
        return None

    aggregates = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
    if next(tree.find_all(*aggregates), None) is not None:
        return None

    cte_aliases = {
        c.alias_or_name for c in tree.find_all(exp.CTE)
        if c.alias_or_name
    }
    tables = [
        t for t in tree.find_all(exp.Table)
        if t.name and t.name not in cte_aliases
    ]
    if len(tables) != 1:
        return None
    return tables[0].name


def strip_limit_offset(sql: str, *, dialect: str = "bigquery") -> str:
    """Return `sql` with LIMIT/OFFSET removed — used when wrapping a
    paginated user query in `SELECT COUNT(*) FROM (...)` for the total."""
    import sqlglot

    tree = sqlglot.parse_one(sql, dialect=dialect)
    tree.set("limit", None)
    tree.set("offset", None)
    return tree.sql(dialect=dialect)


def qualify_table_refs(sql: str, project: str, dataset: str) -> str:
    """Rewrite unqualified table refs to `project.dataset.<name>` for BigQuery.

    User SQL refs look like CKAN resource_ids (`FROM "uuid"` /
    `FROM uuid`); we parse as postgres (accepts double-quoted idents)
    and re-emit as BigQuery. Already-qualified refs and CTE aliases
    are left alone.
    """
    import sqlglot
    from sqlglot import expressions as exp

    tree = sqlglot.parse_one(sql, dialect="postgres")
    cte_aliases = {
        cte.alias_or_name for cte in tree.find_all(exp.CTE)
        if cte.alias_or_name
    }
    for table in tree.find_all(exp.Table):
        name = table.name
        if not name or name in cte_aliases:
            continue
        if table.args.get("catalog") is not None:
            continue
        table.set("catalog", exp.to_identifier(project, quoted=True))
        table.set("db", exp.to_identifier(dataset, quoted=True))
    return tree.sql(dialect="bigquery")


# ── 7. native metadata (encoders + parsers) ────────────────────────────────
#
# Writes go through `table_options_clause` (on CREATE) or
# `set_table_options_sql` (on ALTER). Reads happen in `backend.py`
# via a single `client.get_table` call piped through `table_to_schema`.


def table_to_schema(table: "bigquery.Table") -> dict[str, Any]:
    """Reconstruct the Frictionless schema stored on `table`.

    Returns the user-supplied schema dict verbatim when the table
    carries our `datastore.schema` block; falls back to BQ-column
    inference for unmanaged tables.
    """
    table_meta = _parse_description(table.description).get(DATASTORE_KEY, {})
    stored = table_meta.get("schema")
    if isinstance(stored, dict):
        return stored
    return _infer_schema_from_bq(table)


def _infer_schema_from_bq(table: "bigquery.Table") -> dict[str, Any]:
    """Minimal Frictionless schema from BQ column metadata.
    Used only for tables created outside the engine."""
    fields: list[dict[str, Any]] = [
        {
            "name": col.name,
            # Map to canonical Frictionless names — downstream helpers
            # (filter type maps in delete_sql / search) only understand
            # those, not raw BigQuery type names.
            "type": frictionless_type_from_bigquery(col.field_type),
        }
        for col in table.schema
        if col.name not in SYSTEM_COLUMN_NAMES
    ]
    return {"fields": fields}


def table_options_clause(schema: dict[str, Any]) -> str:
    """Return ` OPTIONS(description = '…', labels = […])` for a table DDL."""
    desc = _encode_table_description(schema)
    return (
        f" OPTIONS(description = {_sql_literal(desc)}, "
        f"labels = [(\"datastore_managed\", \"true\")])"
    )


def set_table_options_sql(table_ref: str, schema: dict[str, Any]) -> str:
    """Stand-alone `ALTER TABLE … SET OPTIONS(...)` statement.

    Separate from column ALTERs because BQ refuses to mix column
    actions and SET OPTIONS in one statement.
    """
    desc = _encode_table_description(schema)
    return (
        f"ALTER TABLE {table_ref} "
        f"SET OPTIONS(description = {_sql_literal(desc)}, "
        f"labels = [(\"datastore_managed\", \"true\")])"
    )


def _parse_description(s: str | None) -> dict[str, Any]:
    """Parse a description blob as JSON, or `{}` on empty/non-object/malformed."""
    if not s:
        return {}
    try:
        result = json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return {}
    return result if isinstance(result, dict) else {}


def _encode_table_description(schema: dict[str, Any]) -> str:
    """JSON-encode the user schema verbatim under `datastore.schema`.
    Strips engine-managed system columns from `fields` first."""
    payload: dict[str, Any] = {
        DATASTORE_KEY: {
            "schema_version": SCHEMA_VERSION,
            "schema": _strip_system_fields(schema),
        }
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _strip_system_fields(schema: dict[str, Any]) -> dict[str, Any]:
    """Shallow copy of `schema` with `_id`/`_updated_at` filtered out of `fields`."""
    return {
        **schema,
        "fields": [
            f for f in schema.get("fields", [])
            if f.get("name") not in SYSTEM_COLUMN_NAMES
        ],
    }


def _sql_literal(s: str) -> str:
    """Single-quote a string for inline use in BigQuery SQL.

    Order matters: backslashes first, then single quotes. Only
    descriptive payloads go through here — never identifiers.
    """
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"
