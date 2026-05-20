"""Pure helpers used by the BigQuery backend.

Everything in here is side-effect free — schema diffs, DDL clause
rendering, JSON-column serialisation, error formatting. The backend
class in `backend.py` orchestrates I/O; these helpers handle the
data-shape massaging so the orchestration stays focused on the
sequence of side effects.

Kept separate from `backend.py` so the helpers are trivially unit
testable, and so a future engine can copy the file (or import from
it) when it needs the same Frictionless-schema reasoning.
"""

from __future__ import annotations

from typing import Any

from datastore.core.exceptions import ConflictError
from datastore.infrastructure.engines.bigquery.types import (
    bigquery_type,
    can_widen,
)

# Frictionless types whose BigQuery column type is `JSON`.
# `insert_rows_json` requires JSON-typed values to arrive as JSON
# strings, not native dicts / lists.
JSON_FRICTIONLESS_TYPES = frozenset({"object", "array", "geojson"})


def column_defs(schema: dict) -> list[str]:
    """Render `schema.fields` as ``\\`name\\` TYPE`` column declarations
    for a `CREATE TABLE` statement.
    """
    return [
        f"`{f['name']}` {bigquery_type(f.get('type'))}"
        for f in schema.get("fields", [])
        if f.get("name")
    ]


def schema_diff(
    old_schema: dict, new_schema: dict
) -> tuple[list[str], list[tuple[str, str | None, str | None]], list[str]]:
    """Compute `(added, type_changes, removed)` between two schemas.

    `type_changes` is `(name, old_type, new_type)` — types are the raw
    Frictionless values; mapping to BigQuery happens at the call site
    so the diff stays dialect-agnostic.
    """
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
    """Raise `ConflictError` if any transition isn't a BigQuery-allowed
    widening. Validation happens up-front so a single bad column never
    half-applies the rest of an `ALTER TABLE`.
    """
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
    """Render the per-column clauses for a single `ALTER TABLE`."""
    new_by_name = {
        f["name"]: f for f in new_schema.get("fields", []) if f.get("name")
    }
    clauses: list[str] = []
    for name in added:
        clauses.append(
            f"ADD COLUMN IF NOT EXISTS `{name}` "
            f"{bigquery_type(new_by_name[name].get('type'))}"
        )
    for name, _, new_t in type_changes:
        clauses.append(
            f"ALTER COLUMN `{name}` SET DATA TYPE {bigquery_type(new_t)}"
        )
    return clauses


def insert_sql(table_ref: str, schema: dict) -> str:
    """Render a DML `INSERT INTO ... SELECT FROM UNNEST(@rows)` statement.

    BigQuery's streaming insert (`Client.insert_rows_json`) puts rows
    into a streaming buffer that DML (`UPDATE` / `DELETE` / `MERGE`)
    cannot touch for 30–90 minutes until it flushes. That makes
    `datastore_create` (streaming) + immediate `datastore_upsert`
    (MERGE) fundamentally broken. DML INSERT writes directly to
    storage — no streaming buffer, immediate consistency for any
    follow-up upsert/update on the same primaryKey.

    Source rows arrive as a JSON-array string parameter `@rows`,
    identical to `merge_sql` / `update_sql`. Each column is extracted
    with the appropriate typed accessor (`_json_extract`) so the
    statement is one round-trip regardless of batch size.

    Raises `ValueError` when the schema has no fields; the backend
    converts that to `ValidationError`.
    """
    fields = [f for f in schema.get("fields", []) if f.get("name")]
    if not fields:
        raise ValueError("schema has no fields; cannot INSERT")

    insert_cols = ", ".join(f"`{f['name']}`" for f in fields)
    select_cols = ", ".join(_json_extract(f) for f in fields)
    return (
        f"INSERT INTO {table_ref} ({insert_cols}) "
        f"SELECT {select_cols} "
        f"FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS r"
    )


def merge_sql(table_ref: str, schema: dict) -> str:
    """Render a BigQuery `MERGE` statement for upserting rows keyed by
    the schema's `primaryKey`.

    Rows arrive as a JSON-array string parameter `@rows`; `JSON_QUERY_ARRAY`
    splits it into JSON values, then each column is extracted with the
    right typed accessor:

      - JSON columns (`object` / `array` / `geojson`) → `PARSE_JSON(JSON_QUERY(...))`
      - `STRING` columns → `JSON_VALUE(...)` (returns the string verbatim)
      - everything else → `CAST(JSON_VALUE(...) AS <bq-type>)`

    `ON` joins on every primaryKey column. `WHEN MATCHED` updates the
    non-key columns; `WHEN NOT MATCHED` inserts the full row. If every
    column is part of the primary key the UPDATE branch is omitted.

    Raises `ValueError` if `schema.primaryKey` is missing or empty —
    upsert has no meaningful semantics without one; the backend turns
    that into a `ValidationError` for the caller.
    """
    fields = [f for f in schema.get("fields", []) if f.get("name")]
    pk_raw = schema.get("primaryKey")
    pk: list[str] = (
        [pk_raw] if isinstance(pk_raw, str) else list(pk_raw or [])
    )
    if not pk:
        raise ValueError(
            "schema has no 'primaryKey'; upsert requires one to "
            "match existing rows"
        )

    pk_set = set(pk)
    non_pk = [f for f in fields if f["name"] not in pk_set]

    # USING clause — typed extraction from the JSON row variable `r`.
    using_cols = ", ".join(
        f"{_json_extract(f)} AS `{f['name']}`" for f in fields
    )
    on_clause = " AND ".join(f"T.`{n}` = S.`{n}`" for n in pk)
    insert_cols = ", ".join(f"`{f['name']}`" for f in fields)
    insert_vals = ", ".join(f"S.`{f['name']}`" for f in fields)

    parts = [
        f"MERGE {table_ref} T",
        f"USING (SELECT {using_cols} "
        f"FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS r) S",
        f"ON {on_clause}",
    ]
    if non_pk:
        update_set = ", ".join(
            f"T.`{f['name']}` = S.`{f['name']}`" for f in non_pk
        )
        parts.append(f"WHEN MATCHED THEN UPDATE SET {update_set}")
    parts.append(
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    return " ".join(parts)


def update_sql(table_ref: str, schema: dict) -> str:
    """Render a BigQuery DML `UPDATE` statement for updating rows
    keyed on `schema.primaryKey`.

    Single statement using `UPDATE … FROM (SELECT … FROM UNNEST(...))`
    syntax. Rows arrive as a JSON-array string parameter `@rows`,
    identical to `merge_sql`. The caller is expected to check
    `num_dml_affected_rows` against the input row count and surface a
    `NotFoundError` when any row's primary key didn't match — DML
    UPDATE silently does nothing for unmatched rows.

    Raises `ValueError` when:
      - `schema.primaryKey` is missing or empty (no key → no row to
        match);
      - every column is part of the primary key (nothing to SET).
    The backend converts both to `ValidationError`.
    """
    fields = [f for f in schema.get("fields", []) if f.get("name")]
    pk_raw = schema.get("primaryKey")
    pk: list[str] = (
        [pk_raw] if isinstance(pk_raw, str) else list(pk_raw or [])
    )
    if not pk:
        raise ValueError(
            "schema has no 'primaryKey'; update requires one to "
            "match existing rows"
        )

    pk_set = set(pk)
    non_pk = [f for f in fields if f["name"] not in pk_set]
    if not non_pk:
        raise ValueError(
            "schema has no non-key columns to update; every field is "
            "part of 'primaryKey'"
        )

    using_cols = ", ".join(
        f"{_json_extract(f)} AS `{f['name']}`" for f in fields
    )
    set_clause = ", ".join(
        f"T.`{f['name']}` = S.`{f['name']}`" for f in non_pk
    )
    where_clause = " AND ".join(f"T.`{n}` = S.`{n}`" for n in pk)

    return (
        f"UPDATE {table_ref} T "
        f"SET {set_clause} "
        f"FROM (SELECT {using_cols} "
        f"FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS r) S "
        f"WHERE {where_clause}"
    )


def _json_extract(field: dict) -> str:
    """Build the typed extraction expression for a single schema field
    against a JSON row variable `r`. See `merge_sql` for the rules."""
    name = field["name"]
    fr_type = field.get("type")
    bq_type = bigquery_type(fr_type)
    path = f"'$.{name}'"
    if fr_type in JSON_FRICTIONLESS_TYPES:
        return f"PARSE_JSON(JSON_QUERY(r, {path}))"
    if bq_type == "STRING":
        return f"JSON_VALUE(r, {path})"
    return f"CAST(JSON_VALUE(r, {path}) AS {bq_type})"


