"""Side-effect-free helpers for the BigQuery backend: schema diffs,
DDL clause rendering, DML statement builders, JSON extractors."""

from __future__ import annotations

from datastore.core.exceptions import ConflictError
from datastore.infrastructure.engines.bigquery.types import (
    bigquery_type,
    can_widen,
)

# Frictionless types that map to BigQuery `JSON`.
JSON_FRICTIONLESS_TYPES = frozenset({"object", "array", "geojson"})

# Engine-managed columns. `_id` always present; `_updated_at` opt-in
# via `Config.INCLUDE_UPDATED_AT`. Same-named user fields are dropped.
SYSTEM_COLUMN_NAMES: frozenset[str] = frozenset({"_id", "_updated_at"})


def _system_col_defs(include_updated_at: bool) -> tuple[str, ...]:
    return (
        ("`_id` INT64", "`_updated_at` TIMESTAMP")
        if include_updated_at
        else ("`_id` INT64",)
    )


def _system_col_insert_list(include_updated_at: bool) -> str:
    return "`_id`, `_updated_at`" if include_updated_at else "`_id`"


def column_defs(schema: dict, *, include_updated_at: bool = True) -> list[str]:
    """Render `schema.fields` as ``\\`name\\` TYPE`` for `CREATE TABLE`,
    prepending system columns and skipping any field that collides."""
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
    """Per-column clauses for a single `ALTER TABLE`."""
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


def insert_sql(
    table_ref: str, schema: dict, *, include_updated_at: bool = True
) -> str:
    """Render `INSERT INTO ... SELECT FROM UNNEST(JSON_QUERY_ARRAY(@rows))`.

    DML (not streaming) so follow-up MERGE/UPDATE stays consistent.
    `_id` = `(SELECT IFNULL(MAX(_id), 0) FROM tbl) + ROW_NUMBER() OVER ()`
    — one MAX baseline per batch, ROW_NUMBER per row.
    """
    fields = [
        f for f in schema.get("fields", [])
        if f.get("name") and f["name"] not in SYSTEM_COLUMN_NAMES
    ]
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

    Matched rows update only if a non-PK column differs (so
    `_updated_at` advances only on real changes). Unmatched rows
    insert with `_id` = `(SELECT MAX(_id) FROM tbl) + _rn`.
    """
    fields = [
        f for f in schema.get("fields", [])
        if f.get("name") and f["name"] not in SYSTEM_COLUMN_NAMES
    ]
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
    """Render `UPDATE T SET ... FROM (SELECT ... FROM UNNEST(@rows)) S
    WHERE <pk match>`. Caller must compare affected rows to input size
    and raise `NotFoundError` for unmatched keys."""
    fields = [
        f for f in schema.get("fields", [])
        if f.get("name") and f["name"] not in SYSTEM_COLUMN_NAMES
    ]
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


def _diff_expr(field: dict) -> str:
    """NULL-safe inequality between `T.<col>` and `S.<col>`. JSON
    columns are canonicalised via `TO_JSON_STRING` first."""
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


def drop_columns_sql(table_ref: str, columns: list[str]) -> str:
    """Render ``ALTER TABLE <target> DROP COLUMN …``. Caller must
    validate column names against the schema first (identifiers can't
    be parameterised)."""
    if not columns:
        raise ValueError("drop_columns_sql requires at least one column")
    clauses = ", ".join(f"DROP COLUMN `{c}`" for c in columns)
    return f"ALTER TABLE {table_ref} {clauses}"


def delete_sql(
    table_ref: str,
    schema: dict,
    filters: dict,
) -> tuple[str, list]:
    """Render parameterised ``DELETE FROM <target> WHERE …``. Empty
    ``filters`` yields ``WHERE TRUE`` (BigQuery requires a WHERE on
    every DELETE). Returns ``(sql, query_parameters)``."""
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


def unfiltered_table_name(
    sql: str, *, dialect: str = "bigquery",
) -> str | None:
    """Return the single source table name when `sql` is a plain
    `SELECT cols FROM <table> [LIMIT/OFFSET]` — i.e. the result row
    count equals the source table's row count.

    Returns None whenever any clause could change the row count:
    WHERE, GROUP BY, HAVING, JOIN, DISTINCT, QUALIFY, aggregate
    functions, set ops (UNION/EXCEPT/INTERSECT), subqueries, or more
    than one source table. The caller falls back to a real
    `COUNT(*) FROM (<inner>)` in those cases.

    Used by `datastore_search_sql` to route the unfiltered total
    through `INFORMATION_SCHEMA.TABLE_STORAGE` — free metadata read,
    no bytes scanned — instead of a full table scan via COUNT(*).
    """
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
    """Return `sql` with its LIMIT and OFFSET clauses removed.

    Used by `datastore_search_sql` to wrap the user's filtered query in
    a `SELECT COUNT(*) FROM (...)` for the total — the count has to
    ignore the page size or it would just report the current page's
    row count, breaking `total_pages` / `next` links.
    """
    import sqlglot

    tree = sqlglot.parse_one(sql, dialect=dialect)
    tree.set("limit", None)
    tree.set("offset", None)
    return tree.sql(dialect=dialect)


def qualify_table_refs(sql: str, project: str, dataset: str) -> str:
    """Rewrite every non-CTE table reference to its fully-qualified
    BigQuery form and re-serialise the SQL in BigQuery dialect.

    Users pass `datastore_search_sql` SQL with table refs that look
    like CKAN resource_ids (`FROM "uuid"` or `FROM uuid`). BigQuery
    needs `project.dataset.uuid` with backticked identifiers — so the
    backend parses the user's SQL (postgres dialect, which accepts
    double-quoted identifiers), tags each unqualified table with the
    configured project + dataset, and serialises out as BigQuery SQL.

    Tables that already carry a `catalog` (project) are left alone —
    callers who fully-qualify their refs win against the auto-prefix.
    CTE aliases are also skipped (they're defined inline, not external
    tables).
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
