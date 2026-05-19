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


def serialise_json_columns(
    schema: dict, records: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Encode any value bound for a JSON-typed column as a JSON string.

    Reads the column types from the canonical Frictionless schema, finds
    the ones that map to BigQuery `JSON`, and walks each row encoding
    just those values via `orjson`. `None` and already-`str` values pass
    through untouched. Other column values are passed through verbatim —
    no implicit type coercion.
    """
    import orjson

    json_cols = {
        f["name"]
        for f in schema.get("fields", [])
        if f.get("name") and f.get("type") in JSON_FRICTIONLESS_TYPES
    }
    if not json_cols:
        return records

    prepared: list[dict[str, Any]] = []
    for row in records:
        out = dict(row)
        for col in json_cols & out.keys():
            val = out[col]
            if val is None or isinstance(val, str):
                continue
            out[col] = orjson.dumps(val).decode("utf-8")
        prepared.append(out)
    return prepared
