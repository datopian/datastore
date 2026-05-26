"""Frictionless ↔ BigQuery type mapping.

One module per engine owns the dialect translation between the
canonical Frictionless Table Schema vocabulary the rest of the app
speaks and the storage engine's native types. Keeping it isolated
makes it easy to add a new engine (DuckLake / Postgres / …) without
touching anything outside its own subpackage.

The mapping is intentionally permissive: unknown Frictionless types
fall through to `STRING` rather than raising, so a slightly newer
schema spec (or a custom type) still loads. Strict validation lives
upstream in `schemas/validators.py:validate_frictionless_schema`.
"""

from __future__ import annotations

# Many-to-one on purpose: every Frictionless type maps to the widest
# BigQuery type that can hold its values. `year` → INT64 keeps it
# arithmetically useful; `yearmonth` / `duration` stay STRING because
# BigQuery has no native equivalent that round-trips losslessly.
FRICTIONLESS_TO_BIGQUERY: dict[str, str] = {
    "integer":   "INT64",
    "number":    "FLOAT64",
    "string":    "STRING",
    "boolean":   "BOOL",
    "date":      "DATE",
    "time":      "TIME",
    "datetime":  "TIMESTAMP",
    "duration":  "STRING",
    "object":    "JSON",
    "array":     "JSON",
    "geojson":   "JSON",
    "geopoint":  "STRING",
    "year":      "INT64",
    "yearmonth": "STRING",
    "any":       "STRING",
}

_DEFAULT_BIGQUERY_TYPE = "STRING"


def bigquery_type(frictionless_type: str | None) -> str:
    """Resolve a Frictionless field type to a BigQuery column type.

    Returns `STRING` for unknown or absent types so a new Frictionless
    spec (or a custom dialect extension) doesn't break table creation.
    Strict validation of the schema descriptor itself happens upstream
    at the request boundary.
    """
    if not frictionless_type:
        return _DEFAULT_BIGQUERY_TYPE
    return FRICTIONLESS_TO_BIGQUERY.get(
        frictionless_type, _DEFAULT_BIGQUERY_TYPE
    )


# BigQuery's `ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE` only
# supports a narrow set of widening transitions. Keys are the current
# BigQuery type; values are the set of types the column may be altered
# to without rewriting the table. Anything outside this map needs a
# planned rebuild and is rejected at the request boundary.
#
# Source: BigQuery DDL docs — INT64/NUMERIC may widen to wider numeric
# types; DATE may widen to DATETIME/TIMESTAMP. No string/JSON/bool
# transitions are supported.
BIGQUERY_ALLOWED_TYPE_CHANGES: dict[str, set[str]] = {
    "INT64":   {"NUMERIC", "BIGNUMERIC", "FLOAT64"},
    "NUMERIC": {"BIGNUMERIC", "FLOAT64"},
    "DATE":    {"DATETIME", "TIMESTAMP"},
}


def can_widen(old_bq_type: str, new_bq_type: str) -> bool:
    """Return True iff BigQuery accepts an in-place `ALTER COLUMN SET
    DATA TYPE` from `old_bq_type` to `new_bq_type`.

    Identity (no change) is trivially allowed.
    """
    if old_bq_type == new_bq_type:
        return True
    return new_bq_type in BIGQUERY_ALLOWED_TYPE_CHANGES.get(old_bq_type, set())


# Inverse of `FRICTIONLESS_TO_BIGQUERY` — used when reading a result
# schema back from BigQuery (e.g. `datastore_search_sql`) and surfacing
# it to clients as Frictionless types. Many-to-one collapses some BQ
# precision distinctions (NUMERIC / BIGNUMERIC / FLOAT64 → number).
BIGQUERY_TO_FRICTIONLESS: dict[str, str] = {
    "INT64":      "integer",
    "INTEGER":    "integer",
    "FLOAT64":    "number",
    "FLOAT":      "number",
    "NUMERIC":    "number",
    "BIGNUMERIC": "number",
    "BOOL":       "boolean",
    "BOOLEAN":    "boolean",
    "STRING":     "string",
    "BYTES":      "string",
    "DATE":       "date",
    "TIME":       "time",
    "DATETIME":   "datetime",
    "TIMESTAMP":  "datetime",
    "JSON":       "object",
}


def frictionless_type_from_bigquery(bq_type: str | None) -> str:
    """Map a BigQuery column type back to a Frictionless type name.

    Unknown or absent types collapse to `string` so a newer BigQuery
    type (e.g. `RANGE`) is still surfaced as a usable column instead
    of breaking the response.
    """
    if not bq_type:
        return "string"
    return BIGQUERY_TO_FRICTIONLESS.get(bq_type.upper(), "string")
