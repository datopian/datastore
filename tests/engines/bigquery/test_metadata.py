"""Unit tests for the BigQuery native-metadata helpers in `lib.py`.

The engine stores the Frictionless schema in the table-level
`description` (under a `datastore` key) rather than a side table, so
these tests pin the encode → decode round-trip, the DDL/ALTER OPTIONS
shape, and the fall-back to BQ-column inference for unmanaged tables.
No real BigQuery is contacted — table objects are lightweight stubs.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from datastore.infrastructure.engines.bigquery.lib import (
    DATASTORE_KEY,
    normalize_pk,
    set_table_options_sql,
    table_options_clause,
    table_to_schema,
)


def _table(description: str | None = None, columns: list[tuple[str, str]] | None = None) -> Any:
    """A `bigquery.Table` stand-in carrying `description` + `schema`."""
    schema = [
        SimpleNamespace(name=name, field_type=ftype)
        for name, ftype in (columns or [])
    ]
    return SimpleNamespace(description=description, schema=schema)


# --- OPTIONS round-trip ----------------------------------------------------


def test_table_options_clause_round_trips_schema_without_system_fields() -> None:
    schema = {
        "fields": [
            {"name": "_id", "type": "integer"},
            {"name": "auction_id", "type": "integer"},
            {"name": "product_code", "type": "string"},
        ],
        "primaryKey": ["auction_id"],
    }
    clause = table_options_clause(schema)

    assert clause.startswith(" OPTIONS(description = '")
    assert 'labels = [("datastore_managed", "true")]' in clause

    # Recover the JSON description and confirm the schema round-trips
    # with system columns stripped.
    desc = clause.split("description = '", 1)[1].rsplit("', labels", 1)[0]
    payload = json.loads(desc.replace("\\'", "'").replace("\\\\", "\\"))
    stored = payload[DATASTORE_KEY]["schema"]
    assert stored["primaryKey"] == ["auction_id"]
    assert [f["name"] for f in stored["fields"]] == ["auction_id", "product_code"]
    assert table_to_schema(_table(description=desc)) == stored


def test_set_table_options_sql_emits_alter_with_managed_label() -> None:
    sql = set_table_options_sql("`p.d.r`", {"fields": [{"name": "a", "type": "string"}]})
    assert sql.startswith("ALTER TABLE `p.d.r` SET OPTIONS(description = '")
    assert 'labels = [("datastore_managed", "true")]' in sql


# --- table_to_schema -------------------------------------------------------


def test_table_to_schema_returns_stored_block_verbatim() -> None:
    stored = {"fields": [{"name": "x", "type": "number", "info": {"unit": "MWh"}}]}
    desc = json.dumps({DATASTORE_KEY: {"schema_version": 1, "schema": stored}})
    assert table_to_schema(_table(description=desc)) == stored


def test_table_to_schema_infers_from_columns_for_unmanaged_table() -> None:
    table = _table(columns=[("_id", "INT64"), ("name", "STRING"), ("age", "INT64")])
    schema = table_to_schema(table)
    # System columns dropped; BQ field types mapped to canonical
    # Frictionless names so downstream filter type maps understand them.
    assert schema == {
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "integer"},
        ]
    }


def test_table_to_schema_ignores_malformed_description() -> None:
    assert table_to_schema(_table(description="not json", columns=[("a", "STRING")])) == {
        "fields": [{"name": "a", "type": "string"}]
    }


# --- normalize_pk ----------------------------------------------------------


def test_normalize_pk_handles_str_list_and_missing() -> None:
    assert normalize_pk({"primaryKey": "id"}) == ["id"]
    assert normalize_pk({"primaryKey": ["a", "b"]}) == ["a", "b"]
    assert normalize_pk({}) == []
    assert normalize_pk({"primaryKey": None}) == []
