"""Unit tests for `services.sql_labels` — the shared helper that
renames BigQuery's `f<N>_` auto-labels to Postgres-style function
names across the v2 and legacy `datastore_search_sql` paths."""

from __future__ import annotations

import pytest
from datastore.services.sql_labels import (
    rename_generic_sql_labels,
    stringified_positions,
    stringify_cells_at,
)

# rename_generic_sql_labels -------------------------------------------------


@pytest.mark.parametrize("key", ["id", "name"])
def test_rename_rewrites_f_prefixed_label_at_unaliased_function(key: str) -> None:
    """Works for both the legacy `{id, type}` and the Frictionless
    `{name, type}` field shapes."""
    fields = [{key: "f0_", "type": "integer"}]
    out = rename_generic_sql_labels(fields, 'SELECT count(*) FROM "t"', key=key)
    assert out == [{key: "count", "type": "integer"}]


def test_rename_covers_every_unaliased_aggregate_position() -> None:
    fields = [
        {"id": "f0_", "type": "integer"},
        {"id": "f1_", "type": "number"},
        {"id": "f2_", "type": "number"},
        {"id": "f3_", "type": "integer"},
        {"id": "f4_", "type": "integer"},
    ]
    out = rename_generic_sql_labels(
        fields,
        'SELECT count(*), sum("x"), avg("y"), min("y"), max("y") FROM "t"',
        key="id",
    )
    assert [f["id"] for f in out] == ["count", "sum", "avg", "min", "max"]


def test_rename_skips_aliased_expression() -> None:
    """Explicit `AS <alias>` means BigQuery already uses that label —
    the rename must not overwrite it."""
    fields = [{"id": "tally", "type": "integer"}]
    out = rename_generic_sql_labels(fields, 'SELECT count(*) AS tally FROM "t"', key="id")
    assert out == fields


def test_rename_preserves_non_generic_ids() -> None:
    fields = [
        {"id": "product_code", "type": "text"},
        {"id": "f0_", "type": "integer"},
    ]
    out = rename_generic_sql_labels(
        fields,
        'SELECT "product_code", count(*) FROM "t"',
        key="id",
    )
    assert [f["id"] for f in out] == ["product_code", "count"]


def test_rename_leaves_plain_columns_and_arithmetic_untouched() -> None:
    """Non-function expressions (columns, `x + 1`, literals) aren't at
    positions that contribute a rename, so their engine labels pass
    through — the loop skips any position whose SELECT-list item isn't
    an unaliased `exp.Func`."""
    fields = [
        {"id": "a", "type": "text"},
        {"id": "f0_", "type": "integer"},  # BigQuery's label for `a + 1`
        {"id": "f1_", "type": "integer"},  # BigQuery's label for `42`
    ]
    out = rename_generic_sql_labels(
        fields, 'SELECT "a", "a" + 1, 42 FROM "t"', key="id"
    )
    # Neither `a + 1` nor `42` is a Func, so both `f<N>_` labels survive.
    assert [f["id"] for f in out] == ["a", "f0_", "f1_"]


def test_rename_is_idempotent() -> None:
    fields = [{"id": "count", "type": "int4"}]
    out = rename_generic_sql_labels(fields, 'SELECT count(*) FROM "t"', key="id")
    assert out == fields


def test_rename_unparseable_sql_returns_fields_unchanged() -> None:
    fields = [{"id": "f0_", "type": "integer"}]
    assert rename_generic_sql_labels(fields, ")) not sql ((", key="id") == fields


# stringified_positions / stringify_cells_at --------------------------------


def test_stringified_positions_picks_count_sum_avg_only() -> None:
    """`count`, `sum`, `avg` return Postgres bigint / numeric → stringified
    in CKAN classic. `min` / `max` preserve the input type → not
    stringified."""
    positions = stringified_positions(
        'SELECT count(*), sum("x"), avg("x"), min("x"), max("x") FROM "t"'
    )
    assert positions == frozenset({0, 1, 2})


def test_stringified_positions_still_counts_aliased_calls() -> None:
    """An alias doesn't change the function's return type, so the cell
    is still stringified."""
    positions = stringified_positions('SELECT count(*) AS tally FROM "t"')
    assert positions == frozenset({0})


def test_stringified_positions_ignores_non_aggregate_expressions() -> None:
    positions = stringified_positions('SELECT "a", "a" + 1 FROM "t"')
    assert positions == frozenset()


def test_stringify_cells_at_wraps_only_listed_positions() -> None:
    rows = iter([(1, 2, 3, 4, 5), (10, 20, 30, 40, 50)])

    out = list(stringify_cells_at(rows, frozenset({0, 2, 4})))

    assert out == [
        ("1", 2, "3", 4, "5"),
        ("10", 20, "30", 40, "50"),
    ]


def test_stringify_cells_at_passes_none_through() -> None:
    out = list(stringify_cells_at(iter([(None, 2)]), frozenset({0})))
    assert out == [(None, 2)]


def test_stringify_cells_at_empty_positions_is_noop() -> None:
    rows = [(1, 2), (3, 4)]
    out = list(stringify_cells_at(iter(rows), frozenset()))
    assert out == rows
