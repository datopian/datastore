"""CKAN-classic shape helpers for `datastore_search_sql` responses.

`rename_generic_sql_labels` rewrites BigQuery's `f<N>_` auto-labels
back to Postgres-style function names (`count`, `sum`, `avg`, `min`,
`max`, …), shared by the v2 and legacy endpoints so a CKAN-classic
client sees `{"id": "count"}` for `SELECT count(*)`.

`stringified_positions` + `stringify_cells_at` mark and stringify the
`count` / `sum` / `avg` cells for the legacy endpoint only — real
CKAN's `psycopg2` returns Postgres `bigint` and `numeric` as strings
so JavaScript clients don't lose precision; `min` / `max` preserve
the input type and stay as JSON numbers.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any


def rename_generic_sql_labels(
    fields: list[dict[str, Any]], sql: str, *, key: str = "id"
) -> list[dict[str, Any]]:
    """Rewrite each engine-emitted `f<N>_` field id to the function name
    at that SELECT-list position. Unaliased function calls contribute;
    aliased expressions and non-function positions pass through. `key`
    is the field-identifier key (`"id"` legacy, `"name"` Frictionless).
    Idempotent — a field already carrying the function name is left
    alone.
    """
    import sqlglot
    from sqlglot import expressions as exp

    try:
        select = sqlglot.parse_one(sql, dialect="postgres").find(exp.Select)
    except Exception:
        return fields
    if select is None:
        return fields

    out = list(fields)
    for pos, e in enumerate(select.expressions):
        if pos >= len(out) or isinstance(e, exp.Alias) or not isinstance(e, exp.Func):
            continue
        label = out[pos].get(key, "")
        # `f<N>_` has at least three chars (`f`, one digit, `_`); the
        # two-end check is enough because we've already proved this
        # position is an unaliased function call, so a user column
        # wouldn't sit here.
        if (
            isinstance(label, str)
            and len(label) >= 3
            and label.startswith("f")
            and label.endswith("_")
        ):
            out[pos] = {**out[pos], key: e.key}
    return out


# SELECT-list functions whose result Postgres (and therefore CKAN
# classic's `datastore_search_sql`) returns as a string in JSON:
# `count` / `sum` return `bigint`, `avg` returns `numeric`, both of
# which `psycopg2` stringifies to avoid losing precision in JavaScript
# clients. `min` / `max` preserve the input type, so a `min(int4)`
# comes back as a JSON number — not stringified. Listed in lowercase
# because `sqlglot`'s `Func.key` is lowercase.
_STRINGIFIED_FUNC_NAMES: frozenset[str] = frozenset({"count", "sum", "avg"})


def stringified_positions(sql: str) -> frozenset[int]:
    """SELECT-list positions whose returned value must be stringified to
    match CKAN classic's wire shape — the `count` / `sum` / `avg` calls
    (aliased or not; the alias doesn't change the return type). `min` /
    `max` and everything else return `frozenset()`."""
    import sqlglot
    from sqlglot import expressions as exp

    try:
        select = sqlglot.parse_one(sql, dialect="postgres").find(exp.Select)
    except Exception:
        return frozenset()
    if select is None:
        return frozenset()
    out: set[int] = set()
    for i, e in enumerate(select.expressions):
        inner = e.this if isinstance(e, exp.Alias) else e
        if isinstance(inner, exp.Func) and inner.key in _STRINGIFIED_FUNC_NAMES:
            out.add(i)
    return frozenset(out)


def stringify_cells_at(
    records: Iterator[tuple[Any, ...]], positions: frozenset[int]
) -> Iterator[tuple[Any, ...]]:
    """Wrap `records` so every cell at a position in `positions` is
    yielded as its decimal `str`. `None` passes through. No-op when
    `positions` is empty (streams the input iterator unchanged)."""
    if not positions:
        yield from records
        return
    for row in records:
        yield tuple(
            str(v) if i in positions and v is not None else v
            for i, v in enumerate(row)
        )
