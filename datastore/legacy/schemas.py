"""Pydantic schemas used ONLY by the legacy CKAN surface.

DEPRECATED — see `datastore/legacy/__init__.py`.

Lives here (not `datastore/schemas/request.py`) so the v2 request
schemas don't carry back-compat subclasses after legacy is removed.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import PrivateAttr, model_validator

from datastore.schemas.request import DatastoreSearchSQLRequest


class DatastoreLegacySearchSQLRequest(DatastoreSearchSQLRequest):
    """Legacy-API variant of `datastore_search_sql`.

    Real CKAN's `datastore_search_sql` accepts SQL without a LIMIT clause —
    customer scripts in the wild rely on that. Flip `_REQUIRE_LIMIT` off
    so a bare `SELECT * FROM "<rid>"` parses; every other guard (SELECT-
    only, single statement, function allow-list, per-table authorize) is
    inherited unchanged.
    """

    _REQUIRE_LIMIT: ClassVar[bool] = False

    _has_select_star: bool = PrivateAttr(default=False)

    @property
    def has_select_star(self) -> bool:
        """True when the top-level projection contains `*` or `t.*`.

        CKAN emits the synthesised `_full_text` column only when the
        caller's SELECT list expands to the whole row (via `*`); the
        legacy writer gates its `_full_text` injection on this flag.
        """
        return self._has_select_star

    @model_validator(mode="after")
    def _detect_select_star(self) -> "DatastoreLegacySearchSQLRequest":
        """`_full_text` only exists as a column on the underlying
        datastore table, so emit it only when the outer `SELECT *`
        is reading from a real table — not from a CTE / subquery /
        join where the inner projection may have dropped it, and not
        when the only `*` is the argument to an aggregate like
        `count(*)` (whole-row count, not whole-row projection).
        """
        import sqlglot
        from sqlglot import expressions as exp

        try:
            tree = sqlglot.parse_one(self.sql, dialect="postgres")
        except Exception:
            return self
        select = tree.find(exp.Select)
        if select is None:
            return self
        # A projection item is `SELECT *` only when the item itself is
        # the Star (bare `*`) or a `Column` whose `this` is the Star
        # (qualified `t.*`). A Star appearing inside a function call
        # (`count(*)`) is an aggregate argument, not a whole-row expansion.
        def _is_projection_star(expr: exp.Expression) -> bool:
            if isinstance(expr, exp.Star):
                return True
            if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                return True
            return False

        has_star = any(_is_projection_star(expr) for expr in select.expressions)
        from_clause = select.args.get("from_")
        source = from_clause.this if from_clause else None
        cte_aliases = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
        reads_real_table = (
            isinstance(source, exp.Table)
            and source.name
            and source.name not in cte_aliases
            and not select.args.get("joins")
        )
        self._has_select_star = has_star and reads_real_table
        return self
