"""PostgreSQL → BigQuery SQL translator for `datastore_search_sql`.

Many customer scripts in the wild were written against a Postgres-backed
CKAN datastore. When the backend was swapped to BigQuery those scripts
started 400ing on dialect quirks that are harmless on Postgres but
illegal on BigQuery — double-quoted identifiers, `"col"::numeric` casts,
`to_char(..., 'YYYY-MM-DD')`, `CURRENT_DATE - 1`, `INTERVAL '10 days'`.

This module does one job: take the user's SQL and return a BigQuery-
compatible version plus a list of plain-English notes about what was
rewritten. Both the v2 and legacy `datastore_search_sql` paths flow
through [bigquery/lib.py](lib.py)'s `qualify_table_refs`, which invokes
the translator first — so there is one translation point for both APIs.

Design notes
------------
- **One sqlglot pass** (`transpile(read='postgres', write='bigquery')`)
  handles most pg-isms (identifier quoting, `::` casts, `to_char`,
  `INTERVAL '10 days'`, window functions, …). Verified against a real
  customer query log before this landed.
- **One targeted AST rewrite** fixes `<date_expr> ± <int-literal>`
  (e.g. `CURRENT_DATE - 1`) → `DATE_SUB(..., INTERVAL n DAY)`. sqlglot
  leaves integer-day arithmetic unchanged and BigQuery rejects it;
  this is the only hole big enough to break real traffic.
- **Transparent**: `translate_pg_to_bq` returns `(sql, notes)` where
  `notes` is a human-readable list of rewrites. Callers can log it; the
  engine logs at DEBUG when the SQL changes. Grep `notes` to see every
  translation path exercised by production.
- **Reversible kill switch**: `translate_pg_to_bq(..., enabled=False)`
  short-circuits to `(sql, [])`. Wired to `Config.BIGQUERY_TRANSLATE_PG_SQL`
  so operators can flip it off per environment without a code change.
- **Fail soft**: if sqlglot can't parse the input, return the original
  string. The downstream parser (`qualify_table_refs`) raises its own
  error; we don't want to turn a parse failure into a translation
  failure the user can't action.
"""

from __future__ import annotations


def translate_pg_to_bq(sql: str, *, enabled: bool = True) -> tuple[str, list[str]]:
    """Rewrite Postgres-dialect `sql` into BigQuery-dialect SQL.

    Returns `(translated_sql, notes)` where `notes` is a list of
    human-readable strings describing each rewrite that was applied
    (empty when the SQL was already BigQuery-compatible or the kill
    switch is off). Never raises — a parse failure returns `(sql, [])`
    so the downstream BigQuery parse surfaces the real error.
    """
    if not enabled or not sql or not sql.strip():
        return sql, []

    import sqlglot
    from sqlglot import expressions as exp

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql, []

    notes: list[str] = []

    # Rewrite `<date_expr> ± <int-literal>` → DATE_SUB/ADD with INTERVAL.
    # sqlglot's postgres→bigquery transpile does NOT fix this on its own
    # (verified against sqlglot 25.x) and it is the one hole we have seen
    # in real customer traffic. Target the specific shape so we don't
    # perturb unrelated integer arithmetic (e.g. a numeric column minus 1).
    _rewrite_date_integer_arithmetic(tree, notes, exp=exp)

    # Pg allows `"timestamp_col" > CURRENT_DATE` through implicit coercion;
    # BigQuery doesn't ("No matching signature for operator > for argument
    # types: TIMESTAMP, DATE"). When one side of a comparison is a bare
    # column reference and the other is a DATE-returning expression, wrap
    # the DATE side in `TIMESTAMP(...)` so BQ sees TIMESTAMP vs TIMESTAMP.
    # Runs after the arithmetic rewrite so DATE_SUB(...) from it gets
    # wrapped too — the single shape `"col" > CURRENT_DATE - 1` ends up as
    # `col > TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY))`.
    _wrap_date_vs_column_comparison(tree, notes, exp=exp)

    # Pg's `EXTRACT(EPOCH FROM a - b)` returns a numeric count of seconds
    # between two timestamps; BQ has no EPOCH extraction and no interval-
    # typed subtraction, so this is `TIMESTAMP_DIFF(a, b, SECOND)`.
    _rewrite_extract_epoch(tree, notes, exp=exp)


    try:
        translated = tree.sql(dialect="bigquery")
    except Exception:
        return sql, []

    # Pg's `::timestamp` is semantically closer to BQ `DATETIME` (zone-less
    # civil time), which is what sqlglot picks on write. But the CKAN
    # BigQuery datastore backend creates `timestamp`-typed columns as BQ
    # `TIMESTAMP` (zone-aware), so a predicate like `"ts_col" > CAST('…' AS
    # DATETIME)` is a type mismatch at execution time. Retarget every
    # `CAST(... AS DATETIME)` the writer emitted back to `CAST(... AS
    # TIMESTAMP)` so comparisons stay legal. Done post-emit because the
    # DATETIME shows up on the write side, not in the parsed AST.
    translated, retargeted = _retarget_datetime_text(translated)
    if retargeted:
        notes.append(
            f"retargeted {retargeted} `CAST(... AS DATETIME)` to "
            "`CAST(... AS TIMESTAMP)` to match the backend's `TIMESTAMP` column type"
        )

    # Fix the fallout of sqlglot's `to_timestamp(ms/1000)` → `TIMESTAMP_SECONDS(ms/1000)`
    # mapping: BigQuery's `/` operator returns FLOAT64, but TIMESTAMP_SECONDS
    # requires INT64. Customer scripts carry the `to_timestamp(epoch_ms/1000)`
    # idiom from pg; we collapse it to the direct `TIMESTAMP_MILLIS(epoch_ms)`
    # equivalent to sidestep the type issue entirely. Runs post-emit because
    # TIMESTAMP_SECONDS only appears after sqlglot's bigquery writer stage.
    translated, ts_fixes = _fix_timestamp_seconds_text(translated)
    if ts_fixes:
        notes.append(
            f"converted {ts_fixes} `TIMESTAMP_SECONDS(x / 1000)` call(s) to "
            "`TIMESTAMP_MILLIS(x)` so BigQuery accepts the INT64 argument"
        )

    # Identifier quoting: pg uses `"ident"`, bq uses backticks. sqlglot
    # does this on the write side; attribute it in `notes` so a reader
    # of the log sees why identifiers changed.
    if '"' in sql and "`" in translated and "`" not in sql:
        notes.append("quoted identifiers rewritten from `\"ident\"` to backticks")

    if "::" in sql and "::" not in translated:
        notes.append("`::type` casts rewritten to `CAST(... AS type)`")

    # Only report "sql dialect translated" (the catch-all) if the string
    # actually changed and nothing more specific has been reported — a
    # pure whitespace reformat doesn't count as a translation.
    if _normalize(translated) != _normalize(sql) and not notes:
        notes.append("sql dialect translated (postgres → bigquery)")

    return translated, notes


def _rewrite_date_integer_arithmetic(tree, notes: list[str], *, exp) -> None:
    """In-place AST rewrite: `<date-ish> ± <int-literal>` → DATE_SUB/ADD.

    BigQuery rejects `CURRENT_DATE - 1` with "No matching signature for
    operator - for argument types: DATE, INT64." Customer scripts rely on
    it heavily (`WHERE deliveryStart > CURRENT_DATE - 1`), so we rewrite
    just that shape. We match on the LEFT side being a date-returning
    expression (CURRENT_DATE/CURRENT_DATETIME/CURRENT_TIMESTAMP or an
    explicit DATE()/DATETIME()/TIMESTAMP() cast) and the RIGHT being a
    bare integer literal — nothing else is touched, so a column of type
    INT64 minus 1 keeps its original meaning.
    """
    _DATE_FUNC_NAMES = {
        "CurrentDate",
        "CurrentDatetime",
        "CurrentTimestamp",
        "Date",
        "Datetime",
        "Timestamp",
    }

    def is_date_expr(node) -> bool:
        return type(node).__name__ in _DATE_FUNC_NAMES

    def is_int_literal(node) -> bool:
        return isinstance(node, exp.Literal) and node.is_int

    rewrites = 0
    for node in list(tree.find_all(exp.Sub, exp.Add)):
        left = node.this
        right = node.expression
        if not (is_date_expr(left) and is_int_literal(right)):
            continue
        interval = exp.Interval(
            this=exp.Literal.number(right.this),
            unit=exp.Var(this="DAY"),
        )
        func_name = "DATE_SUB" if isinstance(node, exp.Sub) else "DATE_ADD"
        replacement = exp.Anonymous(this=func_name, expressions=[left.copy(), interval])
        node.replace(replacement)
        rewrites += 1

    if rewrites:
        notes.append(
            f"rewrote {rewrites} date±integer expression(s) to "
            "`DATE_SUB/ADD(..., INTERVAL n DAY)`"
        )


def _wrap_date_vs_column_comparison(tree, notes: list[str], *, exp) -> None:
    """In-place AST rewrite: `<col> <op> <date_expr>` → `<col> <op> TIMESTAMP(<date_expr>)`.

    BigQuery rejects `TIMESTAMP > DATE` comparisons strictly. Customer SQL
    carried over from Postgres relies on pg's implicit coercion
    (`WHERE deliveryStart > CURRENT_DATE`, `WHERE delivery_date >= DATE '2024-01-01'`,
    `WHERE col BETWEEN CURRENT_DATE AND CURRENT_DATE + 1`). We don't know
    the column's actual BQ type at translation time, but wrapping the DATE
    side in `TIMESTAMP(...)` is safe for both TIMESTAMP and DATETIME column
    types, and is a no-op if the column really is a DATE (BQ accepts
    `DATE > TIMESTAMP` via its own coercion in that direction).

    Trigger conditions:
      - A comparison node (`>`, `>=`, `<`, `<=`, `=`, `!=`), or a BETWEEN.
      - One side is an `exp.Column` (a bare quoted identifier).
      - The other side is a DATE-returning expression we recognise.

    Non-triggers (left untouched to avoid perturbing legitimate SQL):
      - Both sides are columns.
      - The non-column side is already a TIMESTAMP/DATETIME expression.
      - The non-column side is a function result we can't classify.
    """
    _DATE_PRODUCING = {
        "CurrentDate",       # CURRENT_DATE
        "Date",              # DATE('...'), DATE(col)
    }
    _ALREADY_TIMESTAMP = {
        "CurrentTimestamp",
        "CurrentDatetime",
        "Timestamp",         # TIMESTAMP(...) wrapper, or ::timestamp cast
        "Datetime",
        "StrToTime",         # to_timestamp(...)
        "Cast",              # ::timestamp / ::datetime (checked below)
    }

    def is_date_producing(node) -> bool:
        name = type(node).__name__
        if name in _DATE_PRODUCING:
            return True
        # `DATE_SUB(x, INTERVAL n DAY)` / `DATE_ADD` get emitted by the
        # earlier arithmetic rewrite; they are `exp.Anonymous`.
        if isinstance(node, exp.Anonymous):
            func_name = (node.this or "").upper()
            if func_name in ("DATE_SUB", "DATE_ADD"):
                return True
        # `DATE 'literal'` and a cast to DATE.
        if isinstance(node, exp.Cast):
            to = node.args.get("to")
            if to is not None and str(to).strip().upper() == "DATE":
                return True
        # `CURRENT_DATE + INTERVAL '2 day'` — sqlglot models this as
        # exp.Add(this=<date expr>, expression=exp.Interval). Pg accepts it,
        # BQ needs it wrapped; we recurse on the date-side so a nested
        # `CURRENT_DATE + 1 + INTERVAL '…'` chain still classifies.
        if isinstance(node, (exp.Add, exp.Sub)):
            other = node.expression
            if isinstance(other, exp.Interval) and is_date_producing(node.this):
                return True
        return False

    def is_already_timestamp(node) -> bool:
        if type(node).__name__ in _ALREADY_TIMESTAMP:
            # For a Cast, only "already timestamp" when the target type says so.
            if isinstance(node, exp.Cast):
                to = node.args.get("to")
                return to is not None and str(to).strip().upper() in (
                    "TIMESTAMP", "DATETIME", "TIMESTAMPTZ"
                )
            return True
        return False

    def wrap(node):
        return exp.Anonymous(this="TIMESTAMP", expressions=[node.copy()])

    rewrites = 0
    comparison_types = (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.NEQ)

    for cmp_node in list(tree.find_all(*comparison_types)):
        left = cmp_node.this
        right = cmp_node.expression
        if left is None or right is None:
            continue

        # Only act when exactly one side is a bare column reference.
        left_is_col = isinstance(left, exp.Column)
        right_is_col = isinstance(right, exp.Column)
        if left_is_col == right_is_col:
            continue

        col_side, other_side = (left, right) if left_is_col else (right, left)
        if is_already_timestamp(other_side):
            continue
        if not is_date_producing(other_side):
            continue

        if left_is_col:
            cmp_node.set("expression", wrap(other_side))
        else:
            cmp_node.set("this", wrap(other_side))
        rewrites += 1

    # BETWEEN gets its own AST node and a `low`/`high` pair. Same shape
    # argument as above — a column BETWEEN two DATE expressions wants both
    # bounds wrapped.
    for between in list(tree.find_all(exp.Between)):
        subject = between.this
        if not isinstance(subject, exp.Column):
            continue
        for key in ("low", "high"):
            bound = between.args.get(key)
            if bound is None or is_already_timestamp(bound) or not is_date_producing(bound):
                continue
            between.set(key, wrap(bound))
            rewrites += 1

    if rewrites:
        notes.append(
            f"wrapped {rewrites} DATE expression(s) in `TIMESTAMP(...)` "
            "to match TIMESTAMP column comparisons"
        )


import re as _re  # noqa: E402

# Post-emit text rewrite: `TIMESTAMP_SECONDS(<expr> / 1000)` → `TIMESTAMP_MILLIS(<expr>)`.
# `<expr>` is the shortest run of chars that balances parens; we restrict to a
# single argument (no commas) and no quotes so we never accidentally splice
# through a string literal. The `/ 1000` divisor is matched tightly — only
# bare `1000`, not `1000.0` or variables — because that is the specific pg
# idiom (`to_timestamp(epoch_ms/1000)`) we are compensating for.
_TIMESTAMP_SECONDS_MS_RE = _re.compile(
    r"\bTIMESTAMP_SECONDS\(\s*(?P<expr>[^(),'\"]+?)\s*/\s*1000\s*\)",
    _re.IGNORECASE,
)


def _fix_timestamp_seconds_text(translated: str) -> tuple[str, int]:
    """Collapse the `TIMESTAMP_SECONDS(x / 1000)` idiom to `TIMESTAMP_MILLIS(x)`.

    BigQuery `/` returns FLOAT64 whereas TIMESTAMP_SECONDS wants INT64, so
    the literal `TIMESTAMP_SECONDS(x / 1000)` the writer emits for pg's
    `to_timestamp(epoch_ms/1000)` always fails. TIMESTAMP_MILLIS takes the
    numerator directly in INT64 and matches the intended semantics exactly.

    Rewrite is applied only outside single-quoted string literals so a
    column aliased with the literal text `TIMESTAMP_SECONDS(x / 1000)`
    stays verbatim.
    """
    out_parts: list[str] = []
    count = 0
    pos = 0
    n = len(translated)
    while pos < n:
        ch = translated[pos]
        # Walk past a single-quoted string literal untouched. Pg accepts
        # `''` as an embedded single quote; BQ also accepts `\'`.
        if ch == "'":
            end = pos + 1
            while end < n:
                if translated[end] == "\\" and end + 1 < n:
                    end += 2
                    continue
                if translated[end] == "'":
                    if end + 1 < n and translated[end + 1] == "'":
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            out_parts.append(translated[pos:end])
            pos = end
            continue
        # In code: look for the next string literal or the end, and run
        # the regex only within that span.
        next_quote = translated.find("'", pos)
        span_end = n if next_quote == -1 else next_quote
        chunk = translated[pos:span_end]
        new_chunk, c = _TIMESTAMP_SECONDS_MS_RE.subn(
            lambda m: f"TIMESTAMP_MILLIS({m.group('expr').strip()})",
            chunk,
        )
        out_parts.append(new_chunk)
        count += c
        pos = span_end
    return "".join(out_parts), count

# Post-emit text rewrite: `CAST(expr AS DATETIME)` → `CAST(expr AS TIMESTAMP)`.
# The `AS DATETIME)` suffix is distinctive enough that this is safe against
# the specific SQL shapes sqlglot emits (always UPPER-cased type names, always
# a single space before AS). The pattern won't match an identifier literally
# named "DATETIME" inside a string, since those are either single-quoted or
# backtick-quoted, not bare keywords after an AS.
_DATETIME_CAST_RE = _re.compile(r"\bAS\s+DATETIME\s*\)")


def _retarget_datetime_text(translated: str) -> tuple[str, int]:
    """Flip every CAST(... AS DATETIME) in the emitted SQL to AS TIMESTAMP.

    Returns the rewritten SQL and the number of substitutions made so the
    caller can decide whether to attribute a note.
    """
    def _sub(_m: _re.Match[str]) -> str:
        return "AS TIMESTAMP)"

    new, count = _DATETIME_CAST_RE.subn(_sub, translated)
    return new, count


def _rewrite_extract_epoch(tree, notes: list[str], *, exp) -> None:
    """In-place AST rewrite: `EXTRACT(EPOCH FROM a - b)` → `TIMESTAMP_DIFF(a, b, SECOND)`.

    Pg's EXTRACT(EPOCH FROM interval) returns the interval's seconds as a
    number. BQ has no EPOCH extraction unit, and TIMESTAMP subtraction isn't
    an operator at all — the equivalent is `TIMESTAMP_DIFF(a, b, SECOND)`.

    Covers two shapes seen in real customer traffic:
      - `EXTRACT(EPOCH FROM a - b)` → `TIMESTAMP_DIFF(a, b, SECOND)`
      - `EXTRACT(EPOCH FROM col)`  → `UNIX_SECONDS(col)` (bare timestamp,
        pg's "seconds since epoch for a timestamp" idiom)
    """
    rewrites = 0
    for node in list(tree.find_all(exp.Extract)):
        unit = node.this
        if unit is None:
            continue
        unit_name = (unit.name if hasattr(unit, "name") else str(unit)).upper()
        if unit_name != "EPOCH":
            continue
        src = node.expression
        if src is None:
            continue
        # Unwrap one layer of parentheses that sqlglot models as exp.Paren.
        inner = src.this if isinstance(src, exp.Paren) else src
        if isinstance(inner, exp.Sub):
            left_arg = inner.this
            right_arg = inner.expression
            replacement = exp.Anonymous(
                this="TIMESTAMP_DIFF",
                expressions=[left_arg.copy(), right_arg.copy(), exp.Var(this="SECOND")],
            )
        else:
            replacement = exp.Anonymous(this="UNIX_SECONDS", expressions=[inner.copy()])
        node.replace(replacement)
        rewrites += 1

    if rewrites:
        notes.append(
            f"rewrote {rewrites} `EXTRACT(EPOCH FROM …)` expression(s) to "
            "`TIMESTAMP_DIFF(a, b, SECOND)` / `UNIX_SECONDS(col)`"
        )


def _normalize(sql: str) -> str:
    """Collapse whitespace so a dialect change isn't masked by formatting."""
    return " ".join(sql.split())
