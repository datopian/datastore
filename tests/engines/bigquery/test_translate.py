"""Unit tests for the Postgres→BigQuery SQL translator.

Covers every pg-ism we have seen in real customer traffic — the test
cases are deliberately verbatim copies of queries pulled from a
production job log. See the dialect notes in
`datastore/infrastructure/engines/bigquery/translate.py`.

The translator is the one hook that lets customer scripts carried over
from the Postgres-backed CKAN datastore keep working on BigQuery, so
regressions here would break real `datastore_search_sql` and
`dump/query` calls under both the v2 and legacy API surfaces.
"""

from __future__ import annotations

import pytest
import sqlglot
from datastore.infrastructure.engines.bigquery.lib import qualify_table_refs
from datastore.infrastructure.engines.bigquery.translate import translate_pg_to_bq


def _translate(sql: str) -> tuple[str, list[str]]:
    """Convenience wrapper: enabled by default."""
    return translate_pg_to_bq(sql, enabled=True)


# --- identifier quoting ----------------------------------------------------


def test_double_quoted_identifiers_rewritten_to_backticks() -> None:
    sql = 'SELECT * FROM "my_table" ORDER BY "_id" ASC'
    out, notes = _translate(sql)

    assert "`my_table`" in out
    assert "`_id`" in out
    assert '"my_table"' not in out
    assert any("backticks" in n for n in notes)


def test_bare_identifiers_unchanged() -> None:
    sql = "SELECT * FROM my_table"
    out, notes = _translate(sql)

    assert "my_table" in out
    assert notes == []


# --- ::type cast operator --------------------------------------------------


def test_pg_cast_operator_rewritten_to_cast_function() -> None:
    sql = 'SELECT "MW"::numeric FROM "t"'
    out, notes = _translate(sql)

    assert "CAST(`MW` AS NUMERIC)" in out
    assert "::" not in out
    assert any("::type" in n for n in notes)


# --- to_char ---------------------------------------------------------------


def test_to_char_rewritten_to_format_date() -> None:
    sql = "SELECT to_char(\"delivery_start\", 'YYYY-MM-DD') AS day FROM \"t\""
    out, _ = _translate(sql)

    # sqlglot picks the right FORMAT_* variant for the format mask.
    assert "FORMAT_DATE('%F'" in out or "FORMAT_TIMESTAMP('%F'" in out
    assert "to_char" not in out.lower()


# --- INTERVAL '10 days' ----------------------------------------------------


def test_pg_interval_literal_rewritten() -> None:
    sql = "SELECT * FROM t WHERE d > CURRENT_DATE - INTERVAL '10 days'"
    out, _ = _translate(sql)

    # BigQuery wants `INTERVAL '10' DAY` (quoted value + unit keyword).
    assert "INTERVAL '10' DAY" in out


# --- CURRENT_DATE ± integer-literal arithmetic -----------------------------


def test_current_date_minus_integer_rewritten_to_date_sub() -> None:
    sql = 'SELECT * FROM "t" WHERE "d" > CURRENT_DATE - 1'
    out, notes = _translate(sql)

    assert "DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)" in out
    assert any("date±integer" in n for n in notes)


def test_current_date_plus_integer_rewritten_to_date_add() -> None:
    sql = "SELECT * FROM t WHERE d < CURRENT_DATE + 2"
    out, notes = _translate(sql)

    assert "DATE_ADD(CURRENT_DATE, INTERVAL 2 DAY)" in out
    assert any("date±integer" in n for n in notes)


def test_integer_column_minus_integer_not_treated_as_date_arithmetic() -> None:
    # Pure integer arithmetic on a bare column stays untouched: we only
    # want to rewrite date-returning expressions, not every `- 1`.
    sql = "SELECT * FROM t WHERE col > 5"
    out, notes = _translate(sql)

    assert "col > 5" in out
    assert not any("date±integer" in n for n in notes)


# --- combined / real-world queries from the production log -----------------


def test_production_window_function_with_current_date_minus_one() -> None:
    sql = (
        'SELECT COUNT(*) OVER () AS _count, * FROM "a63ab354" '
        'WHERE "deliveryStart" > Current_Date-1 '
        'AND "auctionUnit" = (\'NONE-0\') '
        'ORDER BY "unitResultID" ASC'
    )
    out, _ = _translate(sql)

    # Pg-isms gone, BigQuery-shaped SQL out.
    assert "DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)" in out
    assert "`deliveryStart`" in out
    assert "`a63ab354`" in out
    assert '"' not in out


def test_production_case_with_pg_cast_round_trips() -> None:
    sql = (
        'SELECT COUNT(*) AS total, '
        'SUM(CASE WHEN "Status" = \'Accepted\' THEN "MW"::numeric ELSE 0 END) '
        'FROM "t"'
    )
    out, _ = _translate(sql)

    assert "CAST(`MW` AS NUMERIC)" in out
    assert "::" not in out


# --- kill switch -----------------------------------------------------------


def test_disabled_returns_sql_unchanged() -> None:
    sql = 'SELECT * FROM "t" WHERE "d" > CURRENT_DATE - 1'
    out, notes = translate_pg_to_bq(sql, enabled=False)

    assert out == sql
    assert notes == []


# --- fail-soft on garbage input -------------------------------------------


def test_unparseable_sql_returns_input_unchanged() -> None:
    sql = "::: not actually sql :::"
    out, notes = _translate(sql)

    assert out == sql
    assert notes == []


def test_empty_sql_returns_input_unchanged() -> None:
    for sql in ("", "   ", "\n\t "):
        out, notes = translate_pg_to_bq(sql)
        assert out == sql
        assert notes == []


# --- production-log regression suite --------------------------------------
#
# Every entry below is a real query pulled verbatim from a customer-traffic
# job log; the top-end entries account for 55M+ requests. The suite runs
# each one through the full `qualify_table_refs` entry point (same code
# path as v2 `datastore_search_sql` and legacy `/api/3/action/datastore_search_sql`)
# and asserts the output parses as BigQuery dialect. If the translator
# ever regresses on one of these shapes, a real customer breaks.

PRODUCTION_SAMPLES: list[tuple[str, str]] = [
    (
        "current_date minus integer (1.9M calls)",
        'select count(*) over () as _count, * from '
        '"a63ab354-7e68-44c2-ad96-c6f920c30e85" '
        'where "deliveryStart" > Current_Date-1 '
        'and "auctionUnit" = (\'NONE-0\') '
        'ORDER BY "unitResultID" ASC',
    ),
    (
        "pg INTERVAL '10 days' literal",
        'SELECT * FROM "13b511df-d6ec-4143-afb1-0ecc6fd19810" '
        "WHERE \"deliveryStart\" > CURRENT_DATE - INTERVAL '10 days'",
    ),
    (
        "current_date + INTERVAL '2 day'",
        'SELECT * FROM "t" '
        'WHERE "deliveryStart" >= CURRENT_DATE '
        "AND \"deliveryStart\" < CURRENT_DATE + INTERVAL '2 day'",
    ),
    (
        "::numeric cast inside CASE (271K calls)",
        'SELECT COUNT(*) AS total, '
        'SUM(CASE WHEN "Status" = \'Accepted\' '
        'THEN "DFS Procured MW"::numeric ELSE 0 END) AS accepted_mw '
        'FROM "3ebf77d7-05df-466e-a023-dc45a90efeea" '
        "WHERE \"Zone\" = '10'",
    ),
    (
        "to_char with GROUP BY alias",
        "SELECT to_char(\"deliveryStart\", 'YYYY-MM-DD') AS day, "
        '"auctionUnit" AS unit, '
        'sum("executedQuantity" * "clearingPrice") AS gbp '
        'FROM "c312358e-87da-480e-88cc-d45c1dce1b41" '
        "WHERE \"technologyType\" = 'Batteries' "
        'GROUP BY day, "auctionUnit"',
    ),
    (
        "string_agg with ORDER BY (sqlglot warns, output still valid)",
        'SELECT COUNT(*) AS event_count, MIN("From_Local") AS earliest, '
        "string_agg(DISTINCT \"DFS Submission Time_Local\", ',' "
        'ORDER BY "DFS Submission Time_Local") AS submissions '
        'FROM "3635fd80" '
        "WHERE \"Delivery Date\" = '2026-10-13'",
    ),
    (
        "trailing semicolon",
        'SELECT "ID", "Date", "Volume" '
        'from "0b7e84ec-eded-4458-b111-d29ba4508e85";',
    ),
    (
        "MAX aggregate with aliased quoted column",
        'SELECT MAX("Forecast_Datetime") AS max_ts '
        'FROM "31861619-0b86-47ba-bac2-d008a760af54"',
    ),
    (
        "window + ORDER BY + WHERE on timestamp literal",
        'SELECT COUNT(*) OVER () AS _count, * '
        'FROM "832e6702-3007-4424-9146-8d35db8b2fe4" '
        "WHERE \"CreatedTime\" > '2026-04-17 21:01:00' "
        'order by "CreatedTime" DESC',
    ),
    (
        "LIMIT + OFFSET with multi-column projection",
        'SELECT "_id", "EntryID", "ServiceType", "AssetID", "MW", '
        '"IndicativePrice", "DispatchDateTimeStamp" '
        'FROM "099bd868-d0af-4157-85c6-4508ddbd8a64" '
        "WHERE \"_id\" >= '944' "
        'ORDER BY "_id" ASC LIMIT 10000 OFFSET 0',
    ),
    (
        "date range on datetime column",
        'SELECT * FROM "6a928369-bed3-445f-af8a-69cdb2cc5089" '
        "WHERE \"Published DateTime\" >= '2026-10-01T00:00:00' "
        "AND \"Published DateTime\" <= '2026-10-01T22:00:00.999Z' "
        'ORDER BY "_id" ASC',
    ),
    (
        "SELECT DATE() with GROUP BY on alias",
        "SELECT DATE(delivery_start) AS delivery_date, product_code, "
        "AVG(clearing_price_gbp_per_mwh) AS avg_price "
        "FROM balancing_auction_results_2025 "
        "WHERE accepted = true "
        "GROUP BY delivery_date, product_code",
    ),
]


@pytest.mark.parametrize(
    "label,sql",
    PRODUCTION_SAMPLES,
    ids=[label for label, _ in PRODUCTION_SAMPLES],
)
def test_production_query_translates_and_parses_as_bigquery(label: str, sql: str) -> None:
    """Each real-world query must survive the full qualify_table_refs
    pipeline AND parse under the BigQuery dialect — i.e. BigQuery itself
    would at least accept the SQL (modulo table existence)."""
    qualified = qualify_table_refs(sql, project="p", dataset="d")
    # Round-trip under the BigQuery dialect — this is the final check
    # that BigQuery's parser would at least recognise the shape.
    sqlglot.parse_one(qualified, dialect="bigquery")

    # Fully-qualified table refs are the whole point of qualify_table_refs;
    # if these disappear the function has been accidentally short-circuited.
    assert "`p`.`d`." in qualified
    # And none of the pg-ism shapes should survive — specifically no
    # `::` cast operator anywhere outside a comment and no `"ident"`
    # (backticks only).
    assert "::" not in qualified
    assert '"' not in qualified


# --- spec-driven acceptance tests ------------------------------------------
#
# One test per compatibility area in the migration spec. These are the
# shapes CKAN clients ship today and need to keep working against the
# BigQuery datastore.


def test_spec_ilike_rewritten_to_lower_like() -> None:
    # Spec #1: ILIKE case-insensitive match.
    sql = (
        'SELECT * FROM "3ebf77d7-05df-466e-a023-dc45a90efeea" '
        "WHERE \"Registered DFS Participant\" ILIKE '%Octopus%'"
    )
    out, _ = _translate(sql)
    # The pattern-matching stays logically equivalent — both sides lower-cased.
    assert "LOWER(`Registered DFS Participant`)" in out
    assert "LIKE LOWER('%Octopus%')" in out
    assert "ILIKE" not in out


def test_spec_current_date_minus_integer_wrapped_in_timestamp() -> None:
    # Spec #2: `col > CURRENT_DATE - 1` has to become a TIMESTAMP comparison.
    sql = (
        'SELECT COUNT(*) OVER () AS _count, * '
        'FROM "a63ab354-7e68-44c2-ad96-c6f920c30e85" '
        'WHERE "deliveryStart" > CURRENT_DATE - 1'
    )
    out, notes = _translate(sql)
    # Exactly the shape the spec cites.
    assert "TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY))" in out
    # The two cooperating rewrites both fire.
    assert any("date±integer" in n for n in notes)
    assert any("TIMESTAMP(...)" in n for n in notes)


def test_spec_to_timestamp_epoch_to_timestamp_seconds() -> None:
    # Spec #3: `to_timestamp(epoch_ms/1000)`. The spec cites TIMESTAMP_SECONDS
    # as the shape; in practice BigQuery's `/` returns FLOAT64 while
    # TIMESTAMP_SECONDS requires INT64, so we collapse `/1000` to the direct
    # `TIMESTAMP_MILLIS(epoch_ms)` equivalent (semantically identical, no
    # type coercion needed).
    sql = (
        'SELECT COUNT(*) OVER () AS _count, * '
        'FROM "6a928369-bed3-445f-af8a-69cdb2cc5089" '
        'WHERE "Published DateTime" >= to_timestamp(1791939450000/1000) '
        'AND "Published DateTime" <= to_timestamp(1791939750000/1000) '
        'ORDER BY "_id" ASC LIMIT 100'
    )
    out, _ = _translate(sql)
    assert "TIMESTAMP_MILLIS(1791939450000)" in out
    assert "TIMESTAMP_MILLIS(1791939750000)" in out
    assert "to_timestamp" not in out.lower()


def test_spec_pg_timestamp_cast_targets_bq_timestamp() -> None:
    # Spec #4: `::timestamp` has to land on `CAST(... AS TIMESTAMP)`, not
    # DATETIME — the backend creates `timestamp` columns as BQ TIMESTAMP
    # and a DATETIME literal would make the comparison a type mismatch.
    sql = (
        'SELECT * FROM "13b511df-d6ec-4143-afb1-0ecc6fd19810" '
        "WHERE \"deliveryStart\" BETWEEN "
        "'2026-08-29T22:00:00'::timestamp AND '2026-08-30T21:30:00'::timestamp"
    )
    out, notes = _translate(sql)
    assert "CAST('2026-08-29T22:00:00' AS TIMESTAMP)" in out
    assert "CAST('2026-08-30T21:30:00' AS TIMESTAMP)" in out
    # And the DATETIME intermediate from sqlglot is nowhere to be seen.
    assert "AS DATETIME)" not in out
    assert any("CAST(... AS TIMESTAMP)" in n for n in notes)


def test_spec_extract_epoch_from_interval_to_timestamp_diff() -> None:
    # Spec #5: `EXTRACT(EPOCH FROM (a - b))` → `TIMESTAMP_DIFF(a, b, SECOND)`.
    sql = (
        'SELECT "auctionProduct" AS p, "serviceType" AS s, COUNT(*) AS n, '
        'SUM("clearedVolume" * "clearingPrice" * '
        'EXTRACT(EPOCH FROM ("deliveryEnd"::timestamp - "deliveryStart"::timestamp)) '
        '/ 3600) AS spend '
        'FROM "be55ee51-b79e-47da-b71e-a0f8865d9d66" '
        'GROUP BY 1, 2 ORDER BY spend DESC'
    )
    out, notes = _translate(sql)
    # The SECOND unit and the two column arguments survive, in order.
    assert "TIMESTAMP_DIFF(" in out
    assert "SECOND)" in out
    # The source operands keep their TIMESTAMP cast too (spec #4 cooperates).
    assert "CAST(`deliveryEnd` AS TIMESTAMP)" in out
    assert "CAST(`deliveryStart` AS TIMESTAMP)" in out
    # No EXTRACT(EPOCH ...) leaks through.
    assert "EPOCH" not in out.upper()
    assert any("EXTRACT(EPOCH" in n for n in notes)


def test_spec_extract_epoch_from_bare_column_uses_unix_seconds() -> None:
    # Companion to spec #5: `EXTRACT(EPOCH FROM col)` with no subtraction
    # is the "seconds since epoch for this timestamp" idiom, which BQ
    # spells `UNIX_SECONDS(col)`.
    sql = 'SELECT EXTRACT(EPOCH FROM "ts") FROM "t"'
    out, _ = _translate(sql)
    assert "UNIX_SECONDS(`ts`)" in out
    assert "EXTRACT" not in out


def test_spec_identifier_quoting_preserves_names() -> None:
    # Spec #6: identifier quoting switches from "ident" to `ident`
    # without mangling the inner name (spaces, UUIDs, mixed case).
    sql = (
        'SELECT "Registered DFS Participant" '
        'FROM "3ebf77d7-05df-466e-a023-dc45a90efeea"'
    )
    out, _ = _translate(sql)
    assert "`Registered DFS Participant`" in out
    assert "`3ebf77d7-05df-466e-a023-dc45a90efeea`" in out
    assert '"' not in out


# --- surgical safety-net tests for the new TIMESTAMP-wrap rewrite ----------


def test_date_wrap_non_date_rhs_untouched() -> None:
    # The wrap must not fire on a column vs integer / string / column
    # comparison — nothing to coerce there.
    for sql in (
        "SELECT * FROM t WHERE col > 5",
        "SELECT * FROM t WHERE col = 'hello'",
        "SELECT * FROM t WHERE a = b",
    ):
        out, notes = _translate(sql)
        assert "TIMESTAMP(" not in out
        assert not any("TIMESTAMP(...)" in n for n in notes)


def test_date_wrap_already_timestamp_rhs_untouched() -> None:
    # CURRENT_TIMESTAMP is already TIMESTAMP-typed on BQ, so no wrap needed.
    sql = "SELECT * FROM t WHERE col > CURRENT_TIMESTAMP"
    out, notes = _translate(sql)
    assert "TIMESTAMP(CURRENT_TIMESTAMP" not in out
    assert not any("TIMESTAMP(...)" in n for n in notes)


def test_date_wrap_between_wraps_both_bounds() -> None:
    sql = (
        "SELECT * FROM t WHERE col BETWEEN "
        "CURRENT_DATE AND CURRENT_DATE + INTERVAL '2 day'"
    )
    out, notes = _translate(sql)
    assert "TIMESTAMP(CURRENT_DATE)" in out
    assert "TIMESTAMP(CURRENT_DATE + INTERVAL '2' DAY)" in out
    assert any("wrapped 2" in n for n in notes)


def test_extract_non_epoch_unit_untouched() -> None:
    # Only the EPOCH unit is rewritten; YEAR, DAY, etc. are legitimate on BQ.
    sql = 'SELECT EXTRACT(YEAR FROM "ts") FROM t'
    out, notes = _translate(sql)
    assert "EXTRACT(YEAR FROM `ts`)" in out
    assert not any("EXTRACT(EPOCH" in n for n in notes)


def test_non_datetime_cast_untouched_by_retarget() -> None:
    # The DATETIME→TIMESTAMP retarget must not touch INT64, STRING, …
    sql = 'SELECT CAST("x" AS INT64), CAST("y" AS STRING) FROM t'
    out, notes = _translate(sql)
    assert "CAST(`x` AS INT64)" in out
    assert "CAST(`y` AS STRING)" in out
    assert not any("DATETIME" in n for n in notes)


# --- TIMESTAMP_SECONDS(x/1000) → TIMESTAMP_MILLIS(x) -----------------------


def test_to_timestamp_ms_div_1000_collapsed_to_timestamp_millis() -> None:
    # Pg's `to_timestamp(epoch_ms/1000)` would land on `TIMESTAMP_SECONDS(x/1000)`
    # which BQ rejects (FLOAT64 argument, needs INT64). Collapse to the
    # millisecond equivalent so the type is INT64 and semantics match.
    sql = 'SELECT * FROM "t" WHERE "ts" >= to_timestamp(1790805660000/1000)'
    out, notes = _translate(sql)
    assert "TIMESTAMP_MILLIS(1790805660000)" in out
    assert "TIMESTAMP_SECONDS" not in out
    assert any("TIMESTAMP_MILLIS" in n for n in notes)


def test_to_timestamp_bare_seconds_stays_timestamp_seconds() -> None:
    # `to_timestamp(bare_int)` is already INT64-safe; keep it as
    # TIMESTAMP_SECONDS without the millis collapse.
    sql = 'SELECT * FROM "t" WHERE "ts" >= to_timestamp(1790805660)'
    out, notes = _translate(sql)
    assert "TIMESTAMP_SECONDS(1790805660)" in out
    assert not any("TIMESTAMP_MILLIS" in n for n in notes)


def test_timestamp_seconds_non_1000_divisor_untouched() -> None:
    # Only the `/1000` idiom collapses; other divisors stay as-is (and
    # become the caller's problem to make INT64-safe).
    sql = "SELECT TIMESTAMP_SECONDS(x / 60) FROM t"
    out, _ = _translate(sql)
    assert "TIMESTAMP_SECONDS(x / 60)" in out


def test_timestamp_seconds_fix_skips_string_literals() -> None:
    # A string literal that happens to contain the TIMESTAMP_SECONDS(x/1000)
    # text must not be rewritten.
    sql = "SELECT 'TIMESTAMP_SECONDS(x / 1000)' AS label FROM t"
    out, notes = _translate(sql)
    assert "'TIMESTAMP_SECONDS(x / 1000)'" in out
    assert not any("TIMESTAMP_MILLIS" in n for n in notes)
