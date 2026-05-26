"""End-to-end tests for `GET /api/3/action/datastore_search_sql`.

Only `sql` is accepted as a query parameter; the response reuses the
`datastore_search` envelope shape (same writer, same `_links` / `limit` /
`offset` / `resource_id` fields filled in with safe defaults).

Covers:
    1. happy path — minimal SELECT
    2. response shape — same envelope keys as datastore_search
    3. SQL validation — empty, non-SELECT, multi-statement, comments
    4. extra params rejected (only `sql` allowed)
    5. sqlglot extraction — table / function names from SQL
    6. function allow-list — disallowed functions return 400
    7. per-table auth — each referenced table is authorized

The engine placeholder returns an empty SearchResult, so these tests pin
the request / response shape and the routing — actual query semantics
belong with the real BigQuery backend.
"""

from __future__ import annotations

import pytest
from datastore.schemas.validators import parse_sql_references
from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

SQL_URL = "/api/3/action/datastore_search_sql"


# 1. Happy path -------------------------------------------------------------

def test_basic_sql_succeeds(client: TestClient) -> None:
    response = client.get(SQL_URL, params={"sql": "SELECT 1 LIMIT 10"})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["result"]["records"] == []  # placeholder yields nothing


def test_with_cte_succeeds(client: TestClient) -> None:
    """`WITH ... SELECT` (CTE) is allowed alongside plain SELECT."""
    response = client.get(SQL_URL, params={
        "sql": "WITH t AS (SELECT 1 AS a) SELECT * FROM t LIMIT 10"
    })
    assert response.status_code == 200


def test_trailing_semicolon_allowed(client: TestClient) -> None:
    response = client.get(SQL_URL, params={"sql": "SELECT 1 LIMIT 10;"})
    assert response.status_code == 200


def test_leading_comment_then_select_allowed(client: TestClient) -> None:
    response = client.get(SQL_URL, params={
        "sql": "-- a note\nSELECT 1 LIMIT 10"
    })
    assert response.status_code == 200


def test_missing_limit_rejected(client: TestClient) -> None:
    """LIMIT is required so the server can paginate and so unbounded
    SELECTs can't pin the streaming response open."""
    response = client.get(SQL_URL, params={"sql": "SELECT 1"})
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "LIMIT" in body["error"]["message"]


def test_limit_above_max_rejected(client: TestClient) -> None:
    """LIMIT must be <= `SEARCH_RESULT_ROWS_MAX` (default 32000).
    Above the cap → 400 with a 'paginate with OFFSET' hint."""
    response = client.get(SQL_URL, params={
        "sql": "SELECT 1 LIMIT 50000",
    })
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "OFFSET" in body["error"]["message"]


# 2. Response envelope shape ------------------------------------------------

def test_response_shape_matches_datastore_search(client: TestClient) -> None:
    """Same envelope as `datastore_search` so clients can share a parser.
    `limit` / `offset` come from the SQL's LIMIT / OFFSET literals."""
    response = client.get(SQL_URL, params={
        "sql": "SELECT 1 LIMIT 50 OFFSET 100"
    })

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    result = response.json()["result"]
    assert set(result) >= {
        "resource_id", "schema", "fields", "records", "limit", "offset", "_links",
    }
    # Both column shapes are present: canonical `schema` + legacy `fields`.
    assert isinstance(result["schema"], dict)
    assert "fields" in result["schema"]
    # `resource_id` is empty (raw SQL doesn't bind to one resource);
    # `limit` / `offset` mirror the SQL literals.
    assert result["resource_id"] == ""
    assert result["limit"] == 50
    assert result["offset"] == 100


def test_response_includes_pagination_links(client: TestClient) -> None:
    """`_links` carries `start` + page counters. Placeholder engine
    returns total=0 so `page` / `total_pages` are suppressed (empty
    landing page rule); the URLs rewrite the SQL's OFFSET."""
    response = client.get(SQL_URL, params={"sql": "SELECT 1 LIMIT 10"})

    links = response.json()["result"]["_links"]
    assert "start" in links
    assert links["page_size"] == 10
    # start URL embeds the SQL with OFFSET 0
    assert "OFFSET+0" in links["start"] or "OFFSET%200" in links["start"]


def test_response_echoes_original_sql(client: TestClient) -> None:
    """`result.sql` echoes the request SQL verbatim. Useful when
    `_links.next` rewrites the OFFSET — clients can still see what
    actually ran on this page."""
    sql = (
        'SELECT auction_id FROM "balancing_auction_results_2025" '
        'LIMIT 5 OFFSET 10'
    )
    response = client.get(SQL_URL, params={"sql": sql})
    assert response.status_code == 200
    assert response.json()["result"]["sql"] == sql


def test_pagination_links_rewrite_sql_offset(client: TestClient) -> None:
    """When the placeholder reports total=0 there's no `next`, but
    once the engine reports rows the `next` URL would carry a SQL
    string with OFFSET advanced by LIMIT. Verify the URL builder
    rewrites OFFSET on the `start` link from the current offset back
    to 0."""
    response = client.get(SQL_URL, params={
        "sql": "SELECT 1 LIMIT 50 OFFSET 200"
    })
    assert response.status_code == 200
    links = response.json()["result"]["_links"]
    # `start` resets to OFFSET 0 — `prev` lands at max(0, 200-50) = 150.
    assert "prev" in links
    # URL is percent-encoded; the new OFFSET literal is in the `sql`
    # query param. Decode-ish: look for the substring after encoding.
    assert "OFFSET+150" in links["prev"] or "OFFSET%20150" in links["prev"]


# 3. SQL validation ---------------------------------------------------------

def test_missing_sql_returns_validation_error(client: TestClient) -> None:
    response = client.get(SQL_URL, params={})

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "sql" in body["error"]["fields"]


def test_empty_sql_returns_validation_error(client: TestClient) -> None:
    response = client.get(SQL_URL, params={"sql": "   "})

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "sql" in body["error"]["fields"]


def test_non_select_statements_rejected(client: TestClient) -> None:
    """DDL / DML are rejected client-side; real safety lives at the engine."""
    for sql in (
        "DROP TABLE x",
        "INSERT INTO x VALUES (1)",
        "DELETE FROM x",
        "UPDATE x SET a = 1",
        "CREATE TABLE y (a int)",
        "ALTER TABLE x ADD COLUMN b int",
        "TRUNCATE TABLE x",
    ):
        response = client.get(SQL_URL, params={"sql": sql})
        assert response.status_code == 400, f"expected 400 for: {sql}"


def test_multiple_statements_rejected(client: TestClient) -> None:
    response = client.get(SQL_URL, params={"sql": "SELECT 1; SELECT 2"})

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"


def test_unparseable_sql_rejected(client: TestClient) -> None:
    """SQL that passes the SELECT-only regex but sqlglot can't parse →
    `_extract_sql_references` raises ValueError → 400. Real safety still
    sits at the engine credential layer; this is just fail-fast UX."""
    for sql in (
        "SELECT $$$ random",       # tokenizer error
        "SELECT FROM WHERE",       # bare FROM
        "SELECT * FROM",           # missing table
    ):
        response = client.get(SQL_URL, params={"sql": sql})
        assert response.status_code == 400, f"expected 400 for: {sql}"
        body = response.json()
        assert body["error"]["__type"] == "Validation Error"
        assert "sql" in body["error"]["fields"]


# 4. Extra params rejected --------------------------------------------------

def test_extra_query_param_rejected(client: TestClient) -> None:
    """`extra='forbid'` — only `sql` is allowed on this endpoint."""
    response = client.get(SQL_URL, params={"sql": "SELECT 1", "limit": 10})

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"


# 5. sqlglot extraction (unit tests on parse_sql_references) ---------------

@pytest.mark.parametrize("sql,tables,functions", [
    # tables only
    ('SELECT * FROM "abc-def" WHERE title LIKE \'jones\'',
     ["abc-def"], []),
    # functions, no table
    ("SELECT COUNT(*), pg_read_file('/etc/passwd')",
     [], ["count", "pg_read_file"]),
    # aggregate + date function
    ("SELECT AVG(price), DATE_TRUNC('day', d) FROM auctions GROUP BY 2",
     ["auctions"], ["avg", "date_trunc"]),
    # CTE aliases are NOT external tables
    ("WITH t AS (SELECT 1 AS a) SELECT * FROM t",
     [], []),
    # JOIN — multiple tables, deduped
    ("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
     ["orders", "users"], []),
    # CASE WHEN is syntactic, not a function
    ("SELECT CASE WHEN x > 1 THEN 'big' ELSE 'small' END FROM t",
     ["t"], []),
])
def test_parse_sql_references_extracts_names(
    sql: str, tables: list[str], functions: list[str]
) -> None:
    t, f = parse_sql_references(sql)
    assert t == tables
    assert f == functions


def test_parse_sql_references_rejects_unparseable() -> None:
    """sqlglot raises → we re-raise as ValueError."""
    with pytest.raises(ValueError):
        parse_sql_references("SELECT $$$ random garbage")


# 6. Function allow-list ----------------------------------------------------

def test_disallowed_function_returns_validation_error(client: TestClient) -> None:
    """`pg_read_file` isn't in `ALLOWED_SQL_FUNCTIONS` → 400."""
    response = client.get(SQL_URL, params={
        "sql": "SELECT pg_read_file('/etc/passwd') LIMIT 1",
    })
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "pg_read_file" in body["error"]["message"].lower()


def test_allowed_function_succeeds(client: TestClient) -> None:
    """`COUNT` is in the allow-list — no tables, so no auth call either."""
    response = client.get(SQL_URL, params={"sql": "SELECT COUNT(*) LIMIT 1"})
    assert response.status_code == 200


# 7. Per-table authorization ------------------------------------------------

def test_unknown_table_returns_404(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    """Each referenced table is authorized via CKAN — unknown → 404."""
    response = client.get(SQL_URL, params={
        "sql": 'SELECT * FROM "does-not-exist" LIMIT 10',
    })
    assert response.status_code == 404
    body = response.json()
    assert body["error"]["__type"] == "Not Found Error"


def test_existing_table_authorized(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    """Referenced table that exists in CKAN clears auth → 200."""
    response = client.get(SQL_URL, params={
        "sql": 'SELECT * FROM "balancing_auction_results_2025" LIMIT 10',
    })
    assert response.status_code == 200


def test_denied_api_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    """Auth gate uses the same path as datastore_search — denial returns 403."""
    fake_ckan.deny("test-token")
    response = client.get(SQL_URL, params={
        "sql": 'SELECT * FROM "balancing_auction_results_2025" LIMIT 10',
    })
    assert response.status_code == 403
    assert response.json()["error"]["__type"] == "Authorization Error"


def test_each_table_authorized_once_for_joins(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    """A JOIN over two existing tables calls authorize twice."""
    fake_ckan.add_resource("other_table", package_id="pkg-balancing-2025")
    before = fake_ckan.authorize_calls
    response = client.get(SQL_URL, params={
        "sql": (
            'SELECT a.id FROM "balancing_auction_results_2025" a '
            'JOIN "other_table" b ON a.id = b.id LIMIT 10'
        ),
    })
    assert response.status_code == 200
    assert fake_ckan.authorize_calls - before == 2
