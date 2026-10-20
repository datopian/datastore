"""End-to-end tests for the legacy CKAN compatibility endpoints.

Mounted paths:
    GET/POST  /api/3/action/datastore_search
    GET/POST  /api/3/action/datastore_search_sql
    GET/POST  /api/3/action/datastore_info
    GET       /datastore/dump/{resource_id}

The engine is stubbed (same pattern as `tests/test_datastore_search.py`)
with rows that *include* `_id` and `_updated_at`. Legacy response rules:

  - `_updated_at` is stripped (datastore-internal; CKAN never surfaces it).
  - `_id` passes through — real CKAN exposes it and scripts filter on it.
  - On `datastore_search_sql` only, a `_full_text` column is synthesised
    (space-joined concat of non-null text values) so clients that read
    `record["_full_text"]` on a `SELECT *` get a value in the expected
    shape even though BigQuery has no Postgres tsvector equivalent.
    `datastore_search` and the dump endpoints do NOT emit `_full_text`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from datastore.infrastructure.engines.base import InfoResult, SearchResult
from datastore.infrastructure.engines.bigquery import BigQueryBackend
from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

SEARCH_URL = "/api/3/action/datastore_search"
SEARCH_SQL_URL = "/api/3/action/datastore_search_sql"
INFO_URL = "/api/3/action/datastore_info"
DUMP_URL_TMPL = "/datastore/dump/{rid}"

_RESOURCE_ID = "balancing_auction_results_2025"


# Engine side: a Frictionless schema that INCLUDES the two system columns
# so the stripping logic actually has something to strip.
_FAKE_SCHEMA: dict[str, Any] = {
    "fields": [
        {"name": "_id", "type": "integer"},
        {"name": "auction_id", "type": "integer"},
        {"name": "product_code", "type": "string"},
        {"name": "clearing_price_gbp_per_mwh", "type": "number"},
        {"name": "_updated_at", "type": "datetime"},
    ]
}

_FAKE_ROWS: list[tuple] = [
    (1, 144, "DCL", 47.82, "2026-10-20T00:00:00Z"),
    (2, 145, "DCH", 51.10, "2026-10-20T00:00:01Z"),
    (3, 146, "FFR", 32.40, "2026-10-20T00:00:02Z"),
]


def _install_mock_search(
    monkeypatch: pytest.MonkeyPatch,
    *,
    schema: dict[str, Any] | None = None,
    rows: list[tuple] | None = None,
) -> list[tuple]:
    """Patch `BigQueryBackend.search` + `.search_sql` so legacy tests
    exercise the stripping writers against known data."""
    schema = schema if schema is not None else _FAKE_SCHEMA
    rows = rows if rows is not None else _FAKE_ROWS
    consumed: list[tuple] = []

    def lazy_records() -> Iterator[tuple]:
        for r in rows:
            consumed.append(r)
            yield r

    def fake_search(self: BigQueryBackend, **kwargs: Any) -> SearchResult:
        return SearchResult(
            schema=schema,
            records=lazy_records(),
            total=len(rows),
            records_truncated=False,
        )

    def fake_search_sql(self: BigQueryBackend, sql: str, limit: int) -> SearchResult:
        return SearchResult(
            schema=schema,
            records=lazy_records(),
            total=None,
            records_truncated=False,
        )

    monkeypatch.setattr(BigQueryBackend, "search", fake_search)
    monkeypatch.setattr(BigQueryBackend, "search_sql", fake_search_sql)
    return consumed


def _install_mock_info(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_info(self: BigQueryBackend, resource_id: str) -> InfoResult:
        return InfoResult(schema=_FAKE_SCHEMA, meta={"total": 3})

    monkeypatch.setattr(BigQueryBackend, "info", fake_info)


# --- datastore_search ------------------------------------------------------


def test_legacy_search_basic_envelope(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_mock_search(monkeypatch)
    response = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID, "limit": 10})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    result = body["result"]
    assert result["resource_id"] == _RESOURCE_ID
    assert result["limit"] == 10
    assert result["offset"] == 0
    assert result["records_format"] == "objects"
    assert "_links" in result


def test_legacy_search_fields_shape_matches_ckan(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_updated_at` stripped; `_id` passes through. `_full_text` is
    NOT emitted on `datastore_search` — CKAN only surfaces it via
    `datastore_search_sql`'s `SELECT *` path."""
    _install_mock_search(monkeypatch)
    response = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID})
    assert response.status_code == 200

    result = response.json()["result"]
    field_ids = [f["id"] for f in result["fields"]]
    assert "_updated_at" not in field_ids
    assert "_full_text" not in field_ids
    assert field_ids == [
        "_id",
        "auction_id",
        "product_code",
        "clearing_price_gbp_per_mwh",
    ]
    for f in result["fields"]:
        assert set(f.keys()) == {"id", "type"}, "legacy fields entries only carry id+type"


def test_legacy_search_records_strip_updated_at_keep_id(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each record keeps `_id` and drops `_updated_at`. No `_full_text`
    on `datastore_search` records (search_sql only)."""
    _install_mock_search(monkeypatch)
    response = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID})

    result = response.json()["result"]
    assert len(result["records"]) == 3
    for row in result["records"]:
        assert "_updated_at" not in row
        assert "_id" in row
        assert "_full_text" not in row
    assert result["records"][0] == {
        "_id": 1,
        "auction_id": 144,
        "product_code": "DCL",
        "clearing_price_gbp_per_mwh": 47.82,
    }


def test_legacy_search_no_schema_block(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """V2 emits a `schema` sibling of `fields`; legacy must not."""
    _install_mock_search(monkeypatch)
    response = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID})
    assert "schema" not in response.json()["result"]


def test_legacy_search_post_matches_get(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_mock_search(monkeypatch)
    get_body = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID, "limit": 5}).json()

    _install_mock_search(monkeypatch)
    post_body = client.post(
        SEARCH_URL, json={"resource_id": _RESOURCE_ID, "limit": 5}
    ).json()

    # help URL + _links differ by HTTP method presence; blank them for the diff.
    for b in (get_body, post_body):
        b["help"] = ""
        b["result"]["_links"] = {}
    assert get_body == post_body


def test_legacy_search_total_included_by_default(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_mock_search(monkeypatch)
    result = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID}).json()["result"]
    assert result["total"] == 3
    assert result["include_total"] is True
    # Real CKAN does NOT emit `total_was_truncated` — keep parity.
    assert "total_was_truncated" not in result


# --- datastore_search_sql --------------------------------------------------


def test_legacy_search_sql_no_limit_accepted(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real CKAN accepts SQL without LIMIT; v2 doesn't. Legacy must."""
    _install_mock_search(monkeypatch)
    response = client.get(
        SEARCH_SQL_URL, params={"sql": f'SELECT * FROM "{_RESOURCE_ID}"'}
    )
    assert response.status_code == 200


def test_legacy_search_sql_limit_is_not_capped(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`datastore_search_sql` on legacy has NO row cap — a caller is
    allowed to request `LIMIT 500000` even though
    `SEARCH_RESULT_ROWS_MAX` is 32000 (which still gates
    `datastore_search`). Legacy CKAN scripts assume the SQL path
    returns whatever the query asks for; the engine streams row-by-row
    so there's no memory risk.
    """
    captured: dict[str, Any] = {}

    def fake_search_sql(self: BigQueryBackend, sql: str, limit: int) -> SearchResult:
        captured["limit"] = limit
        return SearchResult(
            schema=_FAKE_SCHEMA, records=iter([]), total=None, records_truncated=False
        )

    monkeypatch.setattr(BigQueryBackend, "search_sql", fake_search_sql)

    response = client.get(
        SEARCH_SQL_URL,
        params={"sql": f'SELECT * FROM "{_RESOURCE_ID}" LIMIT 500000'},
    )
    assert response.status_code == 200
    # The engine saw the caller's LIMIT verbatim — not clamped to
    # SEARCH_RESULT_ROWS_MAX (32000).
    assert captured["limit"] == 500000


def test_legacy_search_sql_no_limit_passes_unbounded_sentinel(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the caller omits LIMIT, the engine gets int32-max — not
    the `SEARCH_RESULT_ROWS_MAX` cap. Confirms the omit-LIMIT path
    drains the full result set rather than silently clamping."""
    captured: dict[str, Any] = {}

    def fake_search_sql(self: BigQueryBackend, sql: str, limit: int) -> SearchResult:
        captured["limit"] = limit
        return SearchResult(
            schema=_FAKE_SCHEMA, records=iter([]), total=None, records_truncated=False
        )

    monkeypatch.setattr(BigQueryBackend, "search_sql", fake_search_sql)

    response = client.get(
        SEARCH_SQL_URL, params={"sql": f'SELECT * FROM "{_RESOURCE_ID}"'}
    )
    assert response.status_code == 200
    assert captured["limit"] == 2**31 - 1


def test_legacy_search_sql_system_column_handling(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SQL path: `_updated_at` stripped; `_id` passes through; and
    `_full_text` is synthesised per row. Matches real CKAN's
    `datastore_search_sql` which exposes the tsvector column via
    `SELECT *` on a Postgres datastore."""
    _install_mock_search(monkeypatch)
    response = client.get(
        SEARCH_SQL_URL,
        params={"sql": f'SELECT * FROM "{_RESOURCE_ID}" LIMIT 10'},
    )
    assert response.status_code == 200

    result = response.json()["result"]
    assert result["sql"].startswith("SELECT")
    for row in result["records"]:
        assert "_updated_at" not in row
        assert "_id" in row
        assert "_full_text" in row
    field_ids = [f["id"] for f in result["fields"]]
    assert "_updated_at" not in field_ids
    assert field_ids[:2] == ["_id", "_full_text"]
    full_text_field = next(f for f in result["fields"] if f["id"] == "_full_text")
    assert full_text_field["type"] == "tsvector"


def test_legacy_search_sql_full_text_is_tsvector_shape(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_full_text` is a Postgres tsvector lexeme string, matching
    `to_tsvector('english', …)` byte-for-byte: lowercased + stopword-
    filtered + Snowball-stemmed, 1-indexed walk-order positions
    (positions advance for stopwords too), alphabetically sorted.

    Row: _id=1, auction_id=144, product_code=DCL,
         clearing_price_gbp_per_mwh=47.82
    After `_id` is excluded, tokens are: 144, DCL, 47, 82 — none
    stopwords, none stemmable, so the output is unchanged by the
    linguistic pipeline:
    """
    _install_mock_search(monkeypatch)
    response = client.get(
        SEARCH_SQL_URL,
        params={"sql": f'SELECT * FROM "{_RESOURCE_ID}" LIMIT 10'},
    )
    row = response.json()["result"]["records"][0]
    assert row["_full_text"] == "'144':1 '47':3 '82':4 'dcl':2"


def test_legacy_full_text_matches_postgres_tsvector_byte_for_byte(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round-trip the actual CKAN sample from `api.neso.energy` to
    prove our tsvector output is byte-identical to Postgres's."""
    schema: dict[str, Any] = {
        "fields": [
            {"name": "_id", "type": "integer"},
            {"name": "Operational Date", "type": "string"},
            {"name": "Auction Type", "type": "string"},
            {"name": "Version", "type": "integer"},
            {"name": "Hourly Time Period GMT", "type": "string"},
            {"name": "Flow in MW To GB", "type": "integer"},
            {"name": "Flow in MW From GB", "type": "integer"},
            {"name": "Reason For Reduction", "type": "string"},
        ]
    }
    rows: list[tuple] = [
        (1, "2024-09-24", "DayAhead", 1, "22:00 to 23:00", 999, 1029, "No Restriction"),
    ]
    _install_mock_search(monkeypatch, schema=schema, rows=rows)
    response = client.get(
        SEARCH_SQL_URL,
        params={"sql": f'SELECT * FROM "{_RESOURCE_ID}" LIMIT 1'},
    )
    row = response.json()["result"]["records"][0]
    # Verbatim from https://api.neso.energy/api/3/action/datastore_search_sql
    assert row["_full_text"] == (
        "'-09':2 '-24':3 '00':7,10 '1':5 '1029':12 '2024':1 "
        "'22':6 '23':9 '999':11 'dayahead':4 'restrict':14"
    )


def test_legacy_search_sql_strips_trailing_whitespace_in_echo(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A trailing `%20` in the query string must not survive to the
    echoed `sql`. Real CKAN emits the SQL with surrounding whitespace
    stripped: `?sql=SELECT…%20` → `"sql": "SELECT…"`.
    """
    _install_mock_search(monkeypatch)
    response = client.get(
        SEARCH_SQL_URL,
        # URL-encoded trailing space (`%20`) — mimics the bug report.
        params={"sql": f'SELECT * FROM "{_RESOURCE_ID}" '},
    )
    assert response.status_code == 200
    sql = response.json()["result"]["sql"]
    assert sql == f'SELECT * FROM "{_RESOURCE_ID}"', (
        f"sql echo retained whitespace: {sql!r}"
    )


def test_legacy_search_sql_omits_records_truncated(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real CKAN's `datastore_search_sql` doesn't emit `records_truncated`."""
    _install_mock_search(monkeypatch)
    response = client.get(
        SEARCH_SQL_URL,
        params={"sql": f'SELECT * FROM "{_RESOURCE_ID}" LIMIT 10'},
    )
    assert "records_truncated" not in response.json()["result"]


def test_legacy_search_sql_post_matches_get(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    sql = f'SELECT * FROM "{_RESOURCE_ID}" LIMIT 5'

    _install_mock_search(monkeypatch)
    get_body = client.get(SEARCH_SQL_URL, params={"sql": sql}).json()

    _install_mock_search(monkeypatch)
    post_body = client.post(SEARCH_SQL_URL, json={"sql": sql}).json()

    for b in (get_body, post_body):
        b["help"] = ""
    assert get_body == post_body


# --- datastore_info --------------------------------------------------------


def test_legacy_info_system_column_handling(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """info: `_updated_at` stripped; `_id` kept. No `_full_text` on info
    (synthesis happens on search records only, not on the schema listing)."""
    _install_mock_info(monkeypatch)
    response = client.get(INFO_URL, params={"resource_id": _RESOURCE_ID})
    assert response.status_code == 200

    result = response.json()["result"]
    field_ids = {f["id"] for f in result["fields"]}
    assert "_updated_at" not in field_ids
    assert "_id" in field_ids
    assert "_full_text" not in field_ids
    assert "schema" not in result, "legacy info must not surface the v2 schema block"
    assert result["meta"] == {"total": 3}


def test_legacy_info_post_accepts_body(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_mock_info(monkeypatch)
    response = client.post(INFO_URL, json={"resource_id": _RESOURCE_ID})
    assert response.status_code == 200
    assert response.json()["result"]["resource_id"] == _RESOURCE_ID


def test_legacy_info_id_alias(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CKAN callers pass `?id=<rid>`; the schema already normalises it."""
    _install_mock_info(monkeypatch)
    response = client.get(INFO_URL, params={"id": _RESOURCE_ID})
    assert response.status_code == 200
    assert response.json()["result"]["resource_id"] == _RESOURCE_ID


# --- auth / errors (CKAN parity: logical errors return HTTP 200) ----------
#
# Real CKAN's action API returns `200 OK` with `success:false` for every
# logical error (ValidationError / NotAuthorized / ObjectNotFound). A
# legacy client doing `if response.status_code == 200: body = r.json()`
# must keep working, so these tests pin the status + envelope shape.


def test_legacy_search_unknown_resource_returns_200_with_error_envelope(
    client: TestClient,
) -> None:
    """No-field-context error: `message` + `__type` under `error`.
    No `fields` sub-dict (CKAN flattens field details onto `error` itself).
    """
    response = client.get(SEARCH_URL, params={"resource_id": "does-not-exist"})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Not Found Error"
    assert "message" in body["error"]
    assert "fields" not in body["error"]
    assert "help" in body


def test_legacy_search_denied_key_returns_200_with_error_envelope(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.deny("test-token")
    response = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Authorization Error"


def test_legacy_search_bad_query_param_returns_200_validation_error(
    client: TestClient,
) -> None:
    """CKAN-shape validation error: per-field details are AT THE ROOT
    of the `error` dict (not nested under `fields`), matching real
    CKAN's `{"error": {"<field>": ["msg"], "__type": "Validation Error"}}`.
    """
    response = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID, "limit": "notanint"})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    error = body["error"]
    assert error["__type"] == "Validation Error"
    # Per-field keys live at the error root (CKAN parity).
    assert "limit" in error
    assert isinstance(error["limit"], list)
    # No nested `fields` sub-dict — v2 shape, not legacy.
    assert "fields" not in error


def test_legacy_search_sql_bad_sql_returns_200(
    client: TestClient,
) -> None:
    """A disallowed statement is a validation error — 200 under CKAN.

    Per CKAN parity: field-context errors land as top-level keys on
    the error dict (`error["sql"]` here), not under `error["fields"]`.
    """
    response = client.get(SEARCH_SQL_URL, params={"sql": "DELETE FROM foo"})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    error = body["error"]
    assert error["__type"] == "Validation Error"
    # The error context from pydantic is `sql`, so it should be at root.
    assert "sql" in error
    assert "fields" not in error


def test_legacy_info_unknown_resource_returns_200(
    client: TestClient,
) -> None:
    response = client.get(INFO_URL, params={"resource_id": "does-not-exist"})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    assert body["error"]["__type"] == "Not Found Error"


def test_legacy_search_post_error_returns_200(
    client: TestClient,
) -> None:
    """POST errors must get the same 200 treatment as GET errors."""
    response = client.post(SEARCH_URL, json={"resource_id": "does-not-exist"})
    assert response.status_code == 200
    assert response.json()["error"]["__type"] == "Not Found Error"


def test_legacy_content_type_is_json(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Successful responses must advertise application/json (CKAN parity)."""
    _install_mock_search(monkeypatch)
    response = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID})
    assert response.headers["content-type"].startswith("application/json")


def test_legacy_error_content_type_is_json(
    client: TestClient,
) -> None:
    response = client.get(SEARCH_URL, params={"resource_id": "does-not-exist"})
    assert response.headers["content-type"].startswith("application/json")


# --- dump ------------------------------------------------------------------


def test_legacy_dump_csv_default(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Default (no ?format) is CSV. Dumps hide BOTH `_updated_at` and
    `_id` — a published download is meant to look like the "clean"
    dataset the publisher would hand over; the datastore's row-id
    serial is an implementation detail that shouldn't leak into CSVs
    people archive and share. `_full_text` is also absent (synthesis
    is for the JSON action API only)."""
    _install_mock_search(monkeypatch)
    response = client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID))
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]

    text = response.text
    lines = text.strip().split("\r\n")
    header = lines[0].split(",")
    assert "_updated_at" not in header
    assert "_id" not in header
    assert "_full_text" not in header
    assert header == ["auction_id", "product_code", "clearing_price_gbp_per_mwh"]
    assert len(lines) == 1 + len(_FAKE_ROWS)
    assert lines[1] == "144,DCL,47.82"


def test_legacy_dump_tsv_format(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_mock_search(monkeypatch)
    response = client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID), params={"format": "tsv"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/tab-separated-values")
    text = response.text
    lines = text.strip().split("\r\n")
    assert lines[0] == "auction_id\tproduct_code\tclearing_price_gbp_per_mwh"
    assert lines[1] == "144\tDCL\t47.82"


def test_legacy_dump_json_format(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """JSON format is a JSON ARRAY of objects (not NDJSON). `_id` and
    `_updated_at` both absent — dump-only hidden-column rule."""
    _install_mock_search(monkeypatch)
    response = client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID), params={"format": "json"})
    assert response.status_code == 200
    body = json.loads(response.content)
    assert isinstance(body, list)
    assert len(body) == 3
    for row in body:
        assert "_updated_at" not in row
        assert "_id" not in row
    assert body[0] == {
        "auction_id": 144,
        "product_code": "DCL",
        "clearing_price_gbp_per_mwh": 47.82,
    }


def test_legacy_dump_xml_format(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_mock_search(monkeypatch)
    response = client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID), params={"format": "xml"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/xml")
    text = response.text
    assert text.startswith('<?xml version="1.0" encoding="UTF-8"?>')
    assert "<data>" in text and "</data>" in text
    assert text.count("<row>") == 3
    assert "<auction_id>144</auction_id>" in text
    assert "<product_code>DCL</product_code>" in text
    # Dumps hide both `_id` and `_updated_at` — see test_legacy_dump_csv_default.
    assert "<_id>" not in text
    assert "<_updated_at>" not in text


def test_legacy_dump_bom_flag(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`?bom=true` prepends the UTF-8 BOM — Excel-on-Windows parity."""
    _install_mock_search(monkeypatch)
    without = client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID))
    _install_mock_search(monkeypatch)
    with_bom = client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID), params={"bom": "true"})

    assert without.content[:3] != b"\xef\xbb\xbf"
    assert with_bom.content[:3] == b"\xef\xbb\xbf"
    # Content after the BOM is identical byte-for-byte.
    assert with_bom.content[3:] == without.content


def test_legacy_dump_rejects_unknown_format(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_mock_search(monkeypatch)
    response = client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID), params={"format": "parquet"})
    # `parquet` is a v2 format but not a legacy one — FastAPI rejects it
    # at the Literal boundary with a 422 (unprocessable entity).
    assert response.status_code in (400, 422)


def test_legacy_dump_passes_filters_through_to_engine(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`filters` / `q` / `sort` / `limit` land on the engine call so
    `curl /datastore/dump/<rid>?filters=...` keeps working."""
    captured: dict[str, Any] = {}

    def fake_search(self: BigQueryBackend, **kwargs: Any) -> SearchResult:
        captured.update(kwargs)
        return SearchResult(
            schema=_FAKE_SCHEMA,
            records=iter(_FAKE_ROWS),
            total=None,
            records_truncated=False,
        )

    monkeypatch.setattr(BigQueryBackend, "search", fake_search)

    response = client.get(
        DUMP_URL_TMPL.format(rid=_RESOURCE_ID),
        params={
            "filters": json.dumps({"product_code": "DCL"}),
            "q": "DCL",
            "sort": "auction_id desc",
            "limit": 50,
            "offset": 10,
            "distinct": "true",
        },
    )
    assert response.status_code == 200
    assert captured["filters"] == {"product_code": "DCL"}
    assert captured["q"] == "DCL"
    assert captured["sort"] == "auction_id desc"
    assert captured["limit"] == 50
    assert captured["offset"] == 10
    assert captured["distinct"] is True


def test_legacy_dump_unbounded_without_limit(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Omitting `limit` passes the int32-max sentinel to the engine so
    a whole-table dump drains without an explicit cap."""
    captured: dict[str, Any] = {}

    def fake_search(self: BigQueryBackend, **kwargs: Any) -> SearchResult:
        captured.update(kwargs)
        return SearchResult(
            schema=_FAKE_SCHEMA, records=iter([]), total=None, records_truncated=False
        )

    monkeypatch.setattr(BigQueryBackend, "search", fake_search)

    client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID))
    assert captured["limit"] == 2**31 - 1


def test_legacy_dump_unknown_resource_returns_404(
    client: TestClient,
) -> None:
    response = client.get(DUMP_URL_TMPL.format(rid="does-not-exist"))
    assert response.status_code == 404


def test_legacy_dump_denied_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN
) -> None:
    fake_ckan.deny("test-token")
    response = client.get(DUMP_URL_TMPL.format(rid=_RESOURCE_ID))
    assert response.status_code == 403


# --- parity with v2 shape sanity -------------------------------------------


def test_legacy_search_record_values_match_input_tuple_order(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Row tuples are engine-column-ordered; stripping must project by
    index, not by name, or values and columns would misalign."""
    _install_mock_search(monkeypatch)
    result = client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID}).json()["result"]

    assert result["records"] == [
        {"_id": 1, "auction_id": 144, "product_code": "DCL", "clearing_price_gbp_per_mwh": 47.82},
        {"_id": 2, "auction_id": 145, "product_code": "DCH", "clearing_price_gbp_per_mwh": 51.10},
        {"_id": 3, "auction_id": 146, "product_code": "FFR", "clearing_price_gbp_per_mwh": 32.40},
    ]


def test_legacy_search_links_shape_matches_ckan(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_links` must be the CKAN-legacy shape: only URL strings, no
    page counters. Real CKAN emits something like
        {"start": "/api/3/action/datastore_search?limit=5&resource_id=...",
         "next":  "/api/3/action/datastore_search?offset=5&limit=5&resource_id=..."}
    v2's `_links` adds `page_size`, `page`, `total_pages` — these must
    not leak into the legacy envelope or old clients that treat
    `_links` as a `{name: href}` map will fail.
    """
    _install_mock_search(monkeypatch)
    links = (
        client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID, "limit": 2})
        .json()["result"]["_links"]
    )
    assert "start" in links
    # total=3, limit=2 → next page exists at offset=2
    assert "next" in links

    # Only URL strings, no integer counters.
    assert "page_size" not in links
    assert "page" not in links
    assert "total_pages" not in links
    for key, val in links.items():
        assert isinstance(val, str), f"_links[{key!r}] is {type(val).__name__}, want str"


def test_legacy_search_links_are_path_only(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CKAN emits `_links` as path+query only, no scheme/host — the
    client appends them to the API host it already hit."""
    _install_mock_search(monkeypatch)
    links = (
        client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID, "limit": 2})
        .json()["result"]["_links"]
    )
    for key, href in links.items():
        assert href.startswith("/api/3/action/datastore_search?"), (
            f"_links[{key!r}] = {href!r}; CKAN emits path-only URLs"
        )
        assert "://" not in href, (
            f"_links[{key!r}] = {href!r} carries a scheme; CKAN strips it"
        )


def test_legacy_search_links_next_omitted_on_last_page(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When `offset + limit >= total`, `next` must not be emitted."""
    _install_mock_search(monkeypatch)
    links = (
        # total=3, limit=10 → no next page
        client.get(SEARCH_URL, params={"resource_id": _RESOURCE_ID, "limit": 10})
        .json()["result"]["_links"]
    )
    assert "start" in links
    assert "next" not in links


def test_legacy_search_links_prev_emitted_when_paging(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`prev` appears once we've paged past offset 0."""
    _install_mock_search(monkeypatch)
    links = (
        client.get(
            SEARCH_URL,
            params={"resource_id": _RESOURCE_ID, "limit": 2, "offset": 2},
        )
        .json()["result"]["_links"]
    )
    assert "prev" in links
    assert "offset=0" in links["prev"]


def test_legacy_search_post_json_body_round_trips_filters(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """POST bodies take `filters` as a real JSON object (not a string).
    FastAPI's `Body(...)` parses it directly; the field validator is
    tolerant of both — a dict passes `to_json_object` as-is."""
    _install_mock_search(monkeypatch)
    response = client.post(
        SEARCH_URL,
        json={"resource_id": _RESOURCE_ID, "filters": json.dumps({"product_code": "DCL"})},
    )
    assert response.status_code == 200
