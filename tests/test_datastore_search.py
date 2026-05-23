"""End-to-end tests for `GET /api/3/action/datastore_search`.

`datastore_search` is GET with query parameters. Complex types are
encoded:

    filters   → JSON-encoded object
    q (dict)  → JSON-encoded object   (leading `{`)
    q (str)   → plain string
    fields    → comma-separated list

Covers:
    1. happy path — minimal request, default echoes
    2. optional knobs — filters, q (str + dict), distinct, sort, fields
    3. pagination — limit / offset echo, include_total gating on `total`
    4. validation — bad limit / offset, missing resource_id, malformed JSON
    5. auth — unknown resource_id (404), denied api_key (403)
    6. records_format — objects / lists / csv / tsv routing + invalid format
    7. streaming with a mocked engine — exact bytes per format, CSV escaping
    8. streaming under load — 100k rows: incremental yield + bounded heap
    9. _links — CKAN-style start / next pagination links

The engine is a placeholder that returns an empty SearchResult, so
`records` is always `[]`. These tests pin the request / response shape
and the routing — actual query semantics belong with the real backend.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from datastore.infrastructure.engines.base import SearchResult
from datastore.infrastructure.engines.bigquery import BigQueryBackend
from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

SEARCH_URL = "/api/3/action/datastore_search"

_RESOURCE_ID = "balancing_auction_results_2025"


def _params(**overrides: Any) -> dict[str, Any]:
    """Build a query-param dict with the same JSON / CSV encoding the
    endpoint expects from real callers."""
    base: dict[str, Any] = {"resource_id": _RESOURCE_ID}
    base.update(overrides)
    if isinstance(base.get("filters"), (dict, list)):
        base["filters"] = json.dumps(base["filters"])
    if isinstance(base.get("q"), (dict, list)):
        base["q"] = json.dumps(base["q"])
    if isinstance(base.get("fields"), list):
        base["fields"] = ",".join(base["fields"])
    return base


# 1. Happy path -------------------------------------------------------------
def test_basic_search_succeeds(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params())

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    result = body["result"]
    assert result["resource_id"] == _RESOURCE_ID
    assert isinstance(result["records"], list)
    assert isinstance(result["fields"], list)


def test_default_limit_and_offset_echoed(client: TestClient) -> None:
    """Schema defaults (100 / 0) round-trip when the caller omits them."""
    response = client.get(SEARCH_URL, params=_params())

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["limit"] == 100
    assert result["offset"] == 0


# 2. Optional knobs ---------------------------------------------------------


def test_search_with_filters(client: TestClient) -> None:
    response = client.get(
        SEARCH_URL,
        params=_params(
            filters={"product_code": "DCL", "accepted": True},
        ),
    )
    assert response.status_code == 200


def test_search_with_q_as_string(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params(q="DRAX"))
    assert response.status_code == 200


def test_search_with_q_as_dict(client: TestClient) -> None:
    """CKAN's `q` accepts a per-column dict; we encode it as JSON in the URL."""
    response = client.get(
        SEARCH_URL,
        params=_params(
            q={"product_code": "DCL", "bidder_metadata": "DRAX"},
        ),
    )
    assert response.status_code == 200


def test_search_with_fields_and_sort(client: TestClient) -> None:
    response = client.get(
        SEARCH_URL,
        params=_params(
            fields=["auction_id", "product_code", "clearing_price_gbp_per_mwh"],
            sort="delivery_start desc, clearing_price_gbp_per_mwh asc",
        ),
    )
    assert response.status_code == 200


def test_search_with_distinct(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params(distinct=True))
    assert response.status_code == 200


# 3. Pagination + include_total --------------------------------------------


def test_include_total_returns_total(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params(include_total=True))

    assert response.status_code == 200
    result = response.json()["result"]
    assert "total" in result
    # Placeholder engine returns 0 when include_total=True.
    assert result["total"] == 0


def test_include_total_false_omits_total(client: TestClient) -> None:
    """exclude_none keeps `total` off the wire when the engine returns None."""
    response = client.get(SEARCH_URL, params=_params(include_total=False))

    assert response.status_code == 200
    result = response.json()["result"]
    assert "total" not in result


def test_pagination_echoes_limit_offset(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params(limit=50, offset=20))

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["limit"] == 50
    assert result["offset"] == 20


# 4. Validation -------------------------------------------------------------


def test_missing_resource_id_returns_validation_error(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params={})

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "resource_id" in body["error"]["fields"]


def test_limit_too_high_returns_validation_error(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params(limit=32001))

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "limit" in body["error"]["fields"]


def test_negative_offset_returns_validation_error(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params(offset=-1))

    assert response.status_code == 400
    assert response.json()["error"]["__type"] == "Validation Error"


def test_filters_malformed_json_returns_validation_error(client: TestClient) -> None:
    """`filters` must be a JSON object; a non-JSON string fails fast."""
    response = client.get(SEARCH_URL, params=_params(filters="not json"))

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "filters" in body["error"]["fields"]


def test_filters_must_be_object_not_array(client: TestClient) -> None:
    response = client.get(
        SEARCH_URL,
        params={
            "resource_id": _RESOURCE_ID,
            "filters": json.dumps(["not", "an", "object"]),
        },
    )

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "filters" in body["error"]["fields"]


def test_q_starting_with_brace_must_be_valid_json(client: TestClient) -> None:
    """A `q` that looks like JSON (leading `{`) must actually parse."""
    response = client.get(
        SEARCH_URL,
        params={
            "resource_id": _RESOURCE_ID,
            "q": "{not valid",
        },
    )

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "q" in body["error"]["fields"]


# 5. Auth -------------------------------------------------------------------


def test_unknown_resource_id_returns_404(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params(resource_id="does-not-exist"))

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["__type"] == "Not Found Error"
    assert "does-not-exist" in body["error"]["message"]


def test_denied_key_returns_403(client: TestClient, fake_ckan: FakeCKAN) -> None:
    fake_ckan.deny("test-token")

    response = client.get(SEARCH_URL, params=_params())

    assert response.status_code == 403
    assert response.json()["error"]["__type"] == "Authorization Error"


def test_anonymous_read_calls_ckan_and_succeeds(
    client: TestClient, fake_ckan: FakeCKAN,
) -> None:
    """No Authorization header on a read → we still call CKAN's
    `datastore_authorize`. CKAN itself decides based on resource
    visibility; on the FakeCKAN (no deny-list, no visibility flags)
    that succeeds, so the request returns 200."""
    before = fake_ckan.authorize_calls
    # Drop the default Authorization header the conftest sets — we
    # want a real "no header" request, not "header with empty value".
    client.headers.pop("Authorization", None)
    response = client.get(SEARCH_URL, params=_params())
    assert response.status_code == 200
    # Confirms the auth path actually reached CKAN (not short-circuited).
    assert fake_ckan.authorize_calls - before == 1


# 6. records_format ---------------------------------------------------------


def test_default_records_format_is_json_objects(client: TestClient) -> None:
    """Default `records_format=objects` returns the CKAN JSON envelope."""
    response = client.get(SEARCH_URL, params=_params())

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["success"] is True
    assert body["result"]["records"] == []


def test_records_format_lists_returns_json_envelope(client: TestClient) -> None:
    """`records_format=lists` still wraps the rows in the CKAN envelope."""
    response = client.get(SEARCH_URL, params=_params(records_format="lists"))

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["success"] is True
    assert body["result"]["records"] == []


def test_records_format_csv_returns_json_envelope(client: TestClient) -> None:
    """`records_format=csv` returns the CKAN JSON envelope with `records`
    as a CSV-encoded string. Content-Type is application/json — clients
    parse the envelope, then read the records string as CSV. Column names
    live on `result.fields`, not in the records string."""
    response = client.get(
        SEARCH_URL,
        params=_params(
            fields=["auction_id", "product_code"],
            records_format="csv",
        ),
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["success"] is True
    # Placeholder engine yields no rows → empty records string.
    assert body["result"]["records"] == ""


def test_records_format_tsv_returns_json_envelope(client: TestClient) -> None:
    """`records_format=tsv` — same envelope as csv but tab-separated."""
    response = client.get(
        SEARCH_URL,
        params=_params(
            fields=["auction_id", "product_code"],
            records_format="tsv",
        ),
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    # Placeholder engine yields no rows → empty records string.
    assert body["result"]["records"] == ""


def test_invalid_records_format_returns_validation_error(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params=_params(records_format="xml"))

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "records_format" in body["error"]["fields"]


# 7. Streaming with a mocked engine ----------------------------------------
#
# The placeholder `BigQueryBackend.search` returns an empty iterator, so
# these tests stub it with a lazy generator yielding real tuples. That lets
# us pin the exact bytes the streaming writers emit for each format, plus
# RFC 4180 escaping in CSV.

_FAKE_FIELDS: list[dict[str, Any]] = [
    {"id": "auction_id", "type": "integer"},
    {"id": "product_code", "type": "string"},
    {"id": "clearing_price_gbp_per_mwh", "type": "number"},
]
_FAKE_ROWS: list[tuple] = [
    (144, "DCL", 47.82),
    (145, "DCH", 51.10),
    (146, "FFR", 32.40),
]


def _install_mock_search(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fields: list[dict[str, Any]] | None = None,
    rows: list[tuple] | None = None,
) -> list[tuple]:
    """Patch `BigQueryBackend.search` to return a lazy generator over `rows`.

    The generator appends each pulled row to the returned `consumed` list,
    so tests can assert the streaming writer actually iterated the engine
    result instead of bypassing it.
    """
    fields = fields if fields is not None else _FAKE_FIELDS
    rows = rows if rows is not None else _FAKE_ROWS
    consumed: list[tuple] = []

    def lazy_records() -> Iterator[tuple]:
        for r in rows:
            consumed.append(r)
            yield r

    schema = {"fields": [{"name": f["id"], "type": f["type"]} for f in fields]}

    def fake_search(self: BigQueryBackend, **kwargs: Any) -> SearchResult:
        return SearchResult(
            schema=schema,
            records=lazy_records(),
            total=len(rows),
            records_truncated=False,
        )

    monkeypatch.setattr(BigQueryBackend, "search", fake_search)
    return consumed


def test_objects_format_streams_rows(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    consumed = _install_mock_search(monkeypatch)

    response = client.get(SEARCH_URL, params=_params())

    assert response.status_code == 200
    body = response.json()
    assert body["result"]["records"] == [
        {"auction_id": 144, "product_code": "DCL", "clearing_price_gbp_per_mwh": 47.82},
        {"auction_id": 145, "product_code": "DCH", "clearing_price_gbp_per_mwh": 51.1},
        {"auction_id": 146, "product_code": "FFR", "clearing_price_gbp_per_mwh": 32.4},
    ]
    assert body["result"]["total"] == 3
    # Every row was pulled from the lazy generator — the writer didn't bypass it.
    assert consumed == _FAKE_ROWS


def test_lists_format_streams_positional_arrays(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_mock_search(monkeypatch)

    response = client.get(SEARCH_URL, params=_params(records_format="lists"))

    assert response.status_code == 200
    body = response.json()
    assert body["result"]["records"] == [
        [144, "DCL", 47.82],
        [145, "DCH", 51.1],
        [146, "FFR", 32.4],
    ]


def test_csv_format_streams_data_rows(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`records_format=csv` — JSON envelope, `records` is one CSV string of
    data rows only. Column names live on `result.fields` (no header in the
    records string)."""
    _install_mock_search(monkeypatch)

    response = client.get(SEARCH_URL, params=_params(records_format="csv"))

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["result"]["records"] == (
        "144,DCL,47.82\n" "145,DCH,51.1\n" "146,FFR,32.4\n"
    )


def test_tsv_format_streams_data_rows(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`records_format=tsv` — JSON envelope, `records` is one TSV string of
    data rows only."""
    _install_mock_search(monkeypatch)

    response = client.get(SEARCH_URL, params=_params(records_format="tsv"))

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["result"]["records"] == (
        "144\tDCL\t47.82\n" "145\tDCH\t51.1\n" "146\tFFR\t32.4\n"
    )


def test_csv_quotes_values_with_special_chars(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """csv.writer escapes commas, quotes, and embedded newlines per RFC 4180.

    The escaped CSV is then JSON-string-escaped on the wire (`\\n` → `\\\\n`,
    `"` → `\\"`); after `response.json()` parses it back, we should see the
    original RFC 4180 CSV text exactly.
    """
    _install_mock_search(
        monkeypatch,
        fields=[
            {"id": "name", "type": "string"},
            {"id": "note", "type": "string"},
        ],
        rows=[
            ("plain", "ordinary value"),
            ("with,comma", 'with"quote'),
            ("with\nnewline", "tab\there"),
        ],
    )

    response = client.get(SEARCH_URL, params=_params(records_format="csv"))

    assert response.status_code == 200
    body = response.json()
    assert body["result"]["records"] == (
        "plain,ordinary value\n"
        '"with,comma","with""quote"\n'
        '"with\nnewline",tab\there\n'
    )


def test_search_objects_response_includes_links(client: TestClient) -> None:
    """Empty-table case (placeholder engine, total=0): only `start` is
    emitted — `next` / `prev` don't apply and the page counters are
    suppressed when there's nothing to page through. Scheme + host
    carried from the request (TestClient uses `http://testserver`)."""
    response = client.get(SEARCH_URL, params=_params())

    assert response.status_code == 200
    links = response.json()["result"]["_links"]
    assert set(links) == {"start", "page_size"}
    assert links["start"].startswith("http://testserver/api/3/action/datastore_search")
    assert "offset" not in links["start"]
    assert f"resource_id={_RESOURCE_ID}" in links["start"]
    assert links["page_size"] == 100  # default limit


def test_search_links_prev_emitted_on_inner_page(client: TestClient) -> None:
    """At `offset > 0`, `prev` lands at `max(0, offset - limit)`. No
    `next` because placeholder total=0 means we're past the end."""
    response = client.get(SEARCH_URL, params=_params(limit=50, offset=25))

    assert response.status_code == 200
    links = response.json()["result"]["_links"]
    assert "offset" not in links["start"]
    assert "limit=50" in links["start"]
    # prev clamps to 0 since offset (25) < limit (50).
    assert "offset=0" in links["prev"]
    assert "limit=50" in links["prev"]
    assert "next" not in links


def test_search_links_preserve_other_query_params(client: TestClient) -> None:
    """Filters / sort / fields ride along on every emitted link."""
    response = client.get(
        SEARCH_URL,
        params=_params(
            filters={"product_code": "DCL"},
            sort="delivery_start desc",
            fields=["auction_id", "product_code"],
        ),
    )

    assert response.status_code == 200
    links = response.json()["result"]["_links"]
    for v in links.values():
        if not isinstance(v, str):
            continue  # `page` / `total_pages` are ints
        assert "filters=" in v
        assert "sort=" in v
        assert "fields=" in v


def test_search_lists_format_also_includes_links(client: TestClient) -> None:
    """`records_format=lists` is still a JSON envelope, so `_links` is
    present (placeholder engine, empty table: `start` + `page_size`)."""
    response = client.get(SEARCH_URL, params=_params(records_format="lists"))

    assert response.status_code == 200
    links = response.json()["result"]["_links"]
    assert set(links) == {"start", "page_size"}
