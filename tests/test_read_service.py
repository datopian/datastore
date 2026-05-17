"""Unit tests for the read service.

`search_datastore` is exercised directly with a fake context — no HTTP,
no FastAPI. Faster than the TestClient suite and isolates engine call,
format dispatch, envelope assembly, and pagination link building from
the request-plumbing layer.

The BigQuery placeholder is used as-is: it returns an empty `SearchResult`
unless `fields` is supplied (then it echoes the column ids with `type:any`).
That's enough to pin envelope structure and routing; row-shape edge cases
live in `test_datastore_search.py` where a mocked engine yields real rows.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

from datastore.core.config import Config
from datastore.services.read import _build_pagination_links, search_datastore


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(config=Config())


def _data_dict(**overrides: Any) -> dict[str, Any]:
    """Expand schema defaults into a plain dict — the endpoint builds this
    via `params.model_dump()`; here we do it by hand for direct calls."""
    base: dict[str, Any] = dict(
        resource_id="res-1",
        filters=None,
        q=None,
        distinct=False,
        plain=True,
        language="english",
        limit=100,
        offset=0,
        fields=None,
        sort=None,
        include_total=True,
        records_format="objects",
    )
    base.update(overrides)
    return base


def _call(
    *, data_dict_overrides: dict[str, Any] | None = None,
    request_url: str = "http://test/api/3/action/datastore_search?resource_id=res-1",
) -> dict[str, Any]:
    """Run `search_datastore`, drain its iterator, and parse the JSON body."""
    body_iter: Iterator[bytes] = asyncio.run(search_datastore(
        _ctx(),
        _data_dict(**(data_dict_overrides or {})),
        request_url=request_url,
    ))
    body = b"".join(body_iter)
    return json.loads(body)


# --- search_datastore: envelope structure ---------------------------------


def test_returns_valid_json_envelope() -> None:
    body = _call()
    assert body["success"] is True
    assert "help" in body
    assert "result" in body


def test_help_field_equals_request_url() -> None:
    """The envelope's `help` field is the request URL passed in."""
    url = "http://test/api/3/action/datastore_search?resource_id=res-1&limit=10"
    body = _call(request_url=url)
    assert body["help"] == url


def test_resource_id_round_trips() -> None:
    body = _call(data_dict_overrides={"resource_id": "auctions"})
    assert body["result"]["resource_id"] == "auctions"


def test_limit_and_offset_round_trip() -> None:
    body = _call(data_dict_overrides={"limit": 50, "offset": 20})
    assert body["result"]["limit"] == 50
    assert body["result"]["offset"] == 20


def test_include_total_true_emits_total() -> None:
    body = _call(data_dict_overrides={"include_total": True})
    # Placeholder engine yields total=0 when include_total=True.
    assert body["result"]["total"] == 0


def test_include_total_false_omits_total() -> None:
    body = _call(data_dict_overrides={"include_total": False})
    assert "total" not in body["result"]


# --- search_datastore: format dispatch -------------------------------------

def test_objects_format_records_is_array() -> None:
    body = _call(data_dict_overrides={"records_format": "objects"})
    assert isinstance(body["result"]["records"], list)


def test_lists_format_records_is_array() -> None:
    body = _call(data_dict_overrides={"records_format": "lists"})
    assert isinstance(body["result"]["records"], list)


def test_csv_format_records_is_string() -> None:
    """records_format=csv — `records` is a JSON string, not an array."""
    body = _call(data_dict_overrides={"records_format": "csv"})
    assert isinstance(body["result"]["records"], str)


def test_tsv_format_records_is_string() -> None:
    body = _call(data_dict_overrides={"records_format": "tsv"})
    assert isinstance(body["result"]["records"], str)


def test_csv_records_string_contains_header_from_fields() -> None:
    """With `fields=a,b` the placeholder engine echoes those columns; the
    CSV records string must begin with the header row, comma-separated."""
    body = _call(data_dict_overrides={
        "records_format": "csv",
        "fields": "auction_id,product_code",
    })
    assert body["result"]["records"] == "auction_id,product_code\n"


def test_tsv_records_string_contains_tab_separated_header() -> None:
    body = _call(data_dict_overrides={
        "records_format": "tsv",
        "fields": "auction_id,product_code",
    })
    assert body["result"]["records"] == "auction_id\tproduct_code\n"


# --- search_datastore: pagination links ------------------------------------


def test_links_present_on_every_format() -> None:
    for fmt in ("objects", "lists", "csv", "tsv"):
        body = _call(data_dict_overrides={"records_format": fmt})
        assert set(body["result"]["_links"]) == {"start", "next"}, fmt


def test_next_link_advances_offset_by_limit() -> None:
    body = _call(
        data_dict_overrides={"limit": 50, "offset": 25},
        request_url="http://test/api/3/action/datastore_search?limit=50&offset=25",
    )
    links = body["result"]["_links"]
    assert "offset=75" in links["next"]
    assert "limit=50" in links["next"]


# --- _build_pagination_links: URL surgery ----------------------------------


def test_links_bare_url() -> None:
    """No query params except offset: start is just the path, next gets offset."""
    links = _build_pagination_links(
        "/api/3/action/datastore_search", limit=100, offset=0
    )
    assert links["start"] == "/api/3/action/datastore_search"
    assert links["next"] == "/api/3/action/datastore_search?offset=100"


def test_links_strip_offset_from_start() -> None:
    links = _build_pagination_links(
        "/api/3/action/datastore_search?resource_id=res-1&offset=50",
        limit=10, offset=50,
    )
    assert "offset" not in links["start"]
    assert "resource_id=res-1" in links["start"]
    assert "offset=60" in links["next"]


def test_links_preserve_other_query_params() -> None:
    """filters, sort, fields ride along on both start and next."""
    url = (
        "/api/3/action/datastore_search"
        "?resource_id=res-1&filters=%7B%22a%22%3A1%7D"
        "&sort=created+desc&fields=a,b"
    )
    links = _build_pagination_links(url, limit=20, offset=0)
    for link in (links["start"], links["next"]):
        assert "filters=" in link
        assert "sort=" in link
        assert "fields=" in link
        assert "resource_id=res-1" in link


def test_links_strip_scheme_and_host_from_full_url() -> None:
    """`urlparse` keeps only path + query — relative URL on both sides."""
    links = _build_pagination_links(
        "http://example.com/api/3/action/datastore_search?limit=100",
        limit=100, offset=0,
    )
    assert links["start"].startswith("/api/3/action/datastore_search")
    assert links["next"].startswith("/api/3/action/datastore_search")
    assert "http" not in links["start"]
    assert "http" not in links["next"]
