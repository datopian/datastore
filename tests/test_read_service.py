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


def test_csv_records_string_is_empty_when_engine_yields_no_rows() -> None:
    """No header row in CSV records — column names live on `result.fields`.
    Placeholder engine yields nothing, so the records string is empty."""
    body = _call(data_dict_overrides={
        "records_format": "csv",
        "fields": "auction_id,product_code",
    })
    assert body["result"]["records"] == ""
    # But the column metadata is echoed on `result.fields`.
    assert [f["id"] for f in body["result"]["fields"]] == [
        "auction_id", "product_code",
    ]


def test_tsv_records_string_is_empty_when_engine_yields_no_rows() -> None:
    body = _call(data_dict_overrides={
        "records_format": "tsv",
        "fields": "auction_id,product_code",
    })
    assert body["result"]["records"] == ""
    assert [f["id"] for f in body["result"]["fields"]] == [
        "auction_id", "product_code",
    ]


# --- search_datastore: pagination links ------------------------------------


def test_links_present_on_every_format() -> None:
    """Placeholder engine returns total=0 → no rows on the page.
    `page` / `total_pages` come through as explicit `null` so clients
    can distinguish "no current page" from "field missing".
    `page_size` is always present when `limit > 0`."""
    for fmt in ("objects", "lists", "csv", "tsv"):
        body = _call(data_dict_overrides={"records_format": fmt})
        links = body["result"]["_links"]
        assert set(links) == {"start", "page_size", "page", "total_pages"}, fmt
        assert links["page_size"] == 100  # default limit
        assert links["page"] is None
        assert links["total_pages"] is None


# --- _build_pagination_links: URL surgery + presence rules -----------------


def test_links_bare_path_url() -> None:
    """Bare path input → bare path output (no scheme/host to preserve).
    With a known `total > offset + limit`, `next` is emitted."""
    links = _build_pagination_links(
        "/api/3/action/datastore_search",
        limit=100, offset=0, total=500,
    )
    assert links["start"] == "/api/3/action/datastore_search"
    assert links["next"] == "/api/3/action/datastore_search?offset=100"


def test_links_strip_offset_from_start() -> None:
    """`start` always drops `offset` (it defaults to 0); `prev` lands
    at `max(0, offset - limit)`; `next` advances by `limit`."""
    links = _build_pagination_links(
        "/api/3/action/datastore_search?resource_id=res-1&offset=50",
        limit=10, offset=50, total=200,
    )
    assert "offset" not in links["start"]
    assert "resource_id=res-1" in links["start"]
    assert "offset=40" in links["prev"]
    assert "offset=60" in links["next"]


def test_links_preserve_other_query_params() -> None:
    """filters, sort, fields ride along on every emitted URL. Page
    counters travel as ints and don't carry params."""
    url = (
        "/api/3/action/datastore_search"
        "?resource_id=res-1&filters=%7B%22a%22%3A1%7D"
        "&sort=created+desc&fields=a,b"
    )
    links = _build_pagination_links(url, limit=20, offset=20, total=100)
    assert set(links) == {
        "start", "prev", "next", "page_size", "page", "total_pages",
    }
    for v in links.values():
        if not isinstance(v, str):
            continue  # `page_size` / `page` / `total_pages` are ints
        assert "filters=" in v
        assert "sort=" in v
        assert "fields=" in v
        assert "resource_id=res-1" in v
    assert links["page_size"] == 20
    assert links["page"] == 2
    assert links["total_pages"] == 5


def test_links_preserve_scheme_and_host_from_full_url() -> None:
    """Full URL input → full URL output (scheme + host carried through)."""
    links = _build_pagination_links(
        "http://example.com/api/3/action/datastore_search?limit=100",
        limit=100, offset=0, total=500,
    )
    assert links["start"].startswith("http://example.com/api/3/action/datastore_search")
    assert links["next"].startswith("http://example.com/api/3/action/datastore_search")
    assert "offset=100" in links["next"]


def test_links_omit_next_when_total_reached() -> None:
    """On the last page (`offset + limit >= total`), `next` is dropped."""
    links = _build_pagination_links(
        "/path", limit=10, offset=90, total=100,
    )
    assert "next" not in links
    assert "prev" in links  # offset > 0


def test_links_omit_next_when_total_unknown() -> None:
    """`include_total=False` → can't tell if a next page exists; drop
    `next` and `total_pages` rather than guess. Clients detect end via
    an empty `records` array. `page` + `page_size` stay since position
    is meaningful for single-page pickers."""
    links = _build_pagination_links(
        "/path", limit=10, offset=0, total=None,
    )
    assert set(links) == {"start", "page_size", "page"}
    assert links["page_size"] == 10
    assert links["page"] == 1
    assert "total_pages" not in links
    assert "next" not in links


def test_links_omit_prev_at_first_page() -> None:
    """`offset == 0` → no previous page exists, so `prev` is dropped."""
    links = _build_pagination_links(
        "/path", limit=10, offset=0, total=100,
    )
    assert "prev" not in links
    assert "next" in links


def test_links_null_page_counters_on_empty_resource() -> None:
    """Empty resource → `page` / `total_pages` are explicit `null`
    (not omitted). `page_size` and `start` are present as usual."""
    links = _build_pagination_links(
        "/path", limit=10, offset=0, total=0,
    )
    assert set(links) == {"start", "page_size", "page", "total_pages"}
    assert links["page_size"] == 10
    assert links["page"] is None
    assert links["total_pages"] is None


def test_links_null_page_counters_when_offset_past_total() -> None:
    """Caller paged past the end (`offset >= total`) → counters would
    lie about position, so they're emitted as explicit `null`. `prev`
    remains so the UI can walk back to a real page."""
    links = _build_pagination_links(
        "/path", limit=100, offset=400, total=302,
    )
    assert links["page"] is None
    assert links["total_pages"] is None
    assert "prev" in links  # offset > 0
    assert "next" not in links  # nothing past the end


def test_links_keep_page_counters_on_real_page() -> None:
    """Within total → page + total_pages reflect a real position."""
    links = _build_pagination_links(
        "/path", limit=100, offset=200, total=302,
    )
    assert links["page"] == 3
    assert links["total_pages"] == 4


def test_links_prev_clamps_to_zero_on_partial_first_page() -> None:
    """Paging back from `offset < limit` must land at offset=0, not a
    negative offset."""
    links = _build_pagination_links(
        "/path", limit=50, offset=20, total=100,
    )
    assert "offset=0" in links["prev"]
