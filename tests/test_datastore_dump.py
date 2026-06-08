"""Tests for `GET /datastore/dump/{resource_id}`.

Engine returns a list of signed-URL shards:
  - len == 1 → endpoint 302s to the URL (no server bandwidth).
  - len > 1  → endpoint stream-concats shards from GCS via async httpx.

We patch `BigQueryBackend.dump` to control how many "shards" the
engine reports, and patch `httpx.AsyncClient` for the stream-concat
tests so they don't try to fetch real URLs.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from datastore.core.exceptions import PayloadTooLargeError, ServerError
from datastore.infrastructure.engines.bigquery import BigQueryBackend
from datastore.infrastructure.engines.bigquery.backend import (
    _build_export_select,
    _is_export_too_large,
)
from datastore.services.dump import _skip_first_line
from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

DUMP_URL = "/datastore/dump/balancing_auction_results_2025"


def _patch_dump(urls_or_exc: list[str] | Exception):
    """Patch `BigQueryBackend.dump` to return URLs or raise."""
    async def fake(self: BigQueryBackend, resource_id: str, fmt: str) -> list[str]:
        if isinstance(urls_or_exc, Exception):
            raise urls_or_exc
        return urls_or_exc
    return patch.object(BigQueryBackend, "dump", fake)


# --- single shard: 302 redirect -------------------------------------------


def test_single_shard_returns_302(client: TestClient) -> None:
    url = "https://storage.googleapis.com/bucket/dumps/x/abc.csv?Sig=abc"
    with _patch_dump([url]):
        response = client.get(DUMP_URL, follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == url
    assert response.content == b""


@pytest.mark.parametrize("fmt", ["csv", "ndjson", "parquet"])
def test_each_format_supports_single_shard_redirect(
    fmt: str, client: TestClient,
) -> None:
    with _patch_dump([f"https://example/x.{fmt}"]):
        response = client.get(
            DUMP_URL, params={"format": fmt}, follow_redirects=False,
        )
    assert response.status_code == 302


# --- multi-shard: stream-concat -------------------------------------------


def test_multi_shard_csv_stream_concat_dedups_header(
    client: TestClient,
) -> None:
    """Header from shard 1 only; shards 2..N have their first line
    dropped before bytes hit the client."""
    shards = {
        "url-1": b"col1,col2\na,1\nb,2\n",
        "url-2": b"col1,col2\nc,3\nd,4\n",
        "url-3": b"col1,col2\ne,5\n",
    }

    with _patch_dump(list(shards.keys())), _patch_httpx_stream(shards):
        response = client.get(DUMP_URL, follow_redirects=False)

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert response.headers["content-disposition"] == (
        'attachment; filename="balancing_auction_results_2025.csv"'
    )
    # Header once, then all rows in order.
    assert response.text.splitlines() == [
        "col1,col2",
        "a,1", "b,2",
        "c,3", "d,4",
        "e,5",
    ]


def test_multi_shard_ndjson_pure_byte_concat(client: TestClient) -> None:
    """Each NDJSON shard is self-contained; bytes concatenate cleanly."""
    shards = {
        "url-1": b'{"id":1}\n{"id":2}\n',
        "url-2": b'{"id":3}\n',
    }
    with _patch_dump(list(shards.keys())), _patch_httpx_stream(shards):
        response = client.get(
            DUMP_URL, params={"format": "ndjson"}, follow_redirects=False,
        )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    assert response.text == (
        '{"id":1}\n{"id":2}\n'
        '{"id":3}\n'
    )


# --- error paths ----------------------------------------------------------


def test_too_large_parquet_returns_413(client: TestClient) -> None:
    with _patch_dump(PayloadTooLargeError("exceeds 1 GB after parquet export")):
        response = client.get(DUMP_URL, params={"format": "parquet"})
    assert response.status_code == 413
    assert response.json()["error"]["__type"] == "Payload Too Large"


def test_unknown_format_returns_validation_error(client: TestClient) -> None:
    response = client.get(DUMP_URL, params={"format": "xml"})
    assert response.status_code == 400
    assert response.json()["error"]["__type"] == "Validation Error"


def test_dump_for_unknown_resource_returns_404(client: TestClient) -> None:
    response = client.get("/datastore/dump/missing-resource")
    assert response.status_code == 404


# --- auth -----------------------------------------------------------------


def test_dump_without_api_key_succeeds_when_public(
    client: TestClient, fake_ckan: FakeCKAN,
) -> None:
    with _patch_dump(["https://example/a.csv?sig=1"]):
        client.headers.pop("Authorization", None)
        response = client.get(DUMP_URL, follow_redirects=False)
    assert response.status_code == 302
    assert fake_ckan.authorize_calls >= 1


def test_dump_with_denied_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN,
) -> None:
    fake_ckan.deny("test-token")
    response = client.get(DUMP_URL)
    assert response.status_code == 403


# --- helpers: ISO date casting --------------------------------------------


def test_build_export_select_iso_casts_timestamp_and_datetime() -> None:
    """TIMESTAMP / DATETIME columns render as `YYYY-MM-DDTHH:MM:SS` —
    no timezone suffix, no fractional seconds. TIMESTAMP is formatted
    in UTC (clients should assume UTC even though the string carries
    no offset)."""
    schema = [
        _bq_field("auction_id", "INT64"),
        _bq_field("delivery_start", "TIMESTAMP"),
        _bq_field("delivery_local", "DATETIME"),
        _bq_field("delivery_day", "DATE"),
    ]
    select = _build_export_select(schema, fmt="csv")
    assert (
        "FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', `delivery_start`, 'UTC')"
        in select
    )
    assert (
        "FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', `delivery_local`)"
        in select
    )
    # No `Z` suffix and no `%E*S` (which would re-introduce fractional seconds).
    assert "Z'," not in select
    assert "%E*S" not in select
    assert "`auction_id`" in select
    assert "`delivery_day`" in select


def test_build_export_select_parquet_returns_star() -> None:
    schema = [_bq_field("delivery_start", "TIMESTAMP")]
    assert _build_export_select(schema, fmt="parquet") == "*"


def _bq_field(name: str, field_type: str) -> Any:
    f = MagicMock()
    f.name = name
    f.field_type = field_type
    return f


# --- helpers: too-large heuristic -----------------------------------------


@pytest.mark.parametrize("message", [
    "Operation cannot be completed when exporting to a single URI",
    "Cannot export more than 1 GB to a single URI; use the wildcard operator",
])
def test_too_large_marker_is_recognised(message: str) -> None:
    assert _is_export_too_large(RuntimeError(message)) is True


def test_unrelated_error_is_not_classified_as_too_large() -> None:
    assert _is_export_too_large(RuntimeError("auth failed")) is False


# --- helpers: CSV header-skip -------------------------------------------


def test_skip_first_line_drops_header_and_forwards_rest() -> None:
    """`_skip_first_line` strips up to and including the first `\\n`,
    then byte-forwards everything else unchanged."""
    import asyncio

    async def chunks() -> AsyncIterator[bytes]:
        yield b"col1,col2\n"
        yield b"a,1\n"
        yield b"b,2\n"

    async def run() -> bytes:
        out = bytearray()
        async for chunk in _skip_first_line(chunks()):
            out.extend(chunk)
        return bytes(out)

    assert asyncio.run(run()) == b"a,1\nb,2\n"


def test_skip_first_line_handles_header_split_across_chunks() -> None:
    """The newline may not arrive in the first chunk — verify the
    buffer accumulates until the newline is found."""
    import asyncio

    async def chunks() -> AsyncIterator[bytes]:
        yield b"col1,"   # no newline yet
        yield b"col2\n"
        yield b"row,1\n"

    async def run() -> bytes:
        out = bytearray()
        async for chunk in _skip_first_line(chunks()):
            out.extend(chunk)
        return bytes(out)

    assert asyncio.run(run()) == b"row,1\n"


# --- engine: placeholder + bucket-missing guards -------------------------


def test_dump_polling_releases_event_loop_between_reloads() -> None:
    """Polling loop should `asyncio.sleep` between `job.reload` calls
    so other coroutines on the same loop keep running. Verified by
    interleaving a ticker — if the dump call hogged the loop, the
    ticker would barely advance during the polls.

    The job is flagged with `error_result` once it transitions to DONE
    so `dump()` raises immediately after the polling loop, without
    reaching the post-extract GCS read."""
    import asyncio

    # `_engine_with_storage` stubs `google.cloud.storage` and gives the
    # backend a real `table.modified` so the pre-extract cache lookup
    # (empty here → cache miss → polling branch) doesn't blow up.
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    # Cache lookup returns no shards → fall through into the extract /
    # poll branch. The post-extract retry is never reached because the
    # job ends with an error.
    bucket_obj.list_blobs.return_value = []

    job = MagicMock()
    job.state = "PENDING"
    reload_calls = 0

    def fake_reload() -> None:
        nonlocal reload_calls
        reload_calls += 1
        if reload_calls >= 3:
            job.state = "DONE"
            # Flag an error so dump raises right after the loop and
            # doesn't try to read the GCS shard list.
            job.error_result = {"message": "test-only error"}

    job.error_result = None
    job.reload = fake_reload
    backend.client.query.return_value = job

    # Speed up the test: 50 ms poll interval.
    with patch(
        "datastore.infrastructure.engines.bigquery.backend"
        "._DUMP_POLL_INTERVAL_SECONDS",
        0.05,
    ):
        async def run() -> int:
            ticks = 0

            async def tick() -> None:
                nonlocal ticks
                while True:
                    await asyncio.sleep(0)
                    ticks += 1

            ticker = asyncio.create_task(tick())
            try:
                with pytest.raises(ServerError, match="test-only error"):
                    await backend.dump("res-1", "csv")
            finally:
                ticker.cancel()
                try:
                    await ticker
                except asyncio.CancelledError:
                    pass
            return ticks

        ticks = asyncio.run(run())
        # 2 sleeps × 50 ms = 100 ms minimum of loop-yielding time;
        # the ticker should rack up far more iterations than the
        # number of reload calls if the loop is genuinely free.
        assert ticks > reload_calls * 10, (
            f"event loop appears blocked during polling: "
            f"only {ticks} ticker passes for {reload_calls} reloads"
        )


# --- engine: GCS-backed cache by table.modified --------------------------


def _engine_with_storage(storage_blobs: list[Any]) -> tuple[BigQueryBackend, Any]:
    """Build a configured BigQueryBackend whose mocked storage client
    returns `storage_blobs` from `list_blobs`. Returns the backend +
    the storage Client mock so callers can assert on it.

    Tests below patch `from google.cloud import storage` (the lazy
    import inside `dump`) so they don't depend on the real package
    being installed in the test env.
    """
    import datetime as dt
    import sys
    import types

    backend = BigQueryBackend(mode="ro")
    backend.client = MagicMock()
    backend.config = MagicMock()
    backend.config.BIGQUERY_PROJECT = "proj-1"
    backend.config.BIGQUERY_DATASET = "ds-1"
    backend.config.BIGQUERY_EXPORT_BUCKET = "bkt"
    backend.config.BIGQUERY_EXPORT_URL_EXPIRY_HOURS = 1
    # Empty creds → load_credentials returns None → storage.Client uses
    # ADC (which we've stubbed via sys.modules below).
    backend.config.BIGQUERY_CREDENTIALS = ""
    backend.config.BIGQUERY_CREDENTIALS_RO = ""

    table = MagicMock()
    table.schema = []
    # Stable `modified` → stable cache key across calls.
    table.modified = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    backend.client.get_table.return_value = table

    storage_client = MagicMock()
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.return_value = list(storage_blobs)

    # Stub the lazy `from google.cloud import storage` inside
    # `_build_storage_client` so test envs without google-cloud-storage
    # still resolve. Both helpers below override the constructor anyway.
    fake_module = types.ModuleType("storage")
    fake_module.Client = MagicMock(return_value=storage_client)
    sys.modules["google.cloud.storage"] = fake_module

    # Inject the same `storage_client` mock for both ro and rw GCS work
    # (a single mock keeps test assertions on `list_blobs` /
    # `bucket.delete` in one place). Inject `backend.client` as the
    # rw BigQuery client so `client.query.return_value = job`
    # assertions still drive the cache-miss extract path.
    backend._build_storage_client = MagicMock(return_value=storage_client)
    backend._build_bq_client = MagicMock(return_value=backend.client)

    return backend, storage_client


def _fake_blob(name: str, signed_url: str = "https://signed/x") -> Any:
    blob = MagicMock()
    blob.name = name
    blob.generate_signed_url.return_value = signed_url
    return blob


def test_dump_cache_hit_skips_extract_job() -> None:
    """When GCS already has shards for this `(rid, fmt, table.modified)`,
    `dump()` returns signed URLs straight from the cache — no
    `client.query` call to BigQuery."""
    import asyncio

    blob = _fake_blob("dumps/res-1/csv/<rev>.csv", "https://cached")
    backend, _ = _engine_with_storage([blob])

    urls = asyncio.run(backend.dump("res-1", "csv"))

    assert urls == ["https://cached"]
    # No extract job submitted — that's the whole point of caching.
    assert backend.client.query.call_count == 0


def test_dump_cache_miss_submits_extract_then_returns_urls() -> None:
    """First call to `list_blobs` returns empty (cache miss);
    `dump()` submits the extract, then `list_blobs` returns the
    written shards on the post-extract retry."""
    import asyncio

    new_blob = _fake_blob("dumps/res-1/csv/<rev>_000.csv", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    # Pre-extract: empty. Post-extract refresh: one shard. GC sweep:
    # same one shard (nothing stale to delete on first dump ever).
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]

    # Job goes straight to DONE without errors.
    job = MagicMock()
    job.state = "DONE"
    job.error_result = None
    backend.client.query.return_value = job

    urls = asyncio.run(backend.dump("res-1", "csv"))

    assert urls == ["https://fresh"]
    # Exactly one extract submitted on cache miss.
    assert backend.client.query.call_count == 1


def test_dump_cache_miss_deletes_older_revisions() -> None:
    """After a successful extract on cache miss, blobs from any older
    revision under `dumps/<rid>/<fmt>/` should be deleted to keep
    storage from growing unbounded across table updates. The current
    revision's blobs stay."""
    import asyncio
    import datetime as dt

    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value

    # Match the rev that backend.dump() computes from table.modified
    # (`_engine_with_storage` sets it to 2026-01-01 UTC).
    table_modified = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    rev = f"{int(table_modified.timestamp() * 1_000_000):x}"
    new_blob = _fake_blob(
        f"dumps/res-1/csv/{rev}_000.csv", "https://fresh"
    )
    old_blob_a = _fake_blob("dumps/res-1/csv/oldrev1_000.csv")
    old_blob_b = _fake_blob("dumps/res-1/csv/oldrev2_000.csv")

    # Calls in order:
    #   1) pre-extract cache lookup (prefix=dumps/.../<rev>) → empty
    #   2) post-extract refresh    (prefix=dumps/.../<rev>) → [new]
    #   3) GC sweep                (prefix=dumps/res-1/csv/) → [new, old_a, old_b]
    bucket_obj.list_blobs.side_effect = [
        [],
        [new_blob],
        [new_blob, old_blob_a, old_blob_b],
    ]

    job = MagicMock()
    job.state = "DONE"
    job.error_result = None
    backend.client.query.return_value = job

    urls = asyncio.run(backend.dump("res-1", "csv"))

    assert urls == ["https://fresh"]
    # The current revision must not be deleted.
    assert new_blob.delete.call_count == 0
    # Both older revisions get cleaned up.
    assert old_blob_a.delete.call_count == 1
    assert old_blob_b.delete.call_count == 1


def test_dump_cache_hit_does_not_delete_anything() -> None:
    """A cache hit must not trigger GC — there's no new revision to
    supersede the existing one."""
    import asyncio

    cached = _fake_blob("dumps/res-1/csv/<rev>_000.csv", "https://cached")
    backend, _ = _engine_with_storage([cached])

    urls = asyncio.run(backend.dump("res-1", "csv"))

    assert urls == ["https://cached"]
    # No extract → no GC.
    assert cached.delete.call_count == 0


def test_dump_cache_key_changes_when_table_modified_advances() -> None:
    """Different `table.modified` → different cache prefix → different
    `list_blobs(prefix=…)` call. Stale cache from an older revision
    can't satisfy a newer request."""
    import asyncio
    import datetime as dt

    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value

    table = backend.client.get_table.return_value
    # Each dump-on-cache-hit lists twice: ro lookup + rw re-fetch for
    # signing. Both calls must return the same blob.
    table.modified = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    first_old = _fake_blob("dumps/res-1/csv/old.csv", "https://old")
    bucket_obj.list_blobs.side_effect = [[first_old], [first_old]]
    asyncio.run(backend.dump("res-1", "csv"))
    # Both calls used the same prefix (the cache-hit rev) — take the
    # earlier one to compare against the next dump's prefix.
    first_prefix = bucket_obj.list_blobs.call_args_list[0].kwargs["prefix"]

    # Bump the table; new call hits a different prefix.
    table.modified = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
    second_new = _fake_blob("dumps/res-1/csv/new.csv", "https://new")
    bucket_obj.list_blobs.side_effect = [[second_new], [second_new]]
    asyncio.run(backend.dump("res-1", "csv"))
    second_prefix = bucket_obj.list_blobs.call_args_list[-2].kwargs["prefix"]

    assert first_prefix != second_prefix, (
        "table.modified change must produce a different cache key"
    )


def test_dump_returns_empty_list_in_placeholder_mode() -> None:
    import asyncio

    backend = BigQueryBackend(mode="ro")
    assert asyncio.run(backend.dump("res-1", "csv")) == []


def test_dump_raises_when_export_bucket_unset() -> None:
    import asyncio

    backend = BigQueryBackend(mode="ro")
    backend.client = MagicMock()
    backend.config = MagicMock()
    backend.config.BIGQUERY_PROJECT = "proj-1"
    backend.config.BIGQUERY_DATASET = "ds-1"
    backend.config.BIGQUERY_EXPORT_BUCKET = ""

    with pytest.raises(ServerError, match="BIGQUERY_EXPORT_BUCKET"):
        asyncio.run(backend.dump("res-1", "csv"))


# --- test infrastructure --------------------------------------------------


def _patch_httpx_stream(shards: dict[str, bytes]):
    """Patch `httpx.AsyncClient.stream` so its async context-manager
    returns a fake `Response` whose `aiter_bytes` walks the bytes for
    that URL. Lets us drive the stream-concat helpers without hitting
    the network."""

    def make_resp(data: bytes) -> Any:
        resp = MagicMock()
        resp.raise_for_status = MagicMock(return_value=None)

        async def aiter_bytes(chunk_size: int = 64 * 1024):
            # Walk the fixture bytes in `chunk_size` slices so callers
            # see real chunk boundaries (matters for the header-skip
            # path which may span chunks).
            for i in range(0, len(data), chunk_size):
                yield data[i:i + chunk_size]

        resp.aiter_bytes = aiter_bytes
        return resp

    class FakeStreamCtx:
        def __init__(self, url: str) -> None:
            self._url = url

        async def __aenter__(self) -> Any:
            return make_resp(shards[self._url])

        async def __aexit__(self, *a: Any) -> None:
            pass

    class FakeClient:
        async def __aenter__(self) -> "FakeClient":
            return self

        async def __aexit__(self, *a: Any) -> None:
            pass

        def stream(self, method: str, url: str) -> FakeStreamCtx:
            return FakeStreamCtx(url)

    return patch(
        "datastore.services.dump.httpx.AsyncClient",
        MagicMock(return_value=FakeClient()),
    )
