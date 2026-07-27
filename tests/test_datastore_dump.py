"""Tests for `GET /datastore/dump/{resource_id}`.

The engine returns one signed URL (csv / gzip / ndjson shards are
composed into a single object), so every format 302s. Only a sharded
parquet export comes back as several URLs, which the endpoint fetches
and streams as one zip.

We patch `BigQueryBackend.dump` to control what the engine reports, and
`app.state.http` to serve the signed URLs from memory.
"""

from __future__ import annotations

import io
import zipfile
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
from datastore.infrastructure.engines.bigquery import BigQueryBackend
from datastore.infrastructure.engines.bigquery.export import (
    _export_select_list,
    _is_export_too_large,
)
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


def stub_signed_urls(client: TestClient, parts: dict[str, bytes]) -> None:
    """Serve `parts` (url → body) from `app.state.http`, the client the
    zip writer fetches the signed URLs with."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=parts[str(request.url)])

    client.app.state.http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    )


# --- single shard: 302 redirect -------------------------------------------


def test_single_shard_returns_302(client: TestClient) -> None:
    url = "https://storage.googleapis.com/bucket/dumps/x/abc.csv?Sig=abc"
    with _patch_dump([url]):
        response = client.get(DUMP_URL, follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == url
    assert response.content == b""


@pytest.mark.parametrize("fmt", ["csv", "gzip", "ndjson", "parquet"])
def test_each_format_supports_single_shard_redirect(
    fmt: str, client: TestClient,
) -> None:
    with _patch_dump([f"https://example/x.{fmt}"]):
        response = client.get(
            DUMP_URL, params={"format": fmt}, follow_redirects=False,
        )
    assert response.status_code == 302


# --- several shards: one streamed zip -------------------------------------


def test_multi_file_parquet_streams_one_zip(client: TestClient) -> None:
    """A sharded parquet export is fetched back and framed into a single
    zip, so the caller still gets one file from one URL."""
    parts = {
        "https://signed/p0.parquet": b"PAR1-first-shard-bytes",
        "https://signed/p1.parquet": b"PAR1-second-shard-bytes",
    }
    stub_signed_urls(client, parts)

    with _patch_dump(list(parts)):
        response = client.get(DUMP_URL, params={"format": "parquet"})

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert (
        response.headers["content-disposition"]
        == 'attachment; filename="balancing_auction_results_2025.zip"'
    )

    archive = zipfile.ZipFile(io.BytesIO(response.content))
    # `testzip` walks every member and checks its CRC — the framing has
    # to be right, not merely parseable.
    assert archive.testzip() is None
    assert archive.namelist() == [
        "balancing_auction_results_2025_01.parquet",
        "balancing_auction_results_2025_02.parquet",
    ]
    assert [archive.read(n) for n in archive.namelist()] == list(parts.values())


def test_zip_members_are_stored_not_deflated(client: TestClient) -> None:
    """Parquet is already compressed — deflating it would cost CPU for
    no size win, so members go in uncompressed."""
    body = b"PAR1" + b"x" * 4096
    parts = {"https://signed/a.parquet": body, "https://signed/b.parquet": body}
    stub_signed_urls(client, parts)

    with _patch_dump(list(parts)):
        response = client.get(DUMP_URL, params={"format": "parquet"})

    archive = zipfile.ZipFile(io.BytesIO(response.content))
    for info in archive.infolist():
        assert info.compress_type == zipfile.ZIP_STORED
        assert info.compress_size == len(body)


# --- error paths ----------------------------------------------------------


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
    select = _export_select_list(schema, fmt="csv")
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
    assert _export_select_list(schema, fmt="parquet") == "*"


def test_build_export_select_parquet_casts_json_columns() -> None:
    schema = [
        _bq_field("id", "INT64"),
        _bq_field("bidder_metadata", "JSON"),
        _bq_field("delivery_start", "TIMESTAMP"),
    ]
    assert _export_select_list(schema, fmt="parquet") == (
        "`id`, TO_JSON_STRING(`bidder_metadata`) AS `bidder_metadata`, "
        "`delivery_start`"
    )


# --- helpers: too-large heuristic -----------------------------------------


@pytest.mark.parametrize("message", [
    "Operation cannot be completed when exporting to a single URI",
    "Cannot export more than 1 GB to a single URI; use the wildcard operator",
])
def test_too_large_marker_is_recognised(message: str) -> None:
    assert _is_export_too_large(RuntimeError(message)) is True


def test_unrelated_error_is_not_classified_as_too_large() -> None:
    assert _is_export_too_large(RuntimeError("auth failed")) is False


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


def _bq_field(name: str, field_type: str) -> Any:
    f = MagicMock()
    f.name = name
    f.field_type = field_type
    return f


def _fake_blob(name: str, signed_url: str = "https://signed/x") -> Any:
    blob = MagicMock()
    blob.name = name
    blob.generate_signed_url.return_value = signed_url
    return blob


def _run_dump(backend: BigQueryBackend, resource_id: str, fmt: str) -> list[str]:
    """Run `dump()` and flush its background cleanup (compose part
    deletion, revision GC) so delete assertions are deterministic."""
    import asyncio

    async def go() -> list[str]:
        urls = await backend.dump(resource_id, fmt)
        if backend._cleanup_tasks:
            await asyncio.gather(*list(backend._cleanup_tasks))
        return urls

    return asyncio.run(go())


def test_dump_cache_miss_submits_extract_then_returns_urls() -> None:
    """First call to `list_blobs` returns empty (cache miss); `dump()`
    submits the extract, then the csv shards are composed into ONE object
    and its single signed URL is returned."""

    new_blob = _fake_blob("dumps/res-1/csv/<rev>_000.csv", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    # Pre-extract: empty. Post-extract refresh: one shard. GC sweep:
    # same one shard (nothing stale to delete on first dump ever).
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]
    # The composite object's signed URL (csv always composes).
    bucket_obj.blob.return_value.generate_signed_url.return_value = (
        "https://composed"
    )

    # Job goes straight to DONE without errors.
    job = MagicMock()          # `job.result()` returns without raising
    backend.client.query.return_value = job

    urls = _run_dump(backend, "res-1", "csv")

    # csv shards are composed into one object → a single 302 URL.
    assert urls == ["https://composed"]
    bucket_obj.blob.return_value.compose.assert_called()
    # Exactly one extract submitted on cache miss.
    assert backend.client.query.call_count == 1


def test_dump_gzip_exports_headerless_and_composes() -> None:
    """`format=gzip` exports header-less GZIP shards under its own cache
    prefix; the composed object gets one gzip header member in front, so
    the download is a plain 302 with no compression work in the pod."""

    new_blob = _fake_blob("dumps/res-1/gzip/<rev>_000.csv.gz", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]

    job = MagicMock()          # `job.result()` returns without raising
    backend.client.query.return_value = job

    _run_dump(backend, "res-1", "gzip")

    sql = backend.client.query.call_args.args[0]
    assert "format='CSV'" in sql
    assert "compression='GZIP'" in sql
    assert "header=false" in sql          # header comes from the composed member
    assert "_*.csv.gz" in sql
    prefixes = {c.kwargs["prefix"] for c in bucket_obj.list_blobs.call_args_list}
    assert all(p.startswith("dumps/res-1/gzip/") for p in prefixes)


def test_dump_parquet_export_uses_wildcard_uri() -> None:
    """BigQuery SQL EXPORT DATA requires a wildcard URI even for
    formats where this endpoint only accepts a single shard.
    """

    new_blob = _fake_blob("dumps/res-1/parquet/<rev>_000.parquet", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]

    job = MagicMock()          # `job.result()` returns without raising
    backend.client.query.return_value = job

    urls = _run_dump(backend, "res-1", "parquet")

    assert urls == ["https://fresh"]
    sql = backend.client.query.call_args.args[0]
    assert "format='PARQUET'" in sql
    assert "_*.parquet" in sql


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
    first_old = _fake_blob("dumps/res-1/csv/old.composed.csv", "https://old")
    bucket_obj.list_blobs.side_effect = [[first_old], [first_old]]
    asyncio.run(backend.dump("res-1", "csv"))
    # Both calls used the same prefix (the cache-hit rev) — take the
    # earlier one to compare against the next dump's prefix.
    first_prefix = bucket_obj.list_blobs.call_args_list[0].kwargs["prefix"]

    # Bump the table; new call hits a different prefix.
    table.modified = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
    second_new = _fake_blob("dumps/res-1/csv/new.composed.csv", "https://new")
    bucket_obj.list_blobs.side_effect = [[second_new], [second_new]]
    asyncio.run(backend.dump("res-1", "csv"))
    second_prefix = bucket_obj.list_blobs.call_args_list[-2].kwargs["prefix"]

    assert first_prefix != second_prefix, (
        "table.modified change must produce a different cache key"
    )


# --- test infrastructure --------------------------------------------------
