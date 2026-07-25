"""Engine-level tests for `BigQueryBackend.dump_sql` (search_sql downloads).

Reuses `_engine_with_storage` / `_fake_blob` / `_bq_field` from
`tests.test_datastore_dump` — same stubbing pattern: `backend.client` is
a MagicMock, `_build_bq_client` / `_build_storage_client` are
monkeypatched on the instance, and `list_blobs` `side_effect` lists
drive cache hit / miss.

Call order on a cache miss:

    client.query #1 — dry run (kwargs `job_config.dry_run is True`)
    client.query #2 — EXPORT DATA (positional SQL)
    list_blobs    — pre-check (cacheable only) → post-export refresh
                    → GC sweep
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from datastore.core.exceptions import (
    NotFoundError,
    PayloadTooLargeError,
    ServerError,
    ValidationError,
)
from datastore.infrastructure.engines.bigquery import BigQueryBackend
from datastore.infrastructure.engines.bigquery.lib import qualify_table_refs
from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN
from tests.test_datastore_dump import (
    _bq_field,
    _engine_with_storage,
    _fake_blob,
    _patch_httpx_stream,
)

DUMP_SQL_URL = "/datastore/dump/sql"

_NOW = dt.datetime.now(dt.timezone.utc)


def _dry_job(schema: list[Any] | None = None) -> Any:
    """A dry-run query job: only `.schema` matters."""
    job = MagicMock()
    job.schema = schema if schema is not None else [_bq_field("a", "INT64")]
    return job


def _export_job() -> Any:
    """An EXPORT DATA job that completes immediately without errors."""
    job = MagicMock()
    job.state = "DONE"
    job.error_result = None
    return job


def _blob(name: str, url: str = "https://signed/x", age_hours: float = 0.0) -> Any:
    """`_fake_blob` with a real `time_created` so the GC age gate can
    compare it against a datetime cutoff."""
    blob = _fake_blob(name, url)
    blob.time_created = _NOW - dt.timedelta(hours=age_hours)
    return blob


def _run(
    backend: BigQueryBackend,
    sql: str = "SELECT * FROM res1",
    fmt: str = "csv",
    resource_ids: list[str] | None = None,
    function_names: list[str] | None = None,
) -> list[str]:
    return asyncio.run(backend.dump_sql(
        sql,
        fmt,
        resource_ids=["res1"] if resource_ids is None else resource_ids,
        function_names=function_names or [],
    ))


def _expected_prefix(sql: str, fmt: str = "csv") -> str:
    """Recompute the cache prefix the engine derives for the fixture
    config (project `proj-1`, dataset `ds-1`, single table `res1`
    modified 2026-01-01 UTC)."""
    qualified = qualify_table_refs(sql, project="proj-1", dataset="ds-1")
    qhash = hashlib.sha256(qualified.encode()).hexdigest()[:16]
    us = int(
        dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1_000_000
    )
    rev = hashlib.sha256(f"res1:{us}".encode()).hexdigest()[:16]
    return f"sql_dumps/{qhash}/{fmt}/{rev}"


# --- cache behaviour --------------------------------------------------------


def test_cache_hit_skips_dry_run_and_export() -> None:
    """Blobs already under the (qhash, fmt, rev) prefix → signed URLs
    straight from GCS; zero BigQuery jobs."""
    blob = _blob("sql_dumps/h/csv/rev_000.csv", "https://cached")
    backend, _ = _engine_with_storage([blob])

    urls = _run(backend)

    assert urls == ["https://cached"]
    assert backend.client.query.call_count == 0


def test_cache_miss_dry_runs_then_exports() -> None:
    new_blob = _blob("sql_dumps/h/csv/rev_000.csv", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]
    backend.client.query.side_effect = [_dry_job(), _export_job()]

    urls = _run(backend)

    assert urls == ["https://fresh"]
    assert backend.client.query.call_count == 2
    # First call is the RO dry run; second is the EXPORT DATA statement.
    dry_call = backend.client.query.call_args_list[0]
    assert dry_call.kwargs["job_config"].dry_run is True
    export_sql = backend.client.query.call_args_list[1].args[0]
    assert "EXPORT DATA OPTIONS(" in export_sql
    assert "sql_dumps/" in export_sql
    assert "overwrite=true" in export_sql
    # The user SQL rides in subquery position; projection comes from the
    # dry run's output schema; no `_id` ordering is injected.
    assert "FROM (" in export_sql
    assert "`a`" in export_sql
    assert "ORDER BY" not in export_sql


def test_cache_prefix_matches_qhash_and_table_rev_scheme() -> None:
    """Pre-check prefix is `sql_dumps/<sha256(qualified sql)[:16]>/<fmt>/
    <sha256(rid:modified_us pairs)[:16]>`."""
    blob = _blob("x", "https://cached")
    backend, storage_client = _engine_with_storage([blob])
    bucket_obj = storage_client.bucket.return_value

    sql = "SELECT * FROM res1"
    _run(backend, sql=sql)

    pre = bucket_obj.list_blobs.call_args_list[0].kwargs["prefix"]
    assert pre == _expected_prefix(sql)


def test_cache_prefix_stable_across_identical_calls() -> None:
    blob = _blob("x", "https://cached")
    backend, storage_client = _engine_with_storage([blob])
    bucket_obj = storage_client.bucket.return_value

    _run(backend)
    _run(backend)

    prefixes = {
        c.kwargs["prefix"] for c in bucket_obj.list_blobs.call_args_list
    }
    assert len(prefixes) == 1


def test_rev_changes_when_any_referenced_table_changes() -> None:
    """Multi-table SQL: bumping either table's `modified` produces a new
    revision prefix; the other table alone can't satisfy the cache."""
    blob = _blob("x", "https://cached")
    backend, storage_client = _engine_with_storage([blob])
    bucket_obj = storage_client.bucket.return_value

    t1, t2 = MagicMock(), MagicMock()
    t1.modified = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    t2.modified = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)
    sql = "SELECT * FROM r1 JOIN r2 ON true"

    backend.client.get_table.side_effect = [t1, t2]
    _run(backend, sql=sql, resource_ids=["r1", "r2"])
    first = bucket_obj.list_blobs.call_args_list[0].kwargs["prefix"]

    t2_bumped = MagicMock()
    t2_bumped.modified = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    backend.client.get_table.side_effect = [t1, t2_bumped]
    _run(backend, sql=sql, resource_ids=["r1", "r2"])
    second = bucket_obj.list_blobs.call_args_list[-1].kwargs["prefix"]

    assert first != second


def test_non_deterministic_sql_bypasses_cache() -> None:
    """`now()`-style SQL never reads the cache and writes under a fresh
    uuid rev each run — two calls, two exports, two prefixes."""
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    b1 = _blob("sql_dumps/h/csv/u1_000.csv", "https://one")
    b2 = _blob("sql_dumps/h/csv/u2_000.csv", "https://two")
    # No pre-check list per run — only post-export refresh + GC sweep.
    bucket_obj.list_blobs.side_effect = [[b1], [b1], [b2], [b2]]
    backend.client.query.side_effect = [
        _dry_job(), _export_job(), _dry_job(), _export_job(),
    ]

    _run(backend, function_names=["now"])
    _run(backend, function_names=["now"])

    assert backend.client.query.call_count == 4
    refresh_1 = bucket_obj.list_blobs.call_args_list[0].kwargs["prefix"]
    refresh_2 = bucket_obj.list_blobs.call_args_list[2].kwargs["prefix"]
    assert refresh_1 != refresh_2


def test_table_modified_none_is_non_cacheable() -> None:
    """A table without `modified` metadata can't participate in a stable
    rev — treated like non-deterministic SQL (uuid rev per run)."""
    backend, storage_client = _engine_with_storage([])
    backend.client.get_table.return_value.modified = None
    bucket_obj = storage_client.bucket.return_value
    b1 = _blob("y1", "https://one")
    b2 = _blob("y2", "https://two")
    bucket_obj.list_blobs.side_effect = [[b1], [b1], [b2], [b2]]
    backend.client.query.side_effect = [
        _dry_job(), _export_job(), _dry_job(), _export_job(),
    ]

    _run(backend)
    _run(backend)

    assert backend.client.query.call_count == 4
    refresh_1 = bucket_obj.list_blobs.call_args_list[0].kwargs["prefix"]
    refresh_2 = bucket_obj.list_blobs.call_args_list[2].kwargs["prefix"]
    assert refresh_1 != refresh_2


def test_gc_cacheable_removes_all_old_revs_immediately() -> None:
    """A cacheable (table-versioned) export deletes every superseded
    revision on the miss that replaces it — no age gate — so a table
    change promptly reclaims the old export. The current rev stays."""
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value

    sql = "SELECT * FROM res1"
    prefix = _expected_prefix(sql)
    base = prefix.rsplit("/", 1)[0]
    current = _blob(f"{prefix}_000.csv", "https://fresh")
    old_stale = _blob(f"{base}/oldrev_000.csv", age_hours=3.0)
    young_stale = _blob(f"{base}/youngrev_000.csv", age_hours=0.0)

    bucket_obj.list_blobs.side_effect = [
        [],                                  # pre-check (cache miss)
        [current],                           # post-export refresh
        [current, old_stale, young_stale],   # GC sweep
    ]
    backend.client.query.side_effect = [_dry_job(), _export_job()]

    urls = _run(backend, sql=sql)

    assert urls == ["https://fresh"]
    assert current.delete.call_count == 0
    # Both superseded revs go immediately, regardless of age.
    assert old_stale.delete.call_count == 1
    assert young_stale.delete.call_count == 1


def test_gc_stale_blobs_no_age_gate_deletes_all_non_current() -> None:
    """`_gc_stale_blobs(min_age=None)` (the cacheable path) deletes every
    blob outside the current revision, ignoring age."""
    backend = BigQueryBackend(mode="ro")
    keep = "sql_dumps/h/csv/current"
    current = _blob(f"{keep}_000.csv")
    old = _blob("sql_dumps/h/csv/old_000.csv", age_hours=5.0)
    young = _blob("sql_dumps/h/csv/young_000.csv", age_hours=0.0)
    rw_gcs = MagicMock()
    rw_gcs.list_blobs.return_value = [current, old, young]

    deleted = backend._gc_stale_blobs(
        rw_gcs, sweep_prefix="sql_dumps/h/csv/", keep_prefix=keep,
        min_age=None,
    )

    assert deleted == 2
    assert current.delete.call_count == 0
    assert old.delete.call_count == 1
    assert young.delete.call_count == 1


def test_gc_stale_blobs_age_gate_keeps_young() -> None:
    """`_gc_stale_blobs(min_age=…)` (the non-cacheable path) deletes only
    superseded blobs older than the cutoff; younger ones (whose signed
    URLs may still be live) survive."""
    backend = BigQueryBackend(mode="ro")
    keep = "sql_dumps/h/csv/current"
    current = _blob(f"{keep}_000.csv")
    old = _blob("sql_dumps/h/csv/old_000.csv", age_hours=5.0)
    young = _blob("sql_dumps/h/csv/young_000.csv", age_hours=0.0)
    rw_gcs = MagicMock()
    rw_gcs.list_blobs.return_value = [current, old, young]

    deleted = backend._gc_stale_blobs(
        rw_gcs, sweep_prefix="sql_dumps/h/csv/", keep_prefix=keep,
        min_age=dt.timedelta(hours=1),
    )

    assert deleted == 1
    assert current.delete.call_count == 0
    assert old.delete.call_count == 1
    assert young.delete.call_count == 0


# --- output ordering ---------------------------------------------------------


def test_export_orders_by_id_when_output_carries_it() -> None:
    """No user ORDER BY + `_id` in the output → outer `ORDER BY _id`,
    matching the JSON path's default ordering (a subquery's ORDER BY
    without LIMIT is ignored by BigQuery, so it must sit outside)."""
    new_blob = _blob("o_000.csv", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]
    backend.client.query.side_effect = [
        _dry_job([_bq_field("_id", "INT64"), _bq_field("a", "INT64")]),
        _export_job(),
    ]

    _run(backend, sql="SELECT * FROM res1")

    export_sql = backend.client.query.call_args_list[1].args[0]
    assert export_sql.endswith(") ORDER BY `_id`")


def test_export_hoists_user_order_by_to_outer_query() -> None:
    """A user ORDER BY on output columns is copied to the outer query so
    the file (and shard concat) actually follows it."""
    new_blob = _blob("o_000.csv", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]
    backend.client.query.side_effect = [
        _dry_job([_bq_field("a", "INT64")]),
        _export_job(),
    ]

    _run(backend, sql="SELECT a FROM res1 ORDER BY a DESC")

    export_sql = backend.client.query.call_args_list[1].args[0]
    # The outer (post-subquery) ORDER BY carries the user's key; sqlglot
    # renders the postgres NULLS ordering explicitly for BigQuery.
    outer = export_sql.rsplit(")", 1)[1]
    assert "ORDER BY `a` DESC" in outer


def test_export_skips_outer_order_for_non_output_sort_keys() -> None:
    """ORDER BY on a column that isn't in the output can't be hoisted
    past the subquery boundary — the export stays unordered rather than
    failing."""
    new_blob = _blob("o_000.csv", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]
    backend.client.query.side_effect = [
        _dry_job([_bq_field("a", "INT64")]),
        _export_job(),
    ]

    _run(backend, sql="SELECT a FROM res1 ORDER BY b")

    export_sql = backend.client.query.call_args_list[1].args[0]
    # The inner ORDER BY b remains inside the parens; no outer ORDER BY.
    assert export_sql.rstrip().endswith(")")


# --- per-format export SQL --------------------------------------------------


def test_parquet_export_casts_json_columns_from_dry_run_schema() -> None:
    """BigQuery can't export native JSON to parquet — the dry run's
    output schema drives a TO_JSON_STRING cast."""
    new_blob = _blob("z_000.parquet", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]
    backend.client.query.side_effect = [
        _dry_job([_bq_field("id", "INT64"), _bq_field("meta", "JSON")]),
        _export_job(),
    ]

    _run(backend, fmt="parquet")

    export_sql = backend.client.query.call_args_list[1].args[0]
    assert "format='PARQUET'" in export_sql
    assert "TO_JSON_STRING(`meta`)" in export_sql


def test_csv_export_iso_casts_timestamps_from_dry_run_schema() -> None:
    """CSV downloads render TIMESTAMP identically to `datastore_search`
    and `/datastore/dump` (shared `format_select_column`)."""
    new_blob = _blob("z_000.csv", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]
    backend.client.query.side_effect = [
        _dry_job([_bq_field("ts", "TIMESTAMP")]),
        _export_job(),
    ]

    _run(backend, fmt="csv")

    export_sql = backend.client.query.call_args_list[1].args[0]
    assert "format='CSV'" in export_sql
    assert "header=true" in export_sql
    assert (
        "FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', `ts`, 'UTC')" in export_sql
    )


def test_multi_shard_parquet_raises_413() -> None:
    shards = [
        _blob("p_000.parquet"),
        _blob("p_001.parquet"),
    ]
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], shards, shards]
    backend.client.query.side_effect = [_dry_job(), _export_job()]

    with pytest.raises(PayloadTooLargeError, match="multiple parquet shards"):
        _run(backend, fmt="parquet")


# --- guards & error mapping --------------------------------------------------


def test_placeholder_mode_returns_empty_list() -> None:
    backend = BigQueryBackend(mode="ro")
    urls = asyncio.run(backend.dump_sql(
        "SELECT 1", "csv", resource_ids=[], function_names=[],
    ))
    assert urls == []


def test_non_ro_mode_rejected() -> None:
    backend = BigQueryBackend(mode="rw")
    backend.client = MagicMock()
    with pytest.raises(ServerError, match="read-only"):
        asyncio.run(backend.dump_sql(
            "SELECT 1", "csv", resource_ids=[], function_names=[],
        ))


def test_bucket_unset_raises_server_error() -> None:
    backend = BigQueryBackend(mode="ro")
    backend.client = MagicMock()
    backend.config = MagicMock()
    backend.config.BIGQUERY_EXPORT_BUCKET = ""
    with pytest.raises(ServerError, match="BIGQUERY_EXPORT_BUCKET"):
        asyncio.run(backend.dump_sql(
            "SELECT 1", "csv", resource_ids=[], function_names=[],
        ))


def test_missing_table_raises_not_found() -> None:
    from google.api_core.exceptions import NotFound

    backend, _ = _engine_with_storage([])
    backend.client.get_table.side_effect = NotFound("gone")
    with pytest.raises(NotFoundError, match="res1"):
        _run(backend)


def test_dry_run_failure_maps_to_validation_error() -> None:
    """SQL that doesn't compile against the real schema fails the RO dry
    run → clean 400, before any RW/export work."""
    backend, storage_client = _engine_with_storage([])
    storage_client.bucket.return_value.list_blobs.side_effect = [[]]
    backend.client.query.side_effect = RuntimeError("Unrecognized name: nope")
    with pytest.raises(ValidationError, match="BigQuery validation"):
        _run(backend)


def test_zero_table_sql_exports_without_get_table() -> None:
    """`SELECT 1`-style queries reference no tables: no `get_table`
    calls, stable empty-pairs rev, export still runs."""
    new_blob = _blob("s_000.csv", "https://fresh")
    backend, storage_client = _engine_with_storage([])
    bucket_obj = storage_client.bucket.return_value
    bucket_obj.list_blobs.side_effect = [[], [new_blob], [new_blob]]
    backend.client.query.side_effect = [_dry_job(), _export_job()]

    urls = _run(backend, sql="SELECT 1", resource_ids=[])

    assert urls == ["https://fresh"]
    assert backend.client.get_table.call_count == 0


# =============================================================================
# Endpoint: GET /datastore/dump/sql
# =============================================================================


def _patch_dump_sql(urls_or_exc: list[str] | Exception):
    """Patch `BigQueryBackend.dump_sql` to return URLs or raise.

    Returns `(patcher, calls)` — `calls` collects the kwargs of every
    invocation so tests can assert the endpoint forwards the parsed SQL
    facts verbatim."""
    calls: list[dict[str, Any]] = []

    async def fake(
        self: BigQueryBackend,
        sql: str,
        fmt: str,
        *,
        resource_ids: list[str],
        function_names: list[str],
    ) -> list[str]:
        calls.append({
            "sql": sql,
            "fmt": fmt,
            "resource_ids": resource_ids,
            "function_names": function_names,
        })
        if isinstance(urls_or_exc, Exception):
            raise urls_or_exc
        return urls_or_exc

    return patch.object(BigQueryBackend, "dump_sql", fake), calls


def test_single_shard_redirects(client: TestClient) -> None:
    url = "https://storage.googleapis.com/bkt/sql_dumps/x.csv?Sig=1"
    patcher, _ = _patch_dump_sql([url])
    with patcher:
        response = client.get(
            DUMP_SQL_URL,
            params={"sql": "SELECT 1 LIMIT 10", "format": "csv"},
            follow_redirects=False,
        )
    assert response.status_code == 302
    assert response.headers["location"] == url


def test_forwards_sql_and_names_verbatim(
    client: TestClient, fake_ckan: FakeCKAN,
) -> None:
    """The engine receives the SQL exactly as sent (LIMIT intact) plus
    the schema-parsed table / function names."""
    patcher, calls = _patch_dump_sql(["https://x/1.json"])
    sql = 'SELECT count(*) FROM "balancing_auction_results_2025" LIMIT 7'
    with patcher:
        response = client.get(
            DUMP_SQL_URL,
            params={"sql": sql, "format": "ndjson"},
            follow_redirects=False,
        )
    assert response.status_code == 302
    assert calls == [{
        "sql": sql,
        "fmt": "ndjson",
        "resource_ids": ["balancing_auction_results_2025"],
        "function_names": ["count"],
    }]


def test_format_defaults_to_csv(client: TestClient) -> None:
    patcher, calls = _patch_dump_sql(["https://x/1.csv"])
    with patcher:
        response = client.get(
            DUMP_SQL_URL,
            params={"sql": "SELECT 1 LIMIT 10"},
            follow_redirects=False,
        )
    assert response.status_code == 302
    assert calls[0]["fmt"] == "csv"


def test_limit_is_optional(client: TestClient) -> None:
    """No LIMIT → the full result set is exported."""
    patcher, _ = _patch_dump_sql(["https://x/1.csv"])
    with patcher:
        response = client.get(
            DUMP_SQL_URL,
            params={"sql": "SELECT 1"},
            follow_redirects=False,
        )
    assert response.status_code == 302


def test_limit_above_search_cap_allowed(client: TestClient) -> None:
    """The SEARCH_RESULT_ROWS_MAX clamp is a JSON-envelope rule; a dump
    honors the SQL's LIMIT as written."""
    patcher, calls = _patch_dump_sql(["https://x/1.csv"])
    with patcher:
        response = client.get(
            DUMP_SQL_URL,
            params={"sql": "SELECT 1 LIMIT 50000"},
            follow_redirects=False,
        )
    assert response.status_code == 302
    assert "LIMIT 50000" in calls[0]["sql"]


def test_offset_without_limit_rejected(client: TestClient) -> None:
    response = client.get(DUMP_SQL_URL, params={
        "sql": "SELECT 1 OFFSET 10",
    })
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "OFFSET" in body["error"]["message"]


def test_bogus_format_rejected(client: TestClient) -> None:
    response = client.get(DUMP_SQL_URL, params={
        "sql": "SELECT 1 LIMIT 5", "format": "xml",
    })
    assert response.status_code == 400
    assert response.json()["error"]["__type"] == "Validation Error"


def test_missing_sql_names_the_field(client: TestClient) -> None:
    """`/datastore/dump/sql` resolves to the SQL route (declared before
    `/{resource_id}`, so `sql` is a reserved resource name) — a missing
    `sql` param is a validation error on this endpoint, not a 404 dump
    of a table called 'sql'."""
    response = client.get(DUMP_SQL_URL)
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["__type"] == "Validation Error"
    assert "sql" in body["error"]["fields"]


def test_multi_shard_csv_streams_one_file(client: TestClient) -> None:
    """N shard URLs → one streamed CSV body with the header deduped and
    a `query_<hash8>.csv` attachment filename."""
    shards = {
        "url-1": b"c1,c2\na,1\n",
        "url-2": b"c1,c2\nb,2\n",
    }
    patcher, _ = _patch_dump_sql(list(shards.keys()))
    sql = "SELECT 1 LIMIT 5"
    with patcher, _patch_httpx_stream(shards):
        response = client.get(
            DUMP_SQL_URL,
            params={"sql": sql, "format": "csv"},
            follow_redirects=False,
        )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    qhash8 = hashlib.sha256(sql.encode()).hexdigest()[:8]
    assert response.headers["content-disposition"] == (
        f'attachment; filename="query_{qhash8}.csv"'
    )
    assert response.text.splitlines() == ["c1,c2", "a,1", "b,2"]


def test_multi_shard_ndjson_streams_one_file(client: TestClient) -> None:
    shards = {
        "url-1": b'{"id":1}\n',
        "url-2": b'{"id":2}\n',
    }
    patcher, _ = _patch_dump_sql(list(shards.keys()))
    with patcher, _patch_httpx_stream(shards):
        response = client.get(
            DUMP_SQL_URL,
            params={"sql": "SELECT 1 LIMIT 5", "format": "ndjson"},
            follow_redirects=False,
        )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    assert response.text == '{"id":1}\n{"id":2}\n'


def test_parquet_too_large_returns_413(client: TestClient) -> None:
    patcher, _ = _patch_dump_sql(
        PayloadTooLargeError("exported as multiple parquet shards"),
    )
    with patcher:
        response = client.get(DUMP_SQL_URL, params={
            "sql": "SELECT 1 LIMIT 5", "format": "parquet",
        })
    assert response.status_code == 413
    assert response.json()["error"]["__type"] == "Payload Too Large"


def test_disallowed_function_rejected_before_engine(
    client: TestClient,
) -> None:
    """The function allow-list applies here too — the service raises
    before any engine/export work."""
    response = client.get(DUMP_SQL_URL, params={
        "sql": "SELECT pg_read_file('/etc/passwd') LIMIT 1",
    })
    assert response.status_code == 400
    assert "pg_read_file" in response.json()["error"]["message"].lower()


def test_unknown_table_returns_404(
    client: TestClient, fake_ckan: FakeCKAN,
) -> None:
    response = client.get(DUMP_SQL_URL, params={
        "sql": 'SELECT * FROM "does-not-exist" LIMIT 10',
    })
    assert response.status_code == 404


def test_denied_key_returns_403(
    client: TestClient, fake_ckan: FakeCKAN,
) -> None:
    fake_ckan.deny("test-token")
    response = client.get(DUMP_SQL_URL, params={
        "sql": 'SELECT * FROM "balancing_auction_results_2025" LIMIT 10',
    })
    assert response.status_code == 403


def test_join_authorizes_each_table(
    client: TestClient, fake_ckan: FakeCKAN,
) -> None:
    fake_ckan.add_resource("other_table", package_id="pkg-balancing-2025")
    before = fake_ckan.authorize_calls
    patcher, _ = _patch_dump_sql(["https://x/1.csv"])
    with patcher:
        response = client.get(
            DUMP_SQL_URL,
            params={
                "sql": (
                    'SELECT a.id FROM "balancing_auction_results_2025" a '
                    'JOIN "other_table" b ON a.id = b.id LIMIT 10'
                ),
            },
            follow_redirects=False,
        )
    assert response.status_code == 302
    assert fake_ckan.authorize_calls - before == 2


def test_unconfigured_engine_returns_500(client: TestClient) -> None:
    """Placeholder engine (no BQ creds in the test env) exports nothing;
    the endpoint refuses to serve an empty file and 500s explicitly."""
    response = client.get(DUMP_SQL_URL, params={
        "sql": "SELECT 1 LIMIT 5",
    })
    assert response.status_code == 500
    assert "not configured" in response.json()["error"]["message"]
