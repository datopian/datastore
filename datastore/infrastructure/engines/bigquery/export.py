"""BigQuery download pipeline — `<API_BASE_PREFIX>/dump/{rid}` + `<API_BASE_PREFIX>/dump/query`.

Read top-down: entry points, then the workflow they share, then the
helpers each step uses.

    1. ENTRY POINTS   `dump` (whole table) · `dump_sql` (vetted SELECT)
                      — derive the cache key + export SQL, nothing else
    2. WORKFLOW       `_prepare_download`:
                          HIT?  newest attempt with a `_SUCCESS` marker
                                └─ yes ──▶ sign → 302 (no BigQuery)
                          EXPORT   EXPORT DATA → shards, own attempt dir
                          COMPOSE  csv/ndjson shards → ONE object
                          PUBLISH  write `_SUCCESS`
                          GC       old attempts/revisions — BACKGROUND
                          SIGN     V4 URL(s) → 302
    3. STEP HELPERS   one per workflow step, in the order used
    4. SQL BUILDERS   pure text/bytes helpers
    5. BACKEND        small accessors over the engine instance

Functions take the `BigQueryBackend` instance (`backend`) explicitly;
`google.cloud` imports stay lazy (optional dep).
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from datastore.core.exceptions import (
    NotFoundError,
    PayloadTooLargeError,
    ServerError,
    ValidationError,
)
from datastore.infrastructure.engines.bigquery.lib import (
    format_select_column,
    qualify_table_refs,
)

log = logging.getLogger(__name__)

# Per-format file extension + BigQuery EXPORT DATA `format` value.
_FMT: dict[str, dict[str, str]] = {
    "csv": {"ext": "csv", "bq": "CSV"},
    "gzip": {"ext": "csv.gz", "bq": "CSV"},
    "ndjson": {"ext": "json", "bq": "JSON"},
    "parquet": {"ext": "parquet", "bq": "PARQUET"},
}

# Content type stamped on the composed object.
_CONTENT_TYPE = {
    "csv": "text/csv",
    "gzip": "application/gzip",
    "ndjson": "application/x-ndjson",
}

# GCS `compose` accepts ≤32 sources per call; more are chained.
_COMPOSE_MAX_SOURCES = 32

# Written last, once the bytes are final. An attempt without it is
# invisible — mid-export or abandoned, either way not servable.
_SUCCESS = "_SUCCESS"

# Bucket root. Everything this service writes lives under one prefix, so
# a lifecycle rule can target it directly. Table dumps key on the
# resource id, query dumps on a SQL hash — both sit directly beneath.
_EXPORT_ROOT = "dumps"

# SQL functions whose results change per run → the cache must be bypassed.
_NON_DETERMINISTIC_SQL_FUNCTIONS = frozenset(
    {
        "now",
        "current_date",
        "current_datetime",
        "current_time",
        "current_timestamp",
        "rand",
        "generate_uuid",
        "session_user",
        "current_user",
    }
)

# Formats that survive a single-file download — named in 413 messages.
_FORMAT_HINT = "`format=csv` or `format=ndjson`"


# ============================================================================
# 1. ENTRY POINTS
# ============================================================================


async def dump(backend: Any, resource_id: str, fmt: str) -> list[str]:
    """Whole-table download → signed URLs. Cache key
    `dumps/<rid>/<fmt>/<rev>/` with `rev` = `table.modified`."""
    if backend.client is None:
        return []

    bucket = _get_export_bucket(backend)
    table = await _get_table(backend, resource_id)

    # No `modified` → no stable version to key on, so the cache can't be
    # read and every request exports.
    cacheable = table.modified is not None
    rev = (
        f"{int(table.modified.timestamp() * 1_000_000):x}"
        if cacheable
        else uuid4().hex[:12]
    )
    prefix = f"{_EXPORT_ROOT}/{resource_id}/{fmt}/{rev}/"
    source = (
        f"`{backend.config.BIGQUERY_PROJECT}"
        f".{backend.config.BIGQUERY_DATASET}.{resource_id}`"
    )

    async def build_export_sql(uri: str) -> tuple[str, bytes | None]:
        # Schema is already known from get_table — no dry run needed.
        sql = _export_data_sql(
            uri,
            fmt,
            _export_select_list(table.schema, fmt),
            source=source,
            suffix=" ORDER BY `_id`",
        )
        header = (
            _csv_header_bytes(table.schema, fmt)
            if fmt in ("csv", "gzip")
            else None
        )
        return sql, header

    return await _prepare_download(
        backend,
        bucket=bucket,
        prefix=prefix,
        fmt=fmt,
        cacheable=cacheable,
        build_export_sql=build_export_sql,
        sweep_prefix=f"{_EXPORT_ROOT}/{resource_id}/{fmt}/",
        filename_base=resource_id,
        what=f"resource {resource_id!r}",
    )


async def dump_sql(
    backend: Any,
    sql: str,
    fmt: str,
    *,
    resource_ids: list[str],
    function_names: list[str],
) -> list[str]:
    """SQL-result download → signed URLs.

    Cache key `dumps/<qhash>/<fmt>/<rev>/`: `qhash` = hash of the
    qualified SQL, `rev` = hash of every referenced table's `modified`.
    Non-deterministic SQL (`now()`, …) is never cached (uuid rev). A
    free RO dry run validates the SQL and yields the output schema for
    the per-format casts; the export wraps the user SQL as a subquery.
    """
    if backend.client is None:
        return []
    if backend.mode != "ro":
        raise ServerError(
            "datastore_search_sql download must run on a read-only "
            "engine; got mode=" + repr(backend.mode)
        )

    from google.cloud import bigquery

    bucket = _get_export_bucket(backend)

    try:
        qualified_sql = qualify_table_refs(
            sql,
            project=backend.config.BIGQUERY_PROJECT,
            dataset=backend.config.BIGQUERY_DATASET,
        )
    except Exception as e:
        raise ServerError(f"failed to qualify table references in SQL: {e}") from e

    tables = {rid: await _get_table(backend, rid) for rid in resource_ids}

    # Deterministic SQL over tables with known versions → stable rev.
    cacheable = not (set(function_names) & _NON_DETERMINISTIC_SQL_FUNCTIONS) and all(
        t.modified is not None for t in tables.values()
    )
    if cacheable:
        pairs = sorted(
            (rid, int(t.modified.timestamp() * 1_000_000)) for rid, t in tables.items()
        )
        rev = hashlib.sha256(
            "|".join(f"{rid}:{us}" for rid, us in pairs).encode()
        ).hexdigest()[:16]
    else:
        rev = uuid4().hex[:12]

    qhash = hashlib.sha256(qualified_sql.encode()).hexdigest()[:16]
    prefix = f"{_EXPORT_ROOT}/{qhash}/{fmt}/{rev}/"

    async def build_export_sql(uri: str) -> tuple[str, bytes | None]:
        # RO dry run (free): validates the SQL + yields the output
        # schema. Deferred — a cache hit never pays for it.
        dry_cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        try:
            dry_job = await asyncio.to_thread(
                backend.client.query,
                qualified_sql,
                job_config=dry_cfg,
            )
        except Exception as e:
            raise ValidationError(f"sql failed BigQuery validation: {e}") from e
        export_sql = _export_data_sql(
            uri,
            fmt,
            _export_select_list(dry_job.schema, fmt),
            source=f"({qualified_sql})",
            suffix=_outer_order_by(qualified_sql, dry_job.schema),
        )
        header = (
            _csv_header_bytes(dry_job.schema, fmt)
            if fmt in ("csv", "gzip")
            else None
        )
        return export_sql, header

    return await _prepare_download(
        backend,
        bucket=bucket,
        prefix=prefix,
        fmt=fmt,
        cacheable=cacheable,
        build_export_sql=build_export_sql,
        sweep_prefix=f"{_EXPORT_ROOT}/{qhash}/{fmt}/",
        filename_base=f"query_{qhash[:8]}",
        what=f"sql query {qhash[:8]}",
    )


# ============================================================================
# 2. WORKFLOW
# ============================================================================


async def _prepare_download(
    backend: Any,
    *,
    bucket: str,
    prefix: str,
    fmt: str,
    cacheable: bool,
    build_export_sql: Callable[[str], Awaitable[tuple[str, bytes | None]]],
    sweep_prefix: str,
    filename_base: str,
    what: str,
) -> list[str]:
    """The download workflow shared by `dump` and `dump_sql`:

        list prefix → an attempt with `_SUCCESS`?  sign its files
                    → otherwise  export · compose · publish · GC

    Every export writes into its own `<prefix><attempt>/`, and an attempt
    is invisible until its `_SUCCESS` lands. Two requests for the same
    (query, table version) therefore cost a duplicate export at worst —
    neither can overwrite the other's objects or read a half-written one.
    """
    ro_gcs = backend._build_storage_client("ro").bucket(bucket)
    rw_gcs = backend._build_storage_client("rw").bucket(bucket)

    blobs: list[Any] = []
    if cacheable and _complete_attempt(await _list_files_sorted(ro_gcs, prefix), fmt):
        # Re-list via rw: signing needs rw creds, and a blob signs through
        # whichever client listed it.
        blobs = _complete_attempt(await _list_files_sorted(rw_gcs, prefix), fmt) or []

    if blobs:
        log.info("BigQuery export cache HIT: %s prefix=%s", what, prefix)
    else:
        attempt = f"{prefix}{uuid4().hex[:12]}/"
        rw_bq = backend._build_bq_client("rw")
        export_sql, header_bytes = await build_export_sql(
            f"gs://{bucket}/{attempt}part_*.{_FMT[fmt]['ext']}"
        )
        await _run_export_job(rw_bq, export_sql, what=what, fmt=fmt)
        log.info("BigQuery export cache MISS: %s attempt=%s", what, attempt)
        blobs = await _list_files_sorted(rw_gcs, attempt)
        if not blobs:
            raise ServerError(
                f"BigQuery EXPORT DATA wrote no shards for {what}; " "check job logs."
            )

        blobs = await _compose_single_file(
            backend, rw_gcs, attempt, fmt, blobs, header_bytes
        )

        # Publishes the attempt. Written after the compose, so it can
        # never be a compose source.
        await asyncio.to_thread(
            rw_gcs.blob(f"{attempt}{_SUCCESS}").upload_from_string, b""
        )

        def gc() -> None:
            removed = _delete_old_cache(
                rw_gcs,
                sweep_prefix=sweep_prefix,
                keep_prefix=attempt,
                min_age=_url_expiry(backend),
            )
            if removed:
                log.info("BigQuery export GC: %s removed=%d", what, removed)

        _cleanup_in_background(backend, gc, what=f"old cache {what}")

    return await _signed_urls(
        backend,
        blobs,
        filename_base=filename_base,
        ext=_FMT[fmt]["ext"],
    )


# ============================================================================
# 3. STEP HELPERS (in workflow order)
# ============================================================================


async def _list_files_sorted(bucket: Any, prefix: str) -> list[Any]:
    """`list_blobs(prefix=…)` off-thread, name-sorted (= shard order)."""
    return sorted(
        await asyncio.to_thread(
            lambda: list(bucket.list_blobs(prefix=prefix)),
        ),
        key=lambda x: x.name,
    )


def _complete_attempt(blobs: list[Any], fmt: str) -> list[Any] | None:
    """Files of the newest attempt carrying `_SUCCESS`, else `None`.

    A listing of `<prefix>` spans every attempt made for this (query,
    table version). Only a published one counts, so an in-flight or
    abandoned attempt can't be served or mistaken for a finished one.

    Within the winner, the composed `data.<ext>` is the whole download
    when it exists; `part_*` shards beside it are leftovers awaiting
    background deletion. Parquet never composes, so its shards *are* the
    download.
    """
    attempts: dict[str, list[Any]] = {}
    for blob in blobs:
        attempts.setdefault(blob.name.rsplit("/", 1)[0], []).append(blob)

    published = {
        name: objs
        for name, objs in attempts.items()
        if any(o.name.endswith(f"/{_SUCCESS}") for o in objs)
    }
    if not published:
        return None

    newest = max(published, key=lambda a: _published_at(published[a]))
    files = [o for o in published[newest] if not o.name.endswith(f"/{_SUCCESS}")]
    composed = [o for o in files if o.name.endswith(f"/data.{_FMT[fmt]['ext']}")]
    return composed or files


def _published_at(objs: list[Any]) -> datetime:
    """When an attempt's `_SUCCESS` landed — picks between attempts that
    both finished."""
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    for obj in objs:
        if obj.name.endswith(f"/{_SUCCESS}"):
            created = getattr(obj, "time_created", None)
            return created if isinstance(created, datetime) else epoch
    return epoch


async def _run_export_job(
    rw_bq: Any,
    export_sql: str,
    *,
    what: str,
    fmt: str,
) -> None:
    """Run `EXPORT DATA` to completion; >1 GB → 413, other errors → 500.

    `job.result()` does the waiting (the client polls with backoff), so
    it holds one worker thread for the length of the export.
    """
    t0 = time.perf_counter()
    try:
        job = await asyncio.to_thread(rw_bq.query, export_sql)
        await asyncio.to_thread(job.result)
    except Exception as e:
        if _is_export_too_large(e):
            raise PayloadTooLargeError(
                f"{what} exceeds 1 GB after export as {fmt!r}; "
                "single-file download isn't possible. "
                f"Try {_FORMAT_HINT} for sharded multi-file downloads "
                "instead."
            ) from e
        raise ServerError(f"BigQuery EXPORT DATA failed for {what}: {e}") from e

    log.debug(
        "BigQuery export: %s in %.2fs",
        what,
        time.perf_counter() - t0,
    )


async def _compose_single_file(
    backend: Any,
    rw_gcs: Any,
    attempt: str,
    fmt: str,
    shards: list[Any],
    header_bytes: bytes | None,
) -> list[Any]:
    """Compose csv/ndjson shards (+ csv header) into ONE GCS object →
    `[composite]` → the endpoint 302s a single URL. csv always composes
    (to inject the header); ndjson only when >1 shard; gzip/parquet pass
    through. Source shards are deleted in the background afterwards."""
    composable = fmt in ("csv", "gzip") or (fmt == "ndjson" and len(shards) > 1)
    if not composable:
        return shards  # lone ndjson shard / gzip / parquet

    ext = _FMT[fmt]["ext"]
    content_type = _CONTENT_TYPE[fmt]

    def compose() -> tuple[Any, list[Any]]:
        sources = list(shards)
        extras: list[Any] = []

        if header_bytes is not None:  # csv header member, sorts first
            header = rw_gcs.blob(f"{attempt}header.{ext}")
            header.upload_from_string(header_bytes, content_type=content_type)
            sources = [header, *sources]
            extras.append(header)

        composite = rw_gcs.blob(f"{attempt}data.{ext}")
        composite.content_type = content_type
        composite.compose(sources[:_COMPOSE_MAX_SOURCES])
        i = _COMPOSE_MAX_SOURCES
        while i < len(sources):
            # `composite` itself counts as one source → add up to 31 more.
            batch = sources[i : i + _COMPOSE_MAX_SOURCES - 1]
            composite.compose([composite, *batch])
            i += _COMPOSE_MAX_SOURCES - 1
        return composite, [*shards, *extras]

    t0 = time.perf_counter()
    composite, parts = await asyncio.to_thread(compose)
    log.debug(
        "BigQuery compose: %s <- %d shard(s) in %.0fms",
        attempt,
        len(shards),
        (time.perf_counter() - t0) * 1000,
    )

    # The composite is standalone → its source shards are garbage.
    # Deleted off the response path. (Old revisions: `_delete_old_cache`.)
    _cleanup_in_background(
        backend,
        lambda: _delete_blobs(parts, "compose sources"),
        what=f"source shards {attempt}",
    )
    return [composite]


def _delete_old_cache(
    rw_gcs: Any,
    *,
    sweep_prefix: str,
    keep_prefix: str,
    min_age: timedelta | None = None,
) -> int:
    """Delete older attempts and revisions under `sweep_prefix`; this
    request's own attempt (`keep_prefix`) is never touched. `min_age`
    spares anything younger than the signed-URL lifetime — covering both
    a sibling attempt still exporting and one whose URLs are live."""
    cutoff = datetime.now(timezone.utc) - min_age if min_age else None

    def is_stale(blob: Any) -> bool:
        if blob.name.startswith(keep_prefix):
            return False  # this request's own attempt
        created = getattr(blob, "time_created", None)
        if cutoff is not None and created is not None and created > cutoff:
            return False  # URLs may still be live
        return True

    stale = [b for b in rw_gcs.list_blobs(prefix=sweep_prefix) if is_stale(b)]
    return _delete_blobs(stale, "old cache")


def _cleanup_in_background(
    backend: Any,
    fn: Callable[[], None],
    *,
    what: str,
) -> None:
    """Fire-and-forget GCS cleanup on a worker thread — the response
    never waits on deletes. The task is held on the backend (a cached
    singleton) because the loop keeps only weak refs; tests await it."""

    async def run() -> None:
        try:
            await asyncio.to_thread(fn)
        except Exception as e:  # noqa: BLE001
            log.warning("background cleanup failed (%s): %s", what, e)

    task = asyncio.get_running_loop().create_task(run())
    backend._cleanup_tasks.add(task)
    task.add_done_callback(backend._cleanup_tasks.discard)


def _delete_blobs(blobs: list[Any], why: str) -> int:
    """Delete blobs, logging (never raising) individual failures — every
    delete here is best-effort cleanup. Sync: run via `to_thread`."""
    deleted = 0
    for blob in blobs:
        try:
            blob.delete()
            deleted += 1
        except Exception as e:  # noqa: BLE001
            log.warning(
                "%s: failed to delete %s: %s",
                why,
                getattr(blob, "name", "?"),
                e,
            )
    return deleted


async def _signed_urls(
    backend: Any,
    blobs: list[Any],
    *,
    filename_base: str,
    ext: str,
) -> list[str]:
    """V4-sign each blob with an attachment filename (`<base>.<ext>`,
    or `<base>_NN.<ext>` when there are several)."""
    expiry = _url_expiry(backend)

    def sign_all() -> list[str]:
        out: list[str] = []
        for i, blob in enumerate(blobs):
            filename = (
                f"{filename_base}.{ext}"
                if len(blobs) == 1
                else f"{filename_base}_{i + 1:02d}.{ext}"
            )
            out.append(
                blob.generate_signed_url(
                    version="v4",
                    expiration=expiry,
                    method="GET",
                    response_disposition=f'attachment; filename="{filename}"',
                )
            )
        return out

    return await asyncio.to_thread(sign_all)


# ============================================================================
# 4. SQL / BYTES BUILDERS (pure)
# ============================================================================


def _export_data_sql(
    uri: str,
    fmt: str,
    select_list: str,
    source: str,
    suffix: str = "",
) -> str:
    """`EXPORT DATA` statement text. csv exports header-less (the compose
    step injects one `_csv_header_bytes` member); gzip keeps per-shard
    headers (streamed with dedup)."""
    if fmt == "csv":
        extra_opts = ", header=false"
    elif fmt == "gzip":
        extra_opts = ", header=false, compression='GZIP'"
    else:
        extra_opts = ""
    return (
        f"EXPORT DATA OPTIONS("
        f"uri='{uri}', format='{_FMT[fmt]['bq']}', overwrite=true"
        f"{extra_opts}"
        ") AS "
        f"SELECT {select_list} FROM {source}{suffix}"
    )


def _export_select_list(schema: Any, fmt: str) -> str:
    """SELECT column list: parquet casts JSON columns to strings (BQ
    can't export native JSON to parquet); csv/ndjson use the same
    `format_select_column` casts as `datastore_search`."""
    if fmt == "parquet":
        fields = list(schema)
        if not any((f.field_type or "").upper() == "JSON" for f in fields):
            return "*"
        return ", ".join(
            (
                f"TO_JSON_STRING(`{f.name}`) AS `{f.name}`"
                if (f.field_type or "").upper() == "JSON"
                else f"`{f.name}`"
            )
            for f in fields
        )
    return ", ".join(format_select_column(f.name, f.field_type) for f in schema)


def _csv_header_bytes(schema: Any, fmt: str) -> bytes:
    """The one CSV header row composed in front of the header-less
    shards. gzip-compressed for `gzip` so it concatenates with the
    gzip shards as another member (column names never need quoting)."""
    row = (",".join(f.name for f in schema) + "\n").encode()
    return gzip.compress(row) if fmt == "gzip" else row


def _outer_order_by(sql: str, schema: Any) -> str:
    """Outer `ORDER BY` for a wrapped SQL export (BigQuery ignores a
    subquery's ORDER BY without LIMIT). Hoists the user's ORDER BY when
    its keys are output columns; else `ORDER BY _id` when `_id` is in
    the output; else "" (unordered)."""
    import sqlglot
    from sqlglot import expressions as exp

    out_cols = {f.name for f in schema}
    try:
        tree = sqlglot.parse_one(sql, dialect="bigquery")
    except Exception:
        return ""

    order = tree.args.get("order")
    if order is None:
        return " ORDER BY `_id`" if "_id" in out_cols else ""

    keys: list[str] = []
    for ordered in order.expressions:
        col = ordered.this
        if not isinstance(col, exp.Column) or col.name not in out_cols:
            return ""
        hoisted = ordered.copy()
        hoisted.set("this", exp.column(col.name, quoted=True))
        keys.append(hoisted.sql(dialect="bigquery"))
    return " ORDER BY " + ", ".join(keys)


def _is_export_too_large(exc: BaseException) -> bool:
    """Does this BigQuery error look like ">1 GB single-file rejection"?"""
    msg = str(exc).lower()
    return "single uri" in msg or "wildcard" in msg


# ============================================================================
# 5. BACKEND ACCESSORS
# ============================================================================


def _get_export_bucket(backend: Any) -> str:
    """Validated GCS export bucket name; `ServerError` when unset."""
    bucket = (getattr(backend.config, "BIGQUERY_EXPORT_BUCKET", "") or "").strip()
    if not bucket:
        raise ServerError(
            "BIGQUERY_EXPORT_BUCKET is not configured — "
            "/datastore/dump cannot run without an export bucket."
        )
    return bucket


def _url_expiry(backend: Any) -> timedelta:
    """Signed-URL lifetime (also the GC age gate for uuid revs)."""
    return timedelta(
        hours=getattr(backend.config, "BIGQUERY_EXPORT_URL_EXPIRY_HOURS", 1),
    )


async def _get_table(backend: Any, resource_id: str) -> Any:
    """Fetch table metadata; NotFound → 404, anything else → 500."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    table_ref = bigquery.TableReference.from_string(
        f"{backend.config.BIGQUERY_PROJECT}"
        f".{backend.config.BIGQUERY_DATASET}.{resource_id}"
    )
    try:
        return await asyncio.to_thread(backend.client.get_table, table_ref)
    except NotFound as e:
        raise NotFoundError(
            f"resource {resource_id!r} is not declared; nothing to dump"
        ) from e
    except Exception as e:
        raise ServerError(
            f"BigQuery get_table failed for resource {resource_id!r}: {e}"
        ) from e
