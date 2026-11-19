"""Writes whose records are too big for one BigQuery request.

BigQuery caps a query request at about 10 MB, and records ride a single
`@rows` parameter. A larger write is cut into chunks and run inside one
transaction: every chunk lands, or none does.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from datastore.core.exceptions import ServerError, ValidationError

log = logging.getLogger(__name__)

_DEFAULT_CHUNK_MB = 9


@dataclass(frozen=True)
class ChunkedWriteResult:
    """Stands in for the `QueryJob` a single-statement write returns."""

    num_dml_affected_rows: int


def chunk_bytes(config: Any) -> int:
    """`BIGQUERY_WRITE_CHUNK_MB` in bytes (default when no config is attached)."""
    mb = getattr(config, "BIGQUERY_WRITE_CHUNK_MB", _DEFAULT_CHUNK_MB)
    if not isinstance(mb, int | float):
        mb = _DEFAULT_CHUNK_MB
    return int(mb * 1024 * 1024)


def is_oversized(config: Any, records: list[Any]) -> bool:
    """True when `records` do not fit in one statement."""
    import orjson

    return len(orjson.dumps(records)) > chunk_bytes(config)


def chunk_rows(records: list[Any], max_bytes: int) -> list[str]:
    """Serialise `records` into JSON-array strings of at most `max_bytes`.

    Each string is one `@rows` parameter. The payload is cut on record
    boundaries, in record order; one that fits comes back as a single
    chunk. A single record that cannot fit in a chunk on its own raises
    `ValueError`.
    """
    import orjson

    payload = orjson.dumps(records)
    if len(payload) <= max_bytes:
        return [payload.decode("utf-8")]

    chunks: list[str] = []
    current: list[bytes] = []
    size = 2  # the enclosing brackets
    for index, record in enumerate(records, start=1):
        item = orjson.dumps(record)
        if len(item) + 2 > max_bytes:
            raise ValueError(
                f"Record {index} is larger than the {max_bytes / (1024 * 1024):g} MB "
                "that can be written in one statement"
            )
        if current and size + len(item) + 1 > max_bytes:
            chunks.append((b"[" + b",".join(current) + b"]").decode("utf-8"))
            current, size = [], 2
        current.append(item)
        size += len(item) + 1
    chunks.append((b"[" + b",".join(current) + b"]").decode("utf-8"))
    return chunks


def write_if_oversized(
    backend: Any, resource_id: str, sql: str, *, op: str, records: list[Any]
) -> ChunkedWriteResult | None:
    """Write `records` in chunks inside one transaction, if they need it.

    Returns `None` when the records fit in one statement — the caller
    then runs its normal single-job write. Raises `ValidationError` for
    a record too big to send at all, `ServerError` for a BigQuery
    failure (after rolling the transaction back).
    """
    try:
        chunks = chunk_rows(records, chunk_bytes(backend.config))
    except ValueError as e:
        raise ValidationError(str(e)) from e
    if len(chunks) == 1:
        return None

    affected = _run_in_transaction(backend, resource_id, sql, op=op, chunks=chunks)
    log.info(
        "BigQuery %s: %s (%d row(s) in %d chunk(s), one transaction)",
        op,
        resource_id,
        len(records),
        len(chunks),
    )
    return ChunkedWriteResult(num_dml_affected_rows=affected)


def _run_in_transaction(
    backend: Any, resource_id: str, sql: str, *, op: str, chunks: list[str]
) -> int:
    """Run `sql` once per chunk inside one transaction; all or nothing.

    Nothing is visible to readers until the COMMIT, and any failure
    rolls every chunk back. Later chunks see the earlier ones, so `_id`
    keeps counting, and `CURRENT_TIMESTAMP()` is the transaction's start
    for all of them, so `_updated_at` is one value per request.

    The session is created in the dataset's location: `BEGIN
    TRANSACTION` names no table, so without one it lands in `US` and
    cannot see an EU dataset.

    The ROLLBACK is explicit on purpose: closing a session whose
    transaction is still open also discards the changes, but leaves the
    table refusing other writes for about five minutes.
    """
    from google.cloud import bigquery

    client = backend.client
    config = backend.config
    location = client.get_dataset(f"{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}").location

    def run(statement: str, job_config: Any) -> Any:
        try:
            job = client.query(statement, job_config=job_config, location=location)
            job.result()
            return job
        except Exception as e:
            raise ServerError(
                "internal error",
                detail=f"BigQuery {op} failed for resource {resource_id!r}: {e}",
            ) from e

    def in_session(session_id: str, payload: str | None = None) -> Any:
        job_config = bigquery.QueryJobConfig(
            connection_properties=[bigquery.ConnectionProperty("session_id", session_id)]
        )
        if payload is not None:
            job_config.query_parameters = [bigquery.ScalarQueryParameter("rows", "STRING", payload)]
        return job_config

    total_mb = sum(len(c) for c in chunks) / (1024 * 1024)
    log.info(
        "BigQuery %s: %s starting, %d chunk(s), %.1f MB", op, resource_id, len(chunks), total_mb
    )
    begin = run("BEGIN TRANSACTION", bigquery.QueryJobConfig(create_session=True))
    session_id = begin.session_info.session_id
    try:
        affected = 0
        try:
            for number, payload in enumerate(chunks, start=1):
                started = time.perf_counter()
                job = run(sql, in_session(session_id, payload))
                rows = job.num_dml_affected_rows or 0
                affected += rows
                log.info(
                    "BigQuery %s: %s chunk %d/%d (%.1f MB, %d row(s)) in %.1fs",
                    op,
                    resource_id,
                    number,
                    len(chunks),
                    len(payload) / (1024 * 1024),
                    rows,
                    time.perf_counter() - started,
                )
            started = time.perf_counter()
            run("COMMIT TRANSACTION", in_session(session_id))
            log.info(
                "BigQuery %s: %s commit in %.1fs", op, resource_id, time.perf_counter() - started
            )
        except Exception:
            try:
                run("ROLLBACK TRANSACTION", in_session(session_id))
            except ServerError:
                log.warning(
                    "BigQuery ROLLBACK failed for %s; closing the session discards the "
                    "transaction, but writes to the table may wait a few minutes.",
                    resource_id,
                    exc_info=True,
                )
            raise
        return affected
    finally:
        try:
            run(f"CALL BQ.ABORT_SESSION('{session_id}')", bigquery.QueryJobConfig())
        except ServerError:
            log.warning("BigQuery session close failed for %s", resource_id, exc_info=True)
