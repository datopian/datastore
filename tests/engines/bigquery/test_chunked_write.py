"""Unit tests for oversized writes — chunked inside one BigQuery transaction.

A write whose serialised records fit in one chunk is a single job, as
before. A larger one opens a session, runs the same statement once per
chunk inside a transaction, and commits — or rolls back on any failure,
so the table never keeps part of a request.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from datastore.core.exceptions import NotFoundError, ValidationError
from datastore.infrastructure.engines.bigquery.backend import BigQueryBackend
from datastore.infrastructure.engines.bigquery.chunked_write import chunk_rows

SCHEMA: dict[str, Any] = {
    "fields": [
        {"name": "k", "type": "integer"},
        {"name": "v", "type": "string"},
    ],
    "primaryKey": ["k"],
}

MB = 1024 * 1024


def _records(total_bytes: int) -> list[dict[str, Any]]:
    """Records whose JSON adds up to roughly `total_bytes`."""
    pad = "x" * 1000
    return [{"k": i, "v": pad} for i in range(1, total_bytes // 1020 + 1)]


# --- chunk_rows -------------------------------------------------------------


def test_chunk_rows_keeps_a_small_payload_as_one_chunk() -> None:
    records = [{"k": 1, "v": "a"}, {"k": 2, "v": "b"}]

    chunks = chunk_rows(records, max_bytes=MB)

    assert len(chunks) == 1
    assert json.loads(chunks[0]) == records


def test_chunk_rows_splits_a_large_payload_in_record_order() -> None:
    records = _records(int(2.5 * MB))

    chunks = chunk_rows(records, max_bytes=MB)

    assert len(chunks) == 3
    assert all(len(c.encode()) <= MB for c in chunks)
    rejoined = [r for c in chunks for r in json.loads(c)]
    assert rejoined == records


def test_chunk_rows_rejects_a_single_record_larger_than_a_chunk() -> None:
    records = [{"k": 1, "v": "a"}, {"k": 2, "v": "x" * (2 * MB)}]

    with pytest.raises(ValueError, match="Record 2 is larger than"):
        chunk_rows(records, max_bytes=MB)


# --- backend ----------------------------------------------------------------


class _Recorder:
    """Stands in for `client.query`: records each call, returns a job."""

    def __init__(self, *, fail_on: str | None = None, affected: int = 0) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail_on = fail_on
        self.affected = affected

    def __call__(self, sql: str, job_config: Any = None, location: Any = None) -> MagicMock:
        params = {p.name: p.value for p in (job_config.query_parameters if job_config else [])}
        props = {p.key: p.value for p in (job_config.connection_properties if job_config else [])}
        self.calls.append(
            {
                "sql": sql,
                "rows": json.loads(params["rows"]) if "rows" in params else None,
                "session": props.get("session_id"),
                "create_session": bool(job_config and job_config.create_session),
                "location": location,
            }
        )
        if self.fail_on and sql.startswith(self.fail_on) and "rows" in params:
            raise RuntimeError("400 Bad int64 value: abc; reason: invalidQuery")
        job = MagicMock()
        job.session_info.session_id = "sess-1"
        job.num_dml_affected_rows = self.affected if "rows" in params else None
        return job

    @property
    def sqls(self) -> list[str]:
        return [c["sql"] for c in self.calls]


def _backend(recorder: _Recorder, schema: dict[str, Any] | None = SCHEMA) -> BigQueryBackend:
    b = BigQueryBackend(mode="rw")
    b.client = MagicMock()
    b.client.query.side_effect = recorder
    b.client.get_dataset.return_value.location = "europe-west1"
    b.config = MagicMock()
    b.config.BIGQUERY_PROJECT = "proj-1"
    b.config.BIGQUERY_DATASET = "ds-1"
    b.config.INCLUDE_UPDATED_AT = True
    b.config.BIGQUERY_WRITE_CHUNK_MB = 1
    b._read_schema = lambda rid: schema  # type: ignore[method-assign]
    return b


def test_small_upsert_is_one_job_without_a_session() -> None:
    rec = _Recorder()
    records = [{"k": 1, "v": "a"}]

    _backend(rec).upsert("r1", records, "upsert", False)

    assert len(rec.calls) == 1
    assert rec.calls[0]["sql"].startswith("MERGE ")
    assert rec.calls[0]["rows"] == records
    assert rec.calls[0]["session"] is None
    assert rec.calls[0]["create_session"] is False


def test_large_upsert_runs_every_chunk_in_one_transaction() -> None:
    rec = _Recorder()
    records = _records(int(2.5 * MB))

    _backend(rec).upsert("r1", records, "upsert", False)

    assert rec.sqls[0] == "BEGIN TRANSACTION"
    assert rec.calls[0]["create_session"] is True
    merges = [c for c in rec.calls if c["sql"].startswith("MERGE ")]
    assert len(merges) == 3
    assert [r for c in merges for r in c["rows"]] == records
    assert rec.sqls[4] == "COMMIT TRANSACTION"
    assert "BQ.ABORT_SESSION" in rec.sqls[5]
    assert len(rec.calls) == 6
    # Everything after the BEGIN rides the session it created.
    assert all(c["session"] == "sess-1" for c in rec.calls[1:5])


def test_large_write_session_is_created_in_the_dataset_location() -> None:
    rec = _Recorder()

    _backend(rec).upsert("r1", _records(int(1.5 * MB)), "upsert", False)

    assert rec.calls[0]["location"] == "europe-west1"


def test_failed_chunk_rolls_back_and_never_commits() -> None:
    rec = _Recorder(fail_on="MERGE ")

    with pytest.raises(ValidationError, match="'abc' is not a valid integer"):
        _backend(rec).upsert("r1", _records(int(2.5 * MB)), "upsert", False)

    assert "COMMIT TRANSACTION" not in rec.sqls
    assert "ROLLBACK TRANSACTION" in rec.sqls
    assert rec.sqls.index("ROLLBACK TRANSACTION") < len(rec.sqls) - 1
    assert "BQ.ABORT_SESSION" in rec.sqls[-1]
    # The first chunk failed, so the other two were never sent.
    assert len([s for s in rec.sqls if s.startswith("MERGE ")]) == 1


def test_large_update_counts_matched_rows_across_chunks() -> None:
    records = _records(int(2.5 * MB))
    # 3 chunks x 10 matched rows is fewer than the records sent.
    rec = _Recorder(affected=10)

    with pytest.raises(NotFoundError, match=f"{len(records) - 30} of {len(records)} row"):
        _backend(rec).upsert("r1", records, "update", False)

    # Same as a single-statement update: matched rows are kept.
    assert "COMMIT TRANSACTION" in rec.sqls


def test_large_update_with_every_row_matched_succeeds() -> None:
    records = _records(int(1.5 * MB))
    rec = _Recorder()
    backend = _backend(rec)
    sizes: list[int] = []

    def query(sql: str, job_config: Any = None, location: Any = None) -> MagicMock:
        job = rec(sql, job_config, location)
        rows = rec.calls[-1]["rows"]
        if rows is not None:
            job.num_dml_affected_rows = len(rows)
            sizes.append(len(rows))
        return job

    backend.client.query.side_effect = query

    backend.upsert("r1", records, "update", False)

    assert sum(sizes) == len(records)
    assert len(sizes) == 2


def test_large_create_makes_the_table_before_the_transaction() -> None:
    rec = _Recorder()
    records = _records(int(1.5 * MB))

    _backend(rec, schema=None).create("r1", SCHEMA, records, False)

    assert rec.sqls[0].startswith("CREATE TABLE ")
    assert rec.calls[0]["session"] is None
    assert rec.sqls[1] == "BEGIN TRANSACTION"
    inserts = rec.calls[2:-2]
    assert len(inserts) == 2
    assert all("CREATE TABLE" not in c["sql"] for c in inserts)
    assert [r for c in inserts for r in c["rows"]] == records
    assert rec.sqls[-2] == "COMMIT TRANSACTION"


def test_record_larger_than_a_chunk_is_rejected_before_any_query() -> None:
    rec = _Recorder()
    records = [{"k": 1, "v": "x" * (2 * MB)}]

    with pytest.raises(ValidationError, match="Record 1 is larger than"):
        _backend(rec).upsert("r1", records, "upsert", False)

    assert rec.calls == []
