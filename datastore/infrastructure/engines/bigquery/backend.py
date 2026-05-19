from __future__ import annotations

import logging
from typing import Any

from datastore.core.config import Config
from datastore.infrastructure.engines.base import (
    DatastoreBackend,
    InfoResult,
    SearchResult,
    WriteResult,
)

log = logging.getLogger(__name__)


class BigQueryBackend(DatastoreBackend):
    def __init__(
        self,
        *,
        context: Any = None,
        config: Config | None = None,
        mode: str = "rw",
    ) -> None:
        self.mode = mode
        self.context = context
        self.config = config
        self.client: Any = None

    def initialize(self) -> None:
        """Build the BigQuery client when configured; no-op otherwise.

        Lenient on missing config: if `BIGQUERY_PROJECT` is unset, log a
        warning and leave `client=None`. Lets the rest of the app boot
        without real GCP creds — `/ready` will return 503 (healthcheck
        returns False with no client) so the misconfiguration is loud
        enough in production without being fatal at import time.
        """
        if self.config is None or not self.config.BIGQUERY_PROJECT.strip():
            log.warning(
                "BigQueryBackend: BIGQUERY_PROJECT unset (mode=%s); client "
                "not built — /ready will return 503 until configured.",
                self.mode,
            )
            return
        from datastore.infrastructure.engines.bigquery.client import build_client

        self.client = build_client(self.config, self.mode)
        log.info(
            "BigQuery client initialised: project=%s mode=%s",
            self.config.BIGQUERY_PROJECT,
            self.mode,
        )

    def create(
        self,
        resource_id: str,
        schema: dict,
        records: list | None,
        include_total: bool,
    ) -> WriteResult:
        """Create/alter table, optionally with records insert.

        Placeholder: echoes inputs. Real impl (Phase 8) issues
        `CREATE TABLE IF NOT EXISTS`, bulk-inserts records, and runs
        `COUNT(*)` when `include_total=True`.
        """
        return {
            "schema": schema,
            "records": records,
            "include_total": include_total,
            "total": len(records or []) if include_total else None,
        }

    def upsert(
        self,
        resource_id: str,
        records: list,
        method: str,
        include_total: bool,
    ) -> WriteResult:
        """Insert / update / upsert records.
        Placeholder: echoes inputs so the call path is exercised end-to-end:
          - "insert"  → `insert_rows_json` (or DML INSERT for large batches)
          - "update"  → DML `UPDATE ... WHERE <key_fields> IN @keys`
          - "upsert"  → `MERGE` with `UNNEST(@records)` as source
        and runs `COUNT(*)` when `include_total=True`.
        """
        return {
            "resource_id": resource_id,
            "records": records,
            "method": method,
            "include_total": include_total,
            "total": len(records),
        }

    def search(
        self,
        resource_id: str,
        filters: dict | None,
        q: str | dict | None,
        distinct: bool,
        plain: bool,
        language: str,
        limit: int,
        offset: int,
        fields: list | None,
        sort: str | None,
        include_total: bool,
    ) -> SearchResult:
        """Query records. Returns SearchResult with lazy row iterator.

        Placeholder: returns an empty result set so the call path is
        exercised end-to-end. Real impl (Phase 8) builds a parameterised
        SELECT honouring `filters` / `q` / `distinct` / `sort`, optionally
        runs `COUNT(*)` when `include_total=True`, and yields tuples
        page-by-page from `query_job.result()`.
        """
        schema: dict = {
            "fields": [{"name": c, "type": "any"} for c in fields]
            if fields else []
        }
        return SearchResult(
            schema=schema,
            records=iter([]),
            total=0 if include_total else None,
            records_truncated=False,
        )

    def search_sql(self, sql: str, limit: int) -> SearchResult:
        """Execute raw SQL SELECT. Returns SearchResult with lazy row iterator.

        Placeholder: returns an empty result set. Real impl will call
        `client.query(sql, job_config=…)` and yield tuples from
        `query_job.result()` page-by-page, setting `records_truncated=True`
        if the iterator hit `limit`.
        """
        return SearchResult(
            schema={"fields": []},
            records=iter([]),
            records_truncated=False,
        )

    def delete(self, resource_id: str, filters: dict | None) -> WriteResult:
        """Delete records (filtered) or drop table (no filters).

        Placeholder: returns an empty WriteResult. Real impl will issue
        `DELETE FROM <resource> WHERE …` (parameterised) for `filters`,
        or `DROP TABLE IF EXISTS <resource>` when filters is None.
        """
        return WriteResult()

    def info(self, resource_id: str) -> InfoResult:
        """Return table metadata: column schema + free-form `meta` dict.

        Placeholder: empty schema + minimal meta echoing the requested
        resource_id. Real impl will read BigQuery's `Table` metadata
        (schema, num_rows, num_bytes, modified, primary_key stored in
        the table description JSON) and translate into the canonical
        type set per §6.1.
        """
        return InfoResult(
            fields=[],
            schema={"fields": []},
            meta={"resource_id": resource_id, "total": 0},
        )

    def get_columns(self, resource_id: str) -> list[str]:
        """Return column names for a table.

        Placeholder — replaced when real `search` lands. Empty list keeps
        callers from crashing on the dead code path.
        """
        return []

    def healthcheck(self) -> bool:
        """Probe the BigQuery client with `SELECT 1`. Returns False on
        any failure so `/ready` can return 503 instead of crashing."""
        if self.client is None:
            return False
        try:
            self.client.query("SELECT 1").result()
            return True
        except Exception as e:
            log.warning("BigQuery healthcheck failed (mode=%s): %s", self.mode, e)
            return False
