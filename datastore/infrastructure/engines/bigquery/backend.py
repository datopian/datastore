"""BigQuery backend.

Public surface is `BigQueryBackend` — the `DatastoreBackend` ABC.
File layout (top to bottom):

  1. Lifecycle (`__init__`, `initialize`).
  2. Low-level client wrappers (`_data_table_path`, `_data_table_ref`,
     `_run_query`, `_run_insert_rows`) — every BigQuery call is routed
     through these so transport / SQL errors surface as `ServerError`
     with `resource_id` + operation name baked in, never as raw
     `google.api_core` exceptions.
  3. Create helpers (`_create_data_table`, `_alter_data_table`,
     `_insert_records`, and the branch helpers `_apply_new_resource` /
     `_apply_existing_resource`).
  4. CKAN action methods (`create`, `upsert`, `search`, `search_sql`,
     `delete`, `info`, `get_columns`, `healthcheck`).
"""

from __future__ import annotations

import logging
from typing import Any

from datastore.core.config import Config
from datastore.core.exceptions import ServerError
from datastore.infrastructure.engines.base import (
    DatastoreBackend,
    InfoResult,
    MetadataStore,
    SearchResult,
    WriteResult,
)
from datastore.infrastructure.engines.bigquery.lib import (
    alter_clauses,
    column_defs,
    reject_unsupported_type_changes,
    schema_diff,
    serialise_json_columns,
)

log = logging.getLogger(__name__)


class BigQueryBackend(DatastoreBackend):
    # ----- lifecycle ------------------------------------------------------

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
        # `metadata` is set in `initialize()` once the client is built.
        # Stays `None` in placeholder mode (no BIGQUERY_PROJECT /
        # BIGQUERY_DATASET) so the rest of the app can boot — `create()`
        # skips the data + metadata writes in that mode rather than crash.
        self.metadata: MetadataStore | None = None

    def initialize(self) -> None:
        """Build the BigQuery client when configured; no-op otherwise.

        Lenient on missing config: if `BIGQUERY_PROJECT` is unset, log a
        warning and leave `client=None`. Lets the rest of the app boot
        without real GCP creds — `/ready` will return 503 (healthcheck
        returns False with no client) so the misconfiguration is loud
        enough in production without being fatal at import time.

        When the client is built, also constructs the `MetadataStore`
        and runs its `initialize()` so the `_table_metadata` table
        exists. Only the read-write engine creates DDL — the read-only
        engine constructs the store for `get()` but skips `initialize()`
        so it doesn't need CREATE privileges.
        """
        if self.config is None or not self.config.BIGQUERY_PROJECT.strip():
            log.warning(
                "BigQueryBackend: BIGQUERY_PROJECT unset (mode=%s); client "
                "not built — /ready will return 503 until configured.",
                self.mode,
            )
            return
        from datastore.infrastructure.engines.bigquery.client import build_client
        from datastore.infrastructure.engines.bigquery.metadata import (
            BigQueryMetadataStore,
        )

        self.client = build_client(self.config, self.mode)
        log.info(
            "BigQuery client initialised: project=%s mode=%s",
            self.config.BIGQUERY_PROJECT, self.mode,
        )

        dataset = self.config.BIGQUERY_DATASET.strip()
        if not dataset:
            log.warning(
                "BigQueryBackend: BIGQUERY_DATASET unset (mode=%s); "
                "metadata store disabled — `datastore_create` will not "
                "record per-resource schemas until configured.",
                self.mode,
            )
            return

        self.metadata = BigQueryMetadataStore(
            client=self.client,
            project=self.config.BIGQUERY_PROJECT,
            dataset=dataset,
        )
        if self.mode == "rw":
            self.metadata.initialize()

    # ----- table refs + low-level client wrappers ------------------------

    def _data_table_path(self, resource_id: str) -> str:
        """Plain `project.dataset.<resource_id>` (no backticks) for the
        Python client API surface (`insert_rows_json`, `get_table`)."""
        return (
            f"{self.config.BIGQUERY_PROJECT}"
            f".{self.config.BIGQUERY_DATASET}.{resource_id}"
        )

    def _data_table_ref(self, resource_id: str) -> str:
        """Backtick-quoted `project.dataset.<resource_id>` for SQL.

        Backticks make resource_ids with hyphens (CKAN UUIDs) parse
        without further escaping.
        """
        return f"`{self._data_table_path(resource_id)}`"

    def _run_query(
        self,
        sql: str,
        *,
        op: str,
        resource_id: str,
        job_config: Any = None,
    ) -> Any:
        """Submit `sql` to BigQuery and wait for the job result.

        Wraps every `client.query` call so any
        `google.api_core` / transport error becomes a CKAN-shaped
        `ServerError` carrying the action name (`op`) and target
        `resource_id`. Callers never have to know about Google's
        exception hierarchy.
        """
        try:
            return self.client.query(sql, job_config=job_config).result()
        except Exception as e:
            raise ServerError(
                f"BigQuery {op} failed for resource {resource_id!r}: {e}"
            ) from e

    def _run_insert_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
        *,
        op: str,
        resource_id: str,
    ) -> list[dict[str, Any]]:
        """Submit `rows` via the streaming insert API.

        Returns the per-row error list from `insert_rows_json` (empty
        on success). Transport / setup failures (table missing,
        permissions, network) raise `ServerError` here; row-level
        errors are returned to the caller so it can include row counts
        in its message.
        """
        try:
            return self.client.insert_rows_json(table, rows)
        except Exception as e:
            raise ServerError(
                f"BigQuery {op} failed for resource {resource_id!r}: {e}"
            ) from e

    # ----- create helpers (DDL + records + branch orchestration) --------
    def _create_data_table(self, resource_id: str, schema: dict) -> None:
        """`CREATE TABLE IF NOT EXISTS` with columns derived from the
        Frictionless schema. Idempotent — a second call on the same
        resource is a no-op DDL on the BigQuery side."""
        cols = column_defs(schema)
        if not cols:
            log.warning(
                "BigQueryBackend.create: schema for %r has no fields; "
                "skipping CREATE TABLE.",
                resource_id,
            )
            return
        sql = (
            f"CREATE TABLE IF NOT EXISTS {self._data_table_ref(resource_id)} "
            f"({', '.join(cols)})"
        )
        self._run_query(sql, op="CREATE TABLE", resource_id=resource_id)
        log.info("BigQuery table created: %s", resource_id)

    def _alter_data_table(
        self, resource_id: str, old_schema: dict, new_schema: dict
    ) -> None:
        """Apply the schema diff as DDL.

        Three diff classes:
          - **Added columns** → `ALTER TABLE ADD COLUMN IF NOT EXISTS`.
          - **Type changes** → `ALTER TABLE ALTER COLUMN SET DATA TYPE`
            when BigQuery accepts the transition (`types.can_widen`).
            Unsupported transitions raise `ConflictError` BEFORE any
            DDL runs so a single bad column can't half-apply the others.
          - **Removed columns** → logged and skipped; dropping a column
            would lose user data on a metadata edit.

        All ADD / ALTER clauses go in a single `ALTER TABLE` statement
        so BigQuery applies them atomically.
        """
        added, type_changes, removed = schema_diff(old_schema, new_schema)
        reject_unsupported_type_changes(type_changes)

        if removed:
            log.info(
                "BigQueryBackend.alter: columns %s dropped from schema "
                "for %r — keeping BigQuery columns to preserve rows.",
                removed, resource_id,
            )

        clauses = alter_clauses(added, type_changes, new_schema)
        if not clauses:
            return
        sql = (
            f"ALTER TABLE {self._data_table_ref(resource_id)} "
            f"{', '.join(clauses)}"
        )
        self._run_query(sql, op="ALTER TABLE", resource_id=resource_id)
        log.info(
            "BigQuery table altered: %s (added=%s, type_changes=%s)",
            resource_id, added, type_changes,
        )

    def _insert_records(
        self, resource_id: str, schema: dict, records: list
    ) -> None:
        """Stream-insert rows into the resource's data table.

        Uses `Client.insert_rows_json` — the standard low-latency path
        for row-level writes. Empty `records` is a no-op.

        BigQuery's `JSON` column type expects values on the wire as
        **JSON strings**, not native dicts / lists, so Frictionless
        `object` / `array` / `geojson` field values are serialised
        up-front via `_serialise_json_columns`.

        Any per-row errors BigQuery reports raise `ServerError`; the
        underlying client raising (transport, schema mismatch, etc.)
        also surfaces as `ServerError` via `_run_insert_rows`.
        """
        if not records:
            return
        table_ref = self._data_table_path(resource_id)
        prepared = serialise_json_columns(schema, records)
        errors = self._run_insert_rows(
            table_ref, prepared, op="INSERT", resource_id=resource_id
        )
        if errors:
            raise ServerError(
                f"BigQuery refused {len(errors)} of {len(records)} row(s) "
                f"on insert into {resource_id!r}: {errors}"
            )
        log.info(
            "BigQuery rows inserted: %s (%d row(s))",
            resource_id, len(records),
        )

    def _apply_new_resource(
        self, resource_id: str, schema: dict, records: list
    ) -> None:
        """First-time declaration: create the table, seed it, record it.

        `metadata.insert` is the final step so any failure earlier
        leaves the metadata store untouched and the resource appears
        un-declared on retry.
        """
        assert self.metadata is not None
        self._create_data_table(resource_id, schema)
        self._insert_records(resource_id, schema, records)
        self.metadata.insert(resource_id, schema)

    def _apply_existing_resource(
        self,
        resource_id: str,
        old_schema: dict,
        new_schema: dict,
        records: list,
    ) -> None:
        """Re-declaration on an existing resource: migrate the table,
        append rows, then update the metadata row.

        If alter OR the record insert raises, `metadata.update` is
        skipped and the metadata stays at the old schema version.
        """
        assert self.metadata is not None
        self._alter_data_table(resource_id, old_schema, new_schema)
        self._insert_records(resource_id, new_schema, records)
        self.metadata.update(resource_id, new_schema)

    # ----- CKAN action methods -------------------------------------------
    def create(
        self,
        resource_id: str,
        schema: dict,
        records: list | None,
        include_total: bool,
    ) -> WriteResult:
        """Declare a resource: DDL → records insert → metadata write.

        The order is load-bearing — see `_apply_new_resource` /
        `_apply_existing_resource` for the per-branch sequence. Any
        failure short-circuits before the metadata write so the
        metadata row never describes a state the actual table doesn't
        match.

        Placeholder mode (no project/dataset) is a no-op echo so the
        unit suite can exercise the call path without GCP creds.
        """
        if self.metadata is not None:
            existing = self.metadata.get(resource_id)
            rows = records or []
            if existing is None:
                self._apply_new_resource(resource_id, schema, rows)
            else:
                self._apply_existing_resource(
                    resource_id, existing, schema, rows
                )

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

        Placeholder: echoes inputs so the call path is exercised
        end-to-end. Real impl:
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
        """Query records. Returns SearchResult with a lazy row iterator.

        Placeholder: returns an empty result set so the call path is
        exercised end-to-end. Real impl builds a parameterised SELECT
        honouring `filters` / `q` / `distinct` / `sort`, optionally
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
        """Execute raw SQL SELECT. Returns SearchResult with a lazy row
        iterator.

        Placeholder: returns an empty result set. Real impl will call
        `client.query(sql, job_config=…)` and yield tuples from
        `query_job.result()` page-by-page, setting
        `records_truncated=True` if the iterator hit `limit`.
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
        Frictionless type vocabulary.
        """
        return InfoResult(
            fields=[],
            schema={"fields": []},
            meta={"resource_id": resource_id, "total": 0},
        )

    def get_columns(self, resource_id: str) -> list[str]:
        """Return column names for a table.

        Placeholder — replaced when real `search` lands. Empty list
        keeps callers from crashing on the dead code path.
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
            log.warning(
                "BigQuery healthcheck failed (mode=%s): %s", self.mode, e
            )
            return False

