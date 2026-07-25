"""BigQuery backend.

Public surface is `BigQueryBackend` — the `DatastoreBackend` ABC.
File layout (top to bottom):

  1. Lifecycle (`__init__`, `initialize`).
  2. Low-level client wrappers (`_data_table_ref`, `_run_query`) —
     every BigQuery call is routed through `_run_query` so transport /
     SQL errors surface as `ServerError` with `resource_id` + operation
     name baked in, never as raw `google.api_core` exceptions.
  3. Write helpers — `_build_dml` (builder SQL; ValueError → 400) and
     `_write_rows` (run a `@rows` write; BQ errors → 400). The DDL /
     DML helpers (`_create_table_sql`, `_alter_data_table`,
     `_insert_records` / `_merge_records` / `_update_records`) and the
     create branches (`_apply_new_resource` / `_apply_existing_resource`)
     build on those two.
  4. CKAN action methods (`create`, `upsert`, `search`, `search_sql`,
     `delete`, `info`, `get_columns`, `healthcheck`).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from datastore.core.config import Config
from datastore.core.exceptions import (
    NotFoundError,
    PayloadTooLargeError,
    ServerError,
    ValidationError,
)
from datastore.infrastructure.engines.base import (
    DatastoreBackend,
    InfoResult,
    SearchResult,
    WriteResult,
)
from datastore.infrastructure.engines.bigquery.lib import (
    PK_CONFLICT_SENTINEL,
    SYSTEM_COLUMN_NAMES,
    alter_clauses,
    column_defs,
    default_order_by,
    delete_sql,
    drop_columns_sql,
    format_select_column,
    insert_guarded_sql,
    insert_sql,
    merge_sql,
    normalize_pk,
    qualify_table_refs,
    reject_unsupported_type_changes,
    schema_diff,
    set_table_options_sql,
    strip_limit_offset,
    table_options_clause,
    table_to_schema,
    unfiltered_table_name,
    update_sql,
)

log = logging.getLogger(__name__)

_HEALTHCHECK_TTL_SECONDS = 15.0
_HEALTHCHECK_TIMEOUT_SECONDS = 5.0


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
        self._health: tuple[float, bool] | None = None

    def initialize(self) -> None:
        """Build the BigQuery client when project + dataset are configured.

        Missing config logs a warning and leaves `client=None` so the
        app still boots — `/ready` returns 503 until configured.
        """
        if self.config is None or not self.config.BIGQUERY_PROJECT.strip():
            log.warning(
                "BigQueryBackend: BIGQUERY_PROJECT unset (mode=%s); client "
                "not built — /ready will return 503 until configured.",
                self.mode,
            )
            return
        if not self.config.BIGQUERY_DATASET.strip():
            log.warning(
                "BigQueryBackend: BIGQUERY_DATASET unset (mode=%s); client "
                "not built — /ready will return 503 until configured.",
                self.mode,
            )
            return
        from datastore.infrastructure.engines.bigquery.client import build_client

        self.client = build_client(self.config, self.mode)
        log.info(
            "BigQuery client initialised: project=%s mode=%s",
            self.config.BIGQUERY_PROJECT, self.mode,
        )

    def _read_schema(self, resource_id: str) -> dict | None:
        """Return Frictionless schema for `resource_id`, or `None` when absent.

        Uses `tables.get` (REST, no query job, ~200ms). Wraps
        non-`NotFound` errors as `ServerError`. Caller must ensure
        `self.client` is set.
        """
        from google.api_core.exceptions import NotFound

        ref = (
            f"{self.config.BIGQUERY_PROJECT}"
            f".{self.config.BIGQUERY_DATASET}.{resource_id}"
        )
        try:
            table = self.client.get_table(ref)
        except NotFound:
            return None
        except Exception as e:
            raise ServerError(
                f"BigQuery tables.get failed for resource {resource_id!r}: {e}"
            ) from e
        return table_to_schema(table)

    # ----- table refs + low-level client wrappers ------------------------

    @property
    def _include_updated_at(self) -> bool:
        """Read the `_updated_at` system-column toggle from config.

        Defaults to `True` when no config is attached (test scaffolds
        that build the backend directly without `initialize()`).
        """
        return getattr(self.config, "INCLUDE_UPDATED_AT", True)

    def _data_table_ref(self, resource_id: str) -> str:
        """Backtick-quoted `project.dataset.<resource_id>` for SQL.

        Backticks make resource_ids with hyphens (CKAN UUIDs) parse
        without further escaping.
        """
        return (
            f"`{self.config.BIGQUERY_PROJECT}"
            f".{self.config.BIGQUERY_DATASET}.{resource_id}`"
        )

    def _read_job_config(self, params: list | None = None) -> Any:
        """QueryJobConfig for SELECT paths — honours `BIGQUERY_USE_QUERY_CACHE`.

        BQ's 24h results cache makes identical SELECTs free + fast on
        hit. Writes don't go through this; BQ's cache only applies to
        SELECT anyway.
        """
        from google.cloud import bigquery
        return bigquery.QueryJobConfig(
            query_parameters=params or [],
            use_query_cache=getattr(
                self.config, "BIGQUERY_USE_QUERY_CACHE", True,
            ),
        )

    def _run_query(
        self,
        sql: str,
        *,
        op: str,
        resource_id: str,
        job_config: Any = None,
    ) -> Any:
        """Submit `sql`, wait for completion, return the QueryJob.

        Wraps any `client.query` exception as `ServerError` carrying
        `op` + `resource_id`. Returning the job lets callers grab
        rows (`job.result()`) or DML counts (`num_dml_affected_rows`).
        """
        try:
            job = self.client.query(sql, job_config=job_config)
            job.result()
            return job
        except Exception as e:
            raise ServerError(
                f"BigQuery {op} failed for resource {resource_id!r}: {e}"
            ) from e

    # ----- create helpers (DDL + records + branch orchestration) --------
    def _create_table_sql(self, resource_id: str, schema: dict) -> str | None:
        """Render the `CREATE TABLE … OPTIONS(...)` DDL.
        """
        cols = column_defs(schema, include_updated_at=self._include_updated_at)
        if not cols:
            log.warning(
                "BigQueryBackend.create: schema for %r has no fields; "
                "skipping CREATE TABLE.",
                resource_id,
            )
            return None
        return (
            f"CREATE TABLE {self._data_table_ref(resource_id)} "
            f"({', '.join(cols)}){table_options_clause(schema)}"
        )

    def _refresh_table_options(self, resource_id: str, schema: dict) -> None:
        """Issue `ALTER TABLE … SET OPTIONS(...)` to rewrite the
        table-level metadata block.
        """
        sql = set_table_options_sql(
            self._data_table_ref(resource_id), schema,
        )
        self._run_query(
            sql, op="ALTER TABLE SET OPTIONS", resource_id=resource_id,
        )

    def _alter_data_table(
        self, resource_id: str, old_schema: dict, new_schema: dict
    ) -> None:
        """Apply the schema diff as DDL.

        Added columns → `ADD COLUMN IF NOT EXISTS`. Widened types →
        `ALTER COLUMN SET DATA TYPE` (unsupported widenings raise
        `ConflictError` before any DDL runs). Removed columns are
        logged and kept (dropping would lose user data).

        Always follows up with `SET OPTIONS` to refresh the
        table-level metadata, even when no column actions ran.
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
        if clauses:
            sql = (
                f"ALTER TABLE {self._data_table_ref(resource_id)} "
                f"{', '.join(clauses)}"
            )
            self._run_query(sql, op="ALTER TABLE", resource_id=resource_id)
            log.info(
                "BigQuery table altered: %s (added=%s, type_changes=%s)",
                resource_id, added, type_changes,
            )

        # Refresh the table-level metadata even when no column actions
        # ran — `primaryKey` or the per-column type hints may have
        # changed without a column add/alter (e.g. user re-declares
        # `primaryKey` on the same column set).
        self._refresh_table_options(resource_id, new_schema)

    def _rows_job_config(self, records: list) -> Any:
        """`QueryJobConfig` carrying `@rows` as a JSON-array string param."""
        import orjson
        from google.cloud import bigquery

        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "rows", "STRING", orjson.dumps(records).decode("utf-8"),
                ),
            ]
        )

    def _build_dml(self, builder: Any, resource_id: str, schema: dict) -> str:
        """Render a DML builder's SQL, surfacing its `ValueError` as 400."""
        try:
            return builder(
                self._data_table_ref(resource_id),
                schema,
                include_updated_at=self._include_updated_at,
            )
        except ValueError as e:
            raise ValidationError(str(e)) from e

    def _write_rows(
        self, resource_id: str, sql: str, *, op: str, action: str, records: list
    ) -> Any:
        """Run a `@rows`-parameterised write; map BQ write errors to 400s.

        `MAX(_id)` and `CURRENT_TIMESTAMP()` are inlined in the SQL, so
        each write is a single round-trip. Returns the job for callers
        that need `num_dml_affected_rows`.
        """
        try:
            job = self._run_query(
                sql, op=op, resource_id=resource_id,
                job_config=self._rows_job_config(records),
            )
        except ServerError as e:
            raise _translate_bigquery_error(e, resource_id, action) from e
        log.info("BigQuery %s: %s (%d row(s))", op, resource_id, len(records))
        return job

    def _insert_records(
        self, resource_id: str, schema: dict, records: list
    ) -> None:
        """DML `INSERT` for `records` (empty → no-op).

        When the resource declares a `primaryKey`, the INSERT is wrapped
        in a single guarded script (`insert_guarded_sql`) that rejects
        the whole batch with `ValidationError` — nothing written — if any
        row would duplicate a key (against an existing table row or
        another row in the same batch). BigQuery doesn't enforce keys, so
        a plain INSERT would silently duplicate them; the guard runs the
        conflict check and the INSERT in *one* job, so the `@rows` batch
        is serialised + uploaded only once. PK-less resources keep the
        plain INSERT.
        """
        if not records:
            return
        # PK declared → guarded script (conflict check + atomic INSERT in
        # one job). No PK → skip the check entirely; plain atomic INSERT.
        if normalize_pk(schema):
            builder = insert_guarded_sql
        else:
            builder = insert_sql
        sql = self._build_dml(builder, resource_id, schema)
        self._write_rows(
            resource_id, sql, op="INSERT", action="insert", records=records,
        )

    def _merge_records(
        self, resource_id: str, schema: dict, records: list
    ) -> None:
        """`MERGE` keyed on `schema.primaryKey` (empty → no-op)."""
        if not records:
            return
        sql = self._build_dml(merge_sql, resource_id, schema)
        self._write_rows(
            resource_id, sql, op="MERGE", action="upsert", records=records,
        )

    def _update_records(
        self, resource_id: str, schema: dict, records: list
    ) -> None:
        """DML `UPDATE` keyed on `schema.primaryKey` (empty → no-op).

        DML UPDATE silently no-ops on PK misses, so any unmatched row
        (affected < input count) is reported as `NotFoundError`.
        """
        if not records:
            return
        sql = self._build_dml(update_sql, resource_id, schema)
        job = self._write_rows(
            resource_id, sql, op="UPDATE", action="update", records=records,
        )
        affected = job.num_dml_affected_rows or 0
        if affected < len(records):
            missing = len(records) - affected
            raise NotFoundError(
                f"datastore_update: {missing} of {len(records)} row(s) "
                f"had no matching primary key in resource {resource_id!r}; "
                "use method='upsert' to insert missing rows"
            )

    def _apply_new_resource(
        self, resource_id: str, schema: dict, records: list
    ) -> None:
        """First-time create: CREATE TABLE (+ INSERT) as one BQ script.

        Empty `records` collapses to a standalone CREATE.
        """
        create_sql = self._create_table_sql(resource_id, schema)
        if create_sql is None:
            return

        if not records:
            self._run_query(
                create_sql, op="CREATE TABLE", resource_id=resource_id,
            )
            log.info("BigQuery table created: %s", resource_id)
            return

        # `;` joins the DDL + DML into one BigQuery script — one job
        # submission, shared `@rows`. The INSERT's `MAX(_id)` subquery
        # sees the just-created empty table, so `_id` starts at 1.
        # With a primaryKey, the guarded INSERT runs its conflict probe
        # against the just-created (empty) table — existing-row hits are
        # impossible, but intra-batch duplicate keys still RAISE before
        # any row lands.
        builder = insert_guarded_sql if normalize_pk(schema) else insert_sql
        script = (
            f"{create_sql};\n"
            f"{self._build_dml(builder, resource_id, schema)}"
        )
        self._write_rows(
            resource_id, script,
            op="CREATE TABLE + INSERT", action="insert", records=records,
        )

    def _apply_existing_resource(
        self,
        resource_id: str,
        old_schema: dict,
        new_schema: dict,
        records: list,
    ) -> None:
        """Re-declare: ALTER (diff + refresh OPTIONS) then INSERT.

        ALTER first so the reader sees the new schema consistently
        even if INSERT fails.
        """
        self._alter_data_table(resource_id, old_schema, new_schema)
        self._insert_records(resource_id, new_schema, records)

    # ----- CKAN action methods -------------------------------------------
    def create(
        self,
        resource_id: str,
        schema: dict,
        records: list | None,
        include_total: bool,
    ) -> WriteResult:
        """Declare a resource, optionally seeding it with rows.

        Always reads existing schema first so column adds / type
        widens apply on re-declares — even when `records` is empty.
        Dispatches to `_apply_new_resource` (table absent) or
        `_apply_existing_resource` (table present). Placeholder mode
        (no client) is an echo.
        """
        if self.client is not None:
            existing = self._read_schema(resource_id)
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
        """Insert / update / upsert records into an existing resource.

        `method="upsert"` → `MERGE` keyed on `schema.primaryKey`;
        `method="insert"` → DML INSERT; when a `primaryKey` is declared,
        a row that would duplicate a key (existing or in-batch) is
        rejected with `ValidationError`;
        `method="update"` → DML UPDATE (missing PK raises
        `NotFoundError`). Resource must already exist
        (`datastore_create` first). Placeholder mode is an echo.
        """
        if self.client is None:
            # Placeholder mode — echo (matches the create() pattern).
            return {
                "resource_id": resource_id,
                "records": records,
                "method": method,
                "include_total": include_total,
                "total": len(records or []),
            }

        schema = self._read_schema(resource_id)
        if schema is None:
            raise NotFoundError(
                f"resource {resource_id!r} not found."
            )

        rows = records or []
        if method == "insert":
            self._insert_records(resource_id, schema, rows)
        elif method == "upsert":
            self._merge_records(resource_id, schema, rows)
        elif method == "update":
            self._update_records(resource_id, schema, rows)
        else:
            raise ValidationError(
                f"unknown upsert method {method!r}; expected one of "
                "'upsert', 'insert', 'update'"
            )

        return {
            "resource_id": resource_id,
            "records": records,
            "method": method,
            "include_total": include_total,
            "total": len(rows) if include_total else None,
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
        """Parameterised SELECT against the data table, returning a tuple iterator.

        Fires the data query and the (optional) count query in
        parallel — wall time ≈ max(both). Unfiltered totals come from
        free `INFORMATION_SCHEMA` metadata instead of COUNT(*).
        `plain` / `language` are accepted for CKAN compatibility but
        ignored (BQ's `SEARCH()` tokenises uniformly). Placeholder
        mode (no client) returns an empty result.
        """
        from datastore.infrastructure.engines.bigquery.search import (
            build_count,
            build_search,
            needs_count_query,
        )

        if self.client is None:
            # Placeholder mode (no GCP creds) — echo the requested
            # field shape so the unit suite can exercise the streaming
            # writer + envelope plumbing without a real backend.
            stub_schema = {
                "fields": [
                    {"name": c, "type": "any"} for c in (fields or [])
                ],
            }
            return SearchResult(
                schema=stub_schema,
                records=iter([]),
                total=0 if include_total else None,
                records_truncated=False,
            )

        schema = self._read_schema(resource_id)
        if schema is None:
            raise NotFoundError(
                f"resource {resource_id!r} not found."
            )

        try:
            sql, params, projected = build_search(
                table_ref=self._data_table_ref(resource_id),
                schema=schema,
                include_updated_at=self._include_updated_at,
                fields=fields,
                filters=filters,
                q=q,
                distinct=distinct,
                sort=sort,
                limit=limit,
                offset=offset,
            )
        except ValueError as e:
            raise ValidationError(str(e)) from e

        # Read-path configs use the query-results cache (see
        # _read_job_config). Identical search params hit a 24h cache
        # entry — free + fast on the second call.
        job_config = self._read_job_config(params=params)

        # Fire both jobs before waiting on either: BigQuery's
        # `client.query()` is non-blocking, so the count and the page
        # query run in parallel — wall time ≈ max(both).
        count_cfg = None
        count_sql = ""
        if include_total and needs_count_query(
            filters=filters, q=q, distinct=distinct,
        ):
            count_sql, count_params = build_count(
                table_ref=self._data_table_ref(resource_id),
                schema=schema,
                include_updated_at=self._include_updated_at,
                fields=fields,
                filters=filters,
                q=q,
                distinct=distinct,
            )
            count_cfg = self._read_job_config(params=count_params)

        # Submit both jobs before waiting on either, so the COUNT and the
        # page query run in parallel. The submits sit inside the try too —
        # submit-time failures (auth, quota, bad config) map to ServerError
        # just like a result() failure, never a raw google exception.
        count_job = None
        try:
            if count_cfg is not None:
                count_job = self.client.query(count_sql, job_config=count_cfg)
            search_job = self.client.query(sql, job_config=job_config)
            row_iter = search_job.result()
        except Exception as e:
            raise ServerError(
                f"BigQuery search failed for resource {resource_id!r}: {e}"
            ) from e

        total: int | None = None
        if include_total:
            if count_job is None:
                # Unfiltered + non-distinct → metadata row_count (free).
                total = self._count_rows(resource_id)
            else:
                try:
                    rows = list(count_job.result())
                except Exception as e:
                    raise ServerError(
                        f"BigQuery search COUNT failed for resource "
                        f"{resource_id!r}: {e}"
                    ) from e
                total = int(rows[0]["n"]) if rows else 0

        return SearchResult(
            schema=projected,
            records=(tuple(row.values()) for row in row_iter),
            total=total,
            records_truncated=False,
        )

    def search_sql(self, sql: str, limit: int) -> SearchResult:
        """Execute a vetted SELECT/WITH, stream tuples, bounded by `limit`.

        Safety relies on upstream layers (schema rejects non-SELECT,
        endpoint authorises tables, service checks function allow-list).
        The load-bearing guard is `mode="ro"` — read-only IAM
        physically refuses any DML/DDL. Total comes from a free
        unfiltered `COUNT(*)` for plain SELECTs, else
        `COUNT(*) FROM (...)`; COUNT failures are non-fatal.
        """
        from itertools import islice

        from datastore.infrastructure.engines.bigquery.types import (
            frictionless_type_from_bigquery,
        )

        if self.client is None:
            return SearchResult(
                schema={"fields": []},
                records=iter([]),
                records_truncated=False,
            )

        if self.mode != "ro":
            raise ServerError(
                "datastore_search_sql must run on a read-only engine; "
                "got mode=" + repr(self.mode)
            )

        # User refers to tables by their CKAN resource_id; BigQuery
        # needs a fully-qualified `project.dataset.table` reference
        # with backticks. The qualifier walks the AST, prepends the
        # configured project + dataset to every non-CTE table ref,
        # and re-emits as BigQuery dialect.
        try:
            qualified_sql = qualify_table_refs(
                sql,
                project=self.config.BIGQUERY_PROJECT,
                dataset=self.config.BIGQUERY_DATASET,
            )
        except Exception as e:
            raise ServerError(
                f"failed to qualify table references in SQL: {e}"
            ) from e

        count_sql, count_params = self._search_sql_count_query(qualified_sql)

        data_sql = default_order_by(qualified_sql)

        # Submit COUNT first (non-blocking) so it runs in parallel with
        # the data query. A COUNT failure is non-fatal — log and degrade
        # `total` to None; a data-query failure is the user's primary
        # request, so it propagates as ServerError.
        count_job = None
        if count_sql:
            try:
                count_cfg = self._read_job_config(params=count_params)
                count_job = self.client.query(count_sql, job_config=count_cfg)
            except Exception as e:
                log.warning("search_sql COUNT submit failed: %s", e)

        try:
            data_job = self.client.query(
                data_sql, job_config=self._read_job_config(),
            )
            row_iter = data_job.result()
        except Exception as e:
            raise ServerError(f"BigQuery search_sql failed: {e}") from e

        total: int | None = None
        if count_job is not None:
            try:
                count_rows = list(count_job.result())
                total = int(count_rows[0]["n"]) if count_rows else 0
            except Exception as e:
                log.warning("search_sql COUNT failed: %s", e)

        schema_fields = [
            {
                "name": field.name,
                "type": frictionless_type_from_bigquery(field.field_type),
            }
            for field in (row_iter.schema or [])
        ]

        rows = (tuple(r.values()) for r in islice(row_iter, limit))
        return SearchResult(
            schema={"fields": schema_fields},
            records=rows,
            total=total,
            records_truncated=False,
        )

    def _search_sql_count_query(
        self, qualified_sql: str
    ) -> tuple[str | None, list]:
        """Pick the cheapest `total` query for a vetted SELECT.

        Plain `SELECT cols FROM t [LIMIT/OFFSET]` counts the source
        table directly — an unfiltered `COUNT(*)` is a BigQuery metadata
        read (0 bytes scanned), so it's free and always fresh. Anything
        that filters/joins/aggregates wraps the LIMIT-stripped query in
        `COUNT(*)`. `RowIterator.total_rows` can't be used — it counts
        the post-LIMIT page, so pagination would always read "last
        page". Returns `(None, [])` if no COUNT can be built (non-fatal).
        """
        try:
            table = unfiltered_table_name(qualified_sql)
            if table is not None:
                # `INFORMATION_SCHEMA.TABLE_STORAGE` is region-scoped
                # (not dataset-scoped), so a `project.dataset.…` ref
                # 404s; a bare unfiltered COUNT(*) is just as cheap.
                return (
                    f"SELECT COUNT(*) AS n FROM {self._data_table_ref(table)}",
                    [],
                )
            inner = strip_limit_offset(qualified_sql)
            return f"SELECT COUNT(*) AS n FROM ({inner})", []
        except Exception as e:
            log.warning(
                "search_sql: could not build COUNT query (%s); "
                "total will be omitted",
                e,
            )
            return None, []

    def delete(
        self,
        resource_id: str,
        filters: dict[str, Any] | None,
        fields: list[str] | None = None,
    ) -> WriteResult:
        """Drop the table (both None), delete rows by `filters`, or
        drop columns by `fields`. Schema layer enforces mutual
        exclusivity."""
        if self.client is None:
            return WriteResult()

        schema = self._read_schema(resource_id)
        if schema is None:
            raise NotFoundError(
                f"resource {resource_id!r} is not declared; nothing to delete"
            )

        if fields is not None:
            new_schema = self._drop_columns(resource_id, schema, fields)
            return WriteResult(schema=new_schema)

        if filters is None:
            # Metadata lives on the table itself, so DROP removes both.
            self._drop_data_table(resource_id)
            return WriteResult()

        self._delete_rows(resource_id, schema, filters)
        return WriteResult()

    def _drop_data_table(self, resource_id: str) -> None:
        """`DROP TABLE IF EXISTS` for the resource's data table."""
        sql = f"DROP TABLE IF EXISTS {self._data_table_ref(resource_id)}"
        self._run_query(sql, op="DROP TABLE", resource_id=resource_id)
        log.info("BigQuery table dropped: %s", resource_id)

    def _delete_rows(
        self,
        resource_id: str,
        schema: dict,
        filters: dict[str, Any],
    ) -> None:
        """Parameterised ``DELETE FROM … WHERE …`` from the filter map."""
        from google.cloud import bigquery
        try:
            sql, params = delete_sql(
                self._data_table_ref(resource_id), schema, filters,
            )
        except ValueError as e:
            raise ValidationError(str(e)) from e

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        try:
            self._run_query(
                sql, op="DELETE", resource_id=resource_id,
                job_config=job_config,
            )
        except ServerError as e:
            raise _translate_bigquery_error(e, resource_id, "delete") from e
        log.info(
            "BigQuery rows deleted: %s (filters=%s)",
            resource_id, sorted(filters.keys()) or "<all>",
        )

    def _drop_columns(
        self,
        resource_id: str,
        schema: dict[str, Any],
        fields: list[str],
    ) -> dict[str, Any]:
        """`ALTER TABLE DROP COLUMN …` + refresh table OPTIONS.

        Rejects system columns, unknown columns, and PK columns up front.
        Returns the resulting Frictionless schema (minus the dropped columns).
        """
        existing = {
            f["name"]
            for f in schema.get("fields", [])
            if f.get("name")
        }

        # System-column check first: `_id` / `_updated_at` aren't in
        # the stored schema, so the unknown-column check would shadow
        # them with a less specific error.
        reserved = [c for c in fields if c in SYSTEM_COLUMN_NAMES]
        if reserved:
            raise ValidationError(
                f"cannot drop engine-reserved system column(s): "
                f"{sorted(reserved)}"
            )
        unknown = [c for c in fields if c not in existing]
        if unknown:
            raise ValidationError(
                f"cannot drop unknown column(s): {sorted(unknown)}"
            )
        pk = set(normalize_pk(schema))
        pk_violations = [c for c in fields if c in pk]
        if pk_violations:
            raise ValidationError(
                f"cannot drop primary-key column(s): "
                f"{sorted(pk_violations)}; re-create the resource with "
                "a new primaryKey instead"
            )

        sql = drop_columns_sql(self._data_table_ref(resource_id), fields)
        self._run_query(sql, op="ALTER DROP COLUMN", resource_id=resource_id)

        drop_set = set(fields)
        new_schema: dict[str, Any] = {
            **schema,
            "fields": [
                f for f in schema.get("fields", [])
                if f.get("name") not in drop_set
            ],
        }
        self._refresh_table_options(resource_id, new_schema)
        log.info(
            "BigQuery columns dropped: %s (%s)", resource_id, sorted(fields),
        )
        return new_schema

    def info(self, resource_id: str) -> InfoResult:
        """Return the table schema + row stats for a resource.

        Schema via `_read_schema` (single `tables.get` call — no SQL
        job); total via `COUNT(*)`. Placeholder mode (no client)
        returns a stub.
        """
        if self.client is None:
            return InfoResult(
                schema={"fields": []},
                meta={"resource_id": resource_id, "total": 0},
            )

        schema = self._read_schema(resource_id)
        if schema is None:
            raise NotFoundError(
                f"resource {resource_id!r} not found."
            )

        total = self._count_rows(resource_id)

        return InfoResult(
            schema=schema,
            meta={
                "resource_id": resource_id,
                "total": total,
                "primary_key": normalize_pk(schema),
            },
        )

    def _count_rows(self, resource_id: str) -> int:
        """`COUNT(*)` on the data table; logs + returns 0 on failure.

        A missing data table while metadata exists is inconsistent
        state — returning 0 keeps `datastore_info` informative
        instead of 500-ing the whole call.
        """
        sql = (
            f"SELECT COUNT(*) AS n FROM "
            f"{self._data_table_ref(resource_id)}"
        )
        try:
            job = self._run_query(
                sql, op="COUNT", resource_id=resource_id,
                job_config=self._read_job_config(),
            )
            rows = list(job.result())
        except ServerError as e:
            log.warning(
                "COUNT(*) failed for resource %r; reporting total=0: %s",
                resource_id, e,
            )
            return 0
        if not rows:
            return 0
        return int(rows[0]["n"])

    async def dump(self, resource_id: str, fmt: str) -> list[str]:
        """Whole-table `EXPORT DATA` → signed URLs.

        Cache key = `dumps/<rid>/<fmt>/<rev>`, `rev` = `table.modified`
        (uuid fallback when the metadata is missing). Always cacheable:
        the export is a pure `SELECT * ... ORDER BY _id`, so its bytes
        depend only on the table. The cache → export → GC → sign
        workflow itself lives in `_cached_export`.
        """
        from uuid import uuid4

        if self.client is None:
            return []

        bucket = self._export_bucket()
        table = await self._get_table(resource_id)

        rev = (
            f"{int(table.modified.timestamp() * 1_000_000):x}"
            if table.modified is not None
            else uuid4().hex[:12]
        )
        prefix = f"dumps/{resource_id}/{fmt}/{rev}"
        uri = f"gs://{bucket}/{prefix}_*.{_FMT[fmt]['ext']}"
        source = (
            f"`{self.config.BIGQUERY_PROJECT}"
            f".{self.config.BIGQUERY_DATASET}.{resource_id}`"
        )

        async def build_export_sql() -> str:
            # Schema is already known from get_table — no dry run needed.
            return _export_statement(
                uri,
                fmt,
                _build_export_select(table.schema, fmt),
                source=source,
                suffix=" ORDER BY `_id`",
            )

        return await self._cached_export(
            bucket=bucket,
            prefix=prefix,
            fmt=fmt,
            cacheable=True,
            build_export_sql=build_export_sql,
            sweep_prefix=f"dumps/{resource_id}/{fmt}/",
            gc_min_age=None,
            filename_base=resource_id,
            what=f"resource {resource_id!r}",
        )

    async def dump_sql(
        self,
        sql: str,
        fmt: str,
        *,
        resource_ids: list[str],
        function_names: list[str],
    ) -> list[str]:
        """Export the result of a vetted SELECT via `EXPORT DATA`.

        Deltas vs `dump()` (whole-table export):

        - Cache key: `sql_dumps/<qhash>/<fmt>/<rev>` — `qhash` hashes the
          qualified SQL text; `rev` hashes every referenced table's
          `modified` timestamp, so a change to any table produces a new
          revision. The top-level `sql_dumps/` prefix can't collide with
          `dumps/<rid>/…` regardless of table names.
        - SQL calling non-deterministic functions (`now()`, …) is never
          cacheable — those runs skip the cache lookup and write under a
          fresh uuid rev so `overwrite=true` can't resurrect stale data.
        - The export SELECT wraps the user SQL as a subquery. Output
          column types come from a free dry run on the RO client, which
          doubles as an RO-credentialed preflight (clean 400 on SQL that
          doesn't compile) before anything touches the RW identity. The
          casts are the same per-format ones `dump()` applies — ISO
          strings for TIMESTAMP/DATETIME on CSV/NDJSON, and
          `TO_JSON_STRING` for JSON columns on parquet (BigQuery cannot
          export the native JSON type to parquet).
        - Row order: BigQuery preserves a user `ORDER BY` globally across
          exported shards, and shards are signed / concatenated in name
          order — so an ORDER BY in the SQL survives into the download.
          No default ordering is injected (arbitrary SQL has no `_id`).
        - Old revisions are GC'd on cache miss. A cacheable table-version
          change deletes the superseded rev immediately (like `dump()`) —
          once a table changes, that rev is permanently unreachable. A
          non-cacheable (uuid) rev age-gates deletion by the signed-URL
          expiry so a rapid re-run can't delete shards a client is still
          fetching.
        """
        import asyncio
        import hashlib
        from datetime import timedelta
        from uuid import uuid4

        if self.client is None:
            return []
        if self.mode != "ro":
            raise ServerError(
                "datastore_search_sql download must run on a read-only "
                "engine; got mode=" + repr(self.mode)
            )

        bucket = self._export_bucket()

        from google.cloud import bigquery

        try:
            qualified_sql = qualify_table_refs(
                sql,
                project=self.config.BIGQUERY_PROJECT,
                dataset=self.config.BIGQUERY_DATASET,
            )
        except Exception as e:
            raise ServerError(
                f"failed to qualify table references in SQL: {e}"
            ) from e

        tables: dict[str, Any] = {}
        for rid in resource_ids:
            tables[rid] = await self._get_table(rid)

        cacheable = (
            not (set(function_names) & _NON_DETERMINISTIC_SQL_FUNCTIONS)
            and all(t.modified is not None for t in tables.values())
        )
        if cacheable:
            pairs = sorted(
                (rid, int(t.modified.timestamp() * 1_000_000))
                for rid, t in tables.items()
            )
            rev = hashlib.sha256(
                "|".join(f"{rid}:{us}" for rid, us in pairs).encode()
            ).hexdigest()[:16]
        else:
            rev = uuid4().hex[:12]

        qhash = hashlib.sha256(qualified_sql.encode()).hexdigest()[:16]
        prefix = f"sql_dumps/{qhash}/{fmt}/{rev}"
        uri = f"gs://{bucket}/{prefix}_*.{_FMT[fmt]['ext']}"

        async def build_export_sql() -> str:
            # Free dry run on the RO client: validates the SQL under
            # read-only credentials and yields the output schema for the
            # per-format cast list. Deferred into `_cached_export`'s miss
            # branch so a cache hit never pays for it.
            dry_cfg = bigquery.QueryJobConfig(
                dry_run=True, use_query_cache=False,
            )
            try:
                dry_job = await asyncio.to_thread(
                    self.client.query, qualified_sql, job_config=dry_cfg,
                )
            except Exception as e:
                raise ValidationError(
                    f"sql failed BigQuery validation: {e}"
                ) from e
            return _export_statement(
                uri,
                fmt,
                _build_export_select(dry_job.schema, fmt),
                source=f"({qualified_sql})",
                suffix=_export_order_suffix(qualified_sql, dry_job.schema),
            )

        return await self._cached_export(
            bucket=bucket,
            prefix=prefix,
            fmt=fmt,
            cacheable=cacheable,
            build_export_sql=build_export_sql,
            sweep_prefix=f"sql_dumps/{qhash}/{fmt}/",
            # Cacheable rev = f(sql, table versions): a changed table makes
            # the old rev permanently unreachable → delete immediately
            # (like dump()). Non-cacheable uuid rev → age-gate by the
            # signed-URL expiry so a rapid re-run can't delete shards a
            # just-superseded run is still serving.
            gc_min_age=None if cacheable else timedelta(
                hours=getattr(
                    self.config, "BIGQUERY_EXPORT_URL_EXPIRY_HOURS", 1,
                ),
            ),
            filename_base=f"query_{qhash[:8]}",
            what=f"sql query {qhash[:8]}",
            hint="`format=csv` or `format=ndjson`",
        )

    # ----- shared export machinery (dump + dump_sql) -----------------------

    async def _cached_export(
        self,
        *,
        bucket: str,
        prefix: str,
        fmt: str,
        cacheable: bool,
        build_export_sql: Callable[[], Awaitable[str]],
        sweep_prefix: str,
        gc_min_age: Any = None,
        filename_base: str,
        what: str,
        hint: str = "`format=csv` or `format=ndjson`",
    ) -> list[str]:
        """Cache → export → GC → sign — the workflow shared by `dump` and
        `dump_sql`. They differ only in how the inputs are derived
        (`prefix`, `cacheable`, the export SQL, `sweep_prefix`,
        `filename_base`, GC age gate); everything below is identical.

          - `cacheable=True` → look for an existing revision at `prefix`
            and short-circuit to signing on a hit.
          - on miss, `build_export_sql()` produces the `EXPORT DATA` text
            (deferred so `dump_sql`'s dry run only runs when needed), the
            job runs under rw creds, shards are re-listed, then stale
            revisions under `sweep_prefix` are GC'd (`gc_min_age` age-gates
            deletion; `None` = delete immediately).
          - `hint` names the caller's format param in the 413 messages.

        Clients: ro for reads (GCS list); rw for the writes (EXPORT DATA
        writes shards under its identity; GCS delete + IAM-signed URLs).
        """
        import asyncio

        rw_bq = self._build_bq_client("rw")
        ro_gcs = self._build_storage_client("ro").bucket(bucket)
        rw_gcs = self._build_storage_client("rw").bucket(bucket)

        blobs = await self._sorted_blobs(ro_gcs, prefix) if cacheable else []

        if not blobs:
            export_sql = await build_export_sql()
            await self._run_export_job(
                rw_bq, export_sql, what=what, fmt=fmt, hint=hint,
            )
            log.info(
                "BigQuery export cache MISS: %s prefix=%s", what, prefix,
            )
            blobs = await self._sorted_blobs(rw_gcs, prefix)
            if not blobs:
                raise ServerError(
                    f"BigQuery EXPORT DATA wrote no shards for {what}; "
                    "check job logs."
                )

            # GC stale revisions under sweep_prefix. Best-effort.
            try:
                gc_count = await asyncio.to_thread(
                    self._gc_stale_blobs,
                    rw_gcs,
                    sweep_prefix=sweep_prefix,
                    keep_prefix=prefix,
                    min_age=gc_min_age,
                )
                if gc_count:
                    log.info(
                        "BigQuery export GC: %s removed=%d", what, gc_count,
                    )
            except Exception as gc_err:  # noqa: BLE001
                log.warning(
                    "BigQuery export GC failed for %s: %s", what, gc_err,
                )
        else:
            log.info(
                "BigQuery export cache HIT: %s prefix=%s shards=%d",
                what, prefix, len(blobs),
            )
            # Re-fetch via rw so the blobs we sign carry rw credentials
            # (signing needs IAM signBlob under workload identity).
            blobs = await self._sorted_blobs(rw_gcs, prefix)

        self._ensure_single_parquet_shard(blobs, fmt, what=what, hint=hint)
        return await self._sign_blobs(
            blobs, filename_base=filename_base, ext=_FMT[fmt]["ext"],
        )

    def _export_bucket(self) -> str:
        """Validated GCS export bucket name; `ServerError` when unset."""
        bucket = (
            getattr(self.config, "BIGQUERY_EXPORT_BUCKET", "") or ""
        ).strip()
        if not bucket:
            raise ServerError(
                "BIGQUERY_EXPORT_BUCKET is not configured — "
                "/datastore/dump cannot run without an export bucket."
            )
        return bucket

    async def _get_table(self, resource_id: str) -> Any:
        """Fetch one table's metadata off-thread, mapping BigQuery errors
        the way both `dump` and `dump_sql` want: NotFound → `NotFoundError`
        (404), anything else → `ServerError` (500).
        """
        import asyncio

        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        table_ref = bigquery.TableReference.from_string(
            f"{self.config.BIGQUERY_PROJECT}"
            f".{self.config.BIGQUERY_DATASET}.{resource_id}"
        )
        try:
            return await asyncio.to_thread(self.client.get_table, table_ref)
        except NotFound as e:
            raise NotFoundError(
                f"resource {resource_id!r} is not declared; nothing to dump"
            ) from e
        except Exception as e:
            raise ServerError(
                f"BigQuery get_table failed for resource {resource_id!r}: {e}"
            ) from e

    async def _sorted_blobs(self, bucket: Any, prefix: str) -> list[Any]:
        """`list_blobs(prefix=…)` off-thread, sorted by name so shard
        order (…_000, …_001, …) matches export order."""
        import asyncio

        return sorted(
            await asyncio.to_thread(
                lambda: list(bucket.list_blobs(prefix=prefix)),
            ),
            key=lambda x: x.name,
        )

    async def _run_export_job(
        self,
        rw_bq: Any,
        export_sql: str,
        *,
        what: str,
        fmt: str,
        hint: str = "`format=csv` or `format=ndjson`",
    ) -> None:
        """Submit an `EXPORT DATA` statement and poll it to completion.

        The poll loop releases the worker thread between `job.reload`
        calls. A ">1 GB single URI" failure maps to 413; anything else
        to `ServerError`. `what` names the export in error messages
        ("resource 'rid'" / "sql query <qhash8>"); `hint` names the
        caller's format parameter in the 413 advice.
        """
        import asyncio

        try:
            job = await asyncio.to_thread(rw_bq.query, export_sql)
        except Exception as e:
            raise ServerError(
                f"BigQuery EXPORT DATA submit failed for {what}: {e}"
            ) from e

        while True:
            await asyncio.to_thread(job.reload)
            if job.state == "DONE":
                break
            await asyncio.sleep(_DUMP_POLL_INTERVAL_SECONDS)

        if job.error_result:
            err_msg = (job.error_result or {}).get("message", "")
            if _is_export_too_large(RuntimeError(err_msg)):
                raise PayloadTooLargeError(
                    f"{what} exceeds 1 GB after export as {fmt!r}; "
                    "single-file download isn't possible. "
                    f"Try {hint} for sharded multi-file downloads instead."
                )
            raise ServerError(
                f"BigQuery EXPORT DATA failed for {what}: {err_msg}"
            )

    def _gc_stale_blobs(
        self,
        rw_gcs: Any,
        *,
        sweep_prefix: str,
        keep_prefix: str,
        min_age: Any = None,
    ) -> int:
        """Delete blobs under `sweep_prefix` not belonging to the current
        revision (`keep_prefix`). With `min_age` (a `timedelta`), blobs
        younger than that are kept — used by `dump_sql` so freshly
        written shards whose signed URLs may still be live survive a
        concurrent re-export. Sync — run via `asyncio.to_thread`.
        """
        from datetime import datetime, timezone

        cutoff = (
            datetime.now(timezone.utc) - min_age
            if min_age is not None
            else None
        )
        deleted = 0
        for old in rw_gcs.list_blobs(prefix=sweep_prefix):
            if old.name.startswith(keep_prefix):
                continue
            if cutoff is not None:
                created = getattr(old, "time_created", None)
                if created is not None and created > cutoff:
                    continue
            try:
                old.delete()
                deleted += 1
            except Exception as gc_err:  # noqa: BLE001
                log.warning(
                    "dump GC: failed to delete %s: %s", old.name, gc_err,
                )
        return deleted

    def _ensure_single_parquet_shard(
        self,
        blobs: list[Any],
        fmt: str,
        *,
        what: str,
        hint: str = "`format=csv` or `format=ndjson`",
    ) -> None:
        """Parquet shards can't be byte-concatenated → 413 when >1."""
        if fmt == "parquet" and len(blobs) > 1:
            raise PayloadTooLargeError(
                f"{what} exported as multiple parquet "
                "shards; single-file download isn't possible. Try "
                f"{hint} for sharded multi-file "
                "downloads instead."
            )

    async def _sign_blobs(
        self, blobs: list[Any], *, filename_base: str, ext: str,
    ) -> list[str]:
        """V4-sign every blob with an attachment filename — `<base>.<ext>`
        for a single shard, `<base>_NN.<ext>` (1-indexed) otherwise.
        Signing is offloaded to a thread (IAM round-trip under workload
        identity)."""
        import asyncio
        from datetime import timedelta

        expiry = timedelta(
            hours=getattr(self.config, "BIGQUERY_EXPORT_URL_EXPIRY_HOURS", 1),
        )

        def _sign_all() -> list[str]:
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

        return await asyncio.to_thread(_sign_all)

    def _build_bq_client(self, mode: str) -> Any:
        """Construct an on-demand BigQuery client for `mode` ("ro" / "rw").

        Used by the dump path's cache-miss branch to elevate to the rw
        SA for `EXPORT DATA` while keeping the rest of the engine on
        `self.client`. Tests stub this to inject mocks instead of
        patching `google.cloud.bigquery` globally.
        """
        from google.cloud import bigquery

        from datastore.infrastructure.engines.bigquery.client import (
            load_credentials,
        )

        creds = load_credentials(self.config, mode=mode)  # type: ignore[arg-type]
        kwargs: dict[str, Any] = {"project": self.config.BIGQUERY_PROJECT}
        if creds is not None:
            kwargs["credentials"] = creds
        return bigquery.Client(**kwargs)

    def _build_storage_client(self, mode: str) -> Any:
        """Construct an on-demand GCS client for `mode` ("ro" / "rw").

        Lazy import keeps `google-cloud-storage` an optional dep — only
        the dump path touches GCS, so test envs without the package
        don't need to install it. Tests stub this to inject mocks.
        """
        from google.cloud import storage

        from datastore.infrastructure.engines.bigquery.client import (
            load_credentials,
        )

        creds = load_credentials(self.config, mode=mode)  # type: ignore[arg-type]
        kwargs: dict[str, Any] = {"project": self.config.BIGQUERY_PROJECT}
        if creds is not None:
            kwargs["credentials"] = creds
        return storage.Client(**kwargs)

    def get_columns(self, resource_id: str) -> list[str]:
        """Return column names. Placeholder — returns `[]`."""
        return []

    def healthcheck(self) -> bool:
        """Probe BigQuery with a dry-run `SELECT 1`. Returns False on
        any failure so `/ready` can return 503 instead of crashing.
        """
        if self.client is None:
            return False
        now = time.monotonic()
        if (
            self._health is not None
            and now - self._health[0] < _HEALTHCHECK_TTL_SECONDS
        ):
            return self._health[1]

        from google.cloud import bigquery

        try:
            self.client.query(
                "SELECT 1",
                job_config=bigquery.QueryJobConfig(
                    dry_run=True, use_query_cache=False
                ),
                timeout=_HEALTHCHECK_TIMEOUT_SECONDS,
            )
            ok = True
        except Exception as e:
            ok = False
            log.warning(
                "BigQuery healthcheck failed (mode=%s): %s", self.mode, e
            )
        self._health = (now, ok)
        return ok

def _translate_bigquery_error(
    exc: ServerError, resource_id: str, action: str
) -> Exception:
    """Translate raw BQ write errors into actionable `ValidationError`s.

    Rewrites BQ messages whose literal text is unhelpful — e.g.
    *"Scalar subquery produced more than one element"* really means
    duplicate primary keys; *"Bad double value: jk"* means a non-numeric
    value for a `number` column. Handles: duplicate PKs, per-column
    type mismatches, out-of-range numerics, bad date/time literals.
    Other errors pass through unchanged.
    """
    import re

    from datastore.core.exceptions import ValidationError

    msg = str(exc)

    # `insert_guarded_sql` RAISEs "<sentinel> <count>" when an INSERT would
    # duplicate a primary key. Rebuild the user-facing message here (the
    # digit run ends at BigQuery's trailing metadata, so no markers needed).
    m = re.search(rf"{PK_CONFLICT_SENTINEL} (\d+)", msg)
    if m:
        return ValidationError(
            f"Found {m.group(1)} row(s) that would duplicate the primary "
            "key. Use method='upsert' to update existing rows, or "
            "deduplicate the records"
        )

    if "Scalar subquery produced more than one element" in msg:
        return ValidationError(
            "Found duplicated rows with the same primary key. "
            f"Deduplicate the records and retry the {action} operation."
        )

    # `Bad int64 value: <v>` etc. — type-coercion failure on CAST(JSON_VALUE).
    m = re.search(
        r"Bad (int64|double|bool|numeric|bignumeric) value: (.+?)(?:;|\\n|$)",
        msg,
        re.IGNORECASE,
    )
    if m:
        bq_type, bad_value = m.group(1).lower(), m.group(2).strip()
        return ValidationError(
            f"Value {bad_value!r} is not a valid "
            f"{_FRIENDLY_BQ_TYPE.get(bq_type, bq_type)}. "
            "Check that each record's column values match the resource "
            "schema's declared types."
        )

    # `Could not cast literal "<v>" to type <BQ_TYPE>` /
    # `Could not parse '<v>' as <BQ_TYPE>` — alternative phrasings for
    # the same coercion failure, depending on BigQuery version / path.
    m = re.search(
        r"Could not (?:cast literal|parse) ['\"](.+?)['\"] "
        r"(?:to type|as) (\w+)",
        msg,
    )
    if m:
        bad_value, bq_type = m.group(1), m.group(2).lower()
        return ValidationError(
            f"Value {bad_value!r} is not a valid "
            f"{_FRIENDLY_BQ_TYPE.get(bq_type, bq_type)}. "
            "Check that each record's column values match the resource "
            "schema's declared types."
        )

    # `Value out of range for INT64: <v>` / `Numeric value … out of range` —
    # the value parsed but doesn't fit the column type's range.
    m = re.search(
        r"(?:Value )?out of range(?: for (\w+))?:? (.+?)(?:;|\\n|$)",
        msg,
        re.IGNORECASE,
    )
    if m:
        bq_type = (m.group(1) or "").lower()
        bad_value = m.group(2).strip()
        friendly = _FRIENDLY_BQ_TYPE.get(bq_type, bq_type or "the column type")
        return ValidationError(
            f"Value {bad_value!r} is out of range for {friendly}. "
            "Use a wider type or check the input range."
        )

    # `Invalid date: <v>`, `Invalid timestamp: <v>`, `Invalid time: <v>` —
    # date/time literal that couldn't be parsed.
    m = re.search(
        r"Invalid (date|timestamp|datetime|time)(?: value)?: (.+?)(?:;|\\n|$)",
        msg,
        re.IGNORECASE,
    )
    if m:
        friendly, bad_value = m.group(1).lower(), m.group(2).strip()
        return ValidationError(
            f"Value {bad_value!r} is not a valid {friendly}. "
            "Check that each record's column values match the resource "
            "schema's declared types."
        )

    return exc


# BigQuery column-type name → Frictionless / user-friendly name.
_FRIENDLY_BQ_TYPE: dict[str, str] = {
    "int64":      "integer",
    "double":     "number",
    "float64":    "number",
    "numeric":    "number",
    "bignumeric": "number",
    "bool":       "boolean",
    "string":     "string",
    "date":       "date",
    "datetime":   "datetime",
    "timestamp":  "timestamp",
    "time":       "time",
    "json":       "object",
    "bytes":      "string",
}


# --- EXPORT DATA helpers -----------------------------------------------------

# Seconds between BigQuery job-status polls during a dump. Each poll
# is a quick metadata HTTP call (~tens of ms); between polls the worker
# thread is released so other requests can run. Bumping this down makes
# small jobs complete faster, bumping it up means fewer reload calls
# per job — 1 s is a safe middle.
_DUMP_POLL_INTERVAL_SECONDS = 1.0

# Per-format filename extension + BigQuery EXPORT DATA `format` value.
# BigQuery writes newline-delimited JSON to `.json` files; we keep that
# extension on the GCS object so clients see the file type they expect.
_FMT: dict[str, dict[str, str]] = {
    "csv":     {"ext": "csv",     "bq": "CSV"},
    "gzip":    {"ext": "csv.gz",  "bq": "CSV"},
    "ndjson":  {"ext": "json",    "bq": "JSON"},
    "parquet": {"ext": "parquet", "bq": "PARQUET"},
}

# SQL functions whose results change between runs. A GCS-cached export
# keyed on (sql, table revisions) is stale by definition for queries that
# call any of these, so `dump_sql` bypasses its cache for them. The list
# is a superset of the engine's allow-list on purpose — cheap insurance
# against the allow-list growing.
_NON_DETERMINISTIC_SQL_FUNCTIONS = frozenset({
    "now",
    "current_date",
    "current_datetime",
    "current_time",
    "current_timestamp",
    "rand",
    "generate_uuid",
    "session_user",
    "current_user",
})


def _export_statement(
    uri: str, fmt: str, select_list: str, source: str, suffix: str = "",
) -> str:
    """The `EXPORT DATA` statement text shared by `dump` and `dump_sql`.

    `header=true` is the documented default for CSV but some client
    versions / project configs treat it as false; be explicit so the
    column names always land in shard 0. NDJSON / Parquet ignore the
    option. `format=gzip` is CSV with BigQuery-side GZIP compression.

    `source` is the FROM target — a backticked table ref for `dump`, a
    parenthesised subquery for `dump_sql`. `suffix` carries `dump`'s
    ` ORDER BY \\`_id\\``.
    """
    extra_opts = ", header=true" if fmt in {"csv", "gzip"} else ""
    if fmt == "gzip":
        extra_opts += ", compression='GZIP'"
    return (
        f"EXPORT DATA OPTIONS("
        f"uri='{uri}', format='{_FMT[fmt]['bq']}', overwrite=true"
        f"{extra_opts}"
        ") AS "
        f"SELECT {select_list} FROM {source}{suffix}"
    )


def _export_order_suffix(sql: str, schema: Any) -> str:
    """Outer `ORDER BY` for a wrapped SQL export (`dump_sql`).

    BigQuery ignores a subquery's ORDER BY when it carries no LIMIT, so
    ordering the exported file requires the ORDER BY on the **outer**
    query — which is also what makes BigQuery preserve the order
    globally across shards.

      - The user SQL has a top-level ORDER BY → hoist a copy when every
        sort key is a plain column present in the export's output
        schema (aliases included; table qualifiers stripped — they
        don't exist outside the subquery). Non-hoistable keys
        (expressions, positions, non-output columns) → no outer order.
        The inner ORDER BY stays either way: with a LIMIT it decides
        *which* rows survive (top-N), the outer copy decides the file
        order.
      - No ORDER BY → `ORDER BY \\`_id\\`` when the output carries
        `_id`, matching the JSON path's `default_order_by` so a plain
        `SELECT *` downloads in the same order it pages.

    Returns ``""`` (unordered) when neither applies.
    """
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


def _build_export_select(schema: Any, fmt: str) -> str:
    """SELECT column list for EXPORT DATA.

    Parquet preserves native logical types, but BigQuery cannot export
    native JSON columns to Parquet. Keep `*` for the common no-JSON path;
    otherwise cast only JSON columns to strings. For CSV / NDJSON, every
    column goes through `format_select_column` (in `bigquery/lib.py`) —
    the same helper `datastore_search` uses — so a given column renders
    identically in a dump and in a search response.
    """
    if fmt == "parquet":
        fields = list(schema)
        if not any((f.field_type or "").upper() == "JSON" for f in fields):
            return "*"
        return ", ".join(
            f"TO_JSON_STRING(`{f.name}`) AS `{f.name}`"
            if (f.field_type or "").upper() == "JSON"
            else f"`{f.name}`"
            for f in fields
        )
    return ", ".join(
        format_select_column(f.name, f.field_type) for f in schema
    )


def _is_export_too_large(exc: BaseException) -> bool:
    """Does this BigQuery error look like ">1 GB single-file rejection"?

    BigQuery's exact wording shifts across SDK versions; both phrasings
    we've seen contain `single URI` or `wildcard`. False negatives just
    surface as a generic 500 instead of 413 — annoying but not silent.
    """
    msg = str(exc).lower()
    return "single uri" in msg or "wildcard" in msg
