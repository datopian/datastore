"""BigQuery backend.

Public surface is `BigQueryBackend` — the `DatastoreBackend` ABC.
File layout (top to bottom):

  1. Lifecycle (`__init__`, `initialize`).
  2. Low-level client wrappers (`_data_table_ref`, `_run_query`) —
     every BigQuery call is routed through `_run_query` so transport /
     SQL errors surface as `ServerError` with `resource_id` + operation
     name baked in, never as raw `google.api_core` exceptions.
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
from datastore.core.exceptions import (
    NotFoundError,
    ServerError,
    ValidationError,
)
from datastore.infrastructure.engines.base import (
    DatastoreBackend,
    InfoResult,
    MetadataStore,
    SearchResult,
    WriteResult,
)
from datastore.infrastructure.engines.bigquery.lib import (
    SYSTEM_COLUMN_NAMES,
    alter_clauses,
    column_defs,
    delete_sql,
    drop_columns_sql,
    insert_sql,
    merge_sql,
    qualify_table_refs,
    reject_unsupported_type_changes,
    schema_diff,
    strip_limit_offset,
    unfiltered_table_name,
    update_sql,
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
        """QueryJobConfig for read paths — enables BigQuery's query cache.

        BigQuery caches the result of every deterministic SELECT for
        ~24h; an identical query hits the cache and returns free + fast
        (no bytes scanned, sub-100ms typically). The flag is on by
        default in BigQuery, but every read site builds its config
        through this helper so:
          - the read-side contract is explicit in the code,
          - the `BIGQUERY_USE_QUERY_CACHE` opt-out actually flows
            through to the wire (e.g. integration tests that need
            a fresh scan can set it to False).

        Write paths (DDL / DML) don't go through this — BigQuery's
        cache only applies to SELECT anyway.
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
        """Submit `sql`, wait for completion, and return the QueryJob.

        Wraps every `client.query` call so any
        `google.api_core` / transport error becomes a CKAN-shaped
        `ServerError` carrying the action name (`op`) and target
        `resource_id`. Callers never have to know about Google's
        exception hierarchy.

        Returning the `QueryJob` (rather than its `.result()` value)
        lets callers grab whichever output they need without a second
        helper: rows from `job.result()`, DML row counts from
        `job.num_dml_affected_rows`. DDL / MERGE callers simply ignore
        the return value — the `.result()` call inside has already
        waited for completion.
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
    def _create_data_table(self, resource_id: str, schema: dict) -> None:
        """`CREATE TABLE IF NOT EXISTS` with columns derived from the
        Frictionless schema. Idempotent — a second call on the same
        resource is a no-op DDL on the BigQuery side."""
        cols = column_defs(schema, include_updated_at=self._include_updated_at)
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
        """Insert rows via DML `INSERT INTO ... SELECT FROM UNNEST(@rows)`.

        Why DML rather than `Client.insert_rows_json`: the streaming
        insert API parks rows in a streaming buffer for 30–90 minutes,
        and DML statements (UPDATE / DELETE / MERGE) cannot touch rows
        still in that buffer. That makes `datastore_create` + immediate
        `datastore_upsert` impossible. DML INSERT writes straight to
        table storage, so any follow-up upsert/update on the same
        primaryKey works without delay.

        Rows ride as a single JSON-array string parameter `@rows`;
        BigQuery unpacks it inside the SQL — one statement regardless
        of batch size, no Python-side serialisation pass needed (JSON
        columns are handled by `PARSE_JSON(JSON_QUERY(...))` inside
        the SELECT).

        Empty `records` is a no-op. SQL/transport errors propagate as
        `ServerError` via `_run_query`.
        """
        import orjson

        if not records:
            return
        try:
            sql = insert_sql(
                self._data_table_ref(resource_id),
                schema,
                include_updated_at=self._include_updated_at,
            )
        except ValueError as e:
            raise ValidationError(str(e)) from e

        from google.cloud import bigquery

        # `MAX(_id)` is computed inline in the INSERT SQL — saves a
        # separate round-trip per call (the older two-statement form
        # cost ~1s of BigQuery job overhead for nothing).
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "rows", "STRING", orjson.dumps(records).decode("utf-8")
                ),
            ]
        )
        try:
            self._run_query(
                sql, op="INSERT", resource_id=resource_id,
                job_config=job_config,
            )
        except ServerError as e:
            raise _translate_bigquery_error(
                e, resource_id, "insert"
            ) from e
        log.info(
            "BigQuery rows inserted: %s (%d row(s))",
            resource_id, len(records),
        )

    def _merge_records(
        self, resource_id: str, schema: dict, records: list
    ) -> None:
        """Upsert rows via `MERGE` keyed on `schema.primaryKey`.

        Rows whose primary-key columns match an existing row are
        UPDATEd; others are INSERTed. The full payload travels as a
        single JSON-array string parameter so we issue one statement
        regardless of batch size.

        Empty `records` is a no-op. Missing primary key on the stored
        schema raises `ValidationError` — upsert can't dedup without
        one; the caller can fall back to `method="insert"` or declare
        a primaryKey on the resource.
        """
        import orjson

        if not records:
            return
        try:
            sql = merge_sql(
                self._data_table_ref(resource_id),
                schema,
                include_updated_at=self._include_updated_at,
            )
        except ValueError as e:
            raise ValidationError(str(e)) from e

        from google.cloud import bigquery

        # `MAX(_id)` is inlined in the MERGE's WHEN NOT MATCHED clause
        # so the upsert is a single round-trip.
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "rows", "STRING", orjson.dumps(records).decode("utf-8")
                ),
            ]
        )
        try:
            self._run_query(
                sql, op="MERGE", resource_id=resource_id,
                job_config=job_config,
            )
        except ServerError as e:
            raise _translate_bigquery_error(e, resource_id, "upsert") from e
        log.info(
            "BigQuery rows upserted: %s (%d row(s))",
            resource_id, len(records),
        )

    def _update_records(
        self, resource_id: str, schema: dict, records: list
    ) -> None:
        """Update existing rows via DML `UPDATE`, keyed on
        `schema.primaryKey`.

        Update-only semantics: every row in `records` must match an
        existing row by primary key. After the statement runs we
        compare `num_dml_affected_rows` against the row count and
        raise `NotFoundError` if any row had no matching key — DML
        UPDATE itself treats misses as a silent no-op, so the count
        check is what gives the caller a real signal.

        Empty `records` is a no-op. Missing primary key or all-PK
        schema raises `ValidationError` (via `update_sql`'s
        `ValueError` re-raise).
        """
        import orjson

        if not records:
            return
        try:
            sql = update_sql(
                self._data_table_ref(resource_id),
                schema,
                include_updated_at=self._include_updated_at,
            )
        except ValueError as e:
            raise ValidationError(str(e)) from e

        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "rows", "STRING", orjson.dumps(records).decode("utf-8")
                ),
            ]
        )
        try:
            job = self._run_query(
                sql, op="UPDATE", resource_id=resource_id,
                job_config=job_config,
            )
        except ServerError as e:
            raise _translate_bigquery_error(e, resource_id, "update") from e
        affected = job.num_dml_affected_rows or 0
        if affected < len(records):
            missing = len(records) - affected
            raise NotFoundError(
                f"datastore_update: {missing} of {len(records)} row(s) "
                f"had no matching primary key in resource {resource_id!r}; "
                "use method='upsert' to insert missing rows"
            )
        log.info(
            "BigQuery rows updated: %s (%d row(s))", resource_id, affected,
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
        """Insert / update / upsert records into an existing resource.

        Method dispatch:
          - **"upsert"** (default): `MERGE` keyed on `schema.primaryKey`.
            Rows that match an existing key are UPDATEd; the rest are
            INSERTed. Requires a `primaryKey` on the stored schema.
          - **"insert"**: plain streaming insert (no PK check). Faster
            than upsert; raises if any row collides with an existing
            primary key (BigQuery row-level errors).
          - **"update"**: DML `UPDATE` keyed on `schema.primaryKey`.
            Every row must match an existing row — otherwise
            `NotFoundError` is raised after the statement runs. Requires
            a `primaryKey`.

        The resource must have been declared by `datastore_create`
        first; the schema (column types + primaryKey) is read from the
        metadata store and used to build the SQL. Calling `upsert` on
        an undeclared resource raises `NotFoundError`.

        Placeholder mode (no project/dataset) is a no-op echo so the
        unit suite can exercise the call path without GCP creds.
        """
        if self.metadata is None:
            # Placeholder mode — echo (matches the create() pattern).
            return {
                "resource_id": resource_id,
                "records": records,
                "method": method,
                "include_total": include_total,
                "total": len(records or []),
            }

        schema = self.metadata.get(resource_id)
        if schema is None:
            raise NotFoundError(
                f"resource {resource_id!r} is not declared; call "
                "datastore_create before upsert"
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
        """Run a parameterised SELECT against the data table.

        Pipeline:
          1. Resolve schema from `_table_metadata` (404 if undeclared).
          2. Build search + (optional) count SQL via `search.py`.
             Validation of `fields` / `sort` / `filters` / `q` columns
             happens inside the builders so a bad request becomes a
             clean 400, never reaches BigQuery.
          3. Submit both queries. When only an unfiltered total is
             needed, fall back to `__TABLES__.row_count` — free vs the
             COUNT(*) billing.
          4. Return a row iterator that yields tuples in projection
             order; memory stays bounded by the RowIterator's page
             size, not the result set size.

        `plain` and `language` are accepted for CKAN compatibility but
        currently have no effect on the BigQuery side — `SEARCH()`
        tokenises uniformly regardless of `plain`, and we don't expose
        the analyzer arg.

        Placeholder mode (no metadata store) returns an empty result so
        the unit suite can exercise the call path without GCP creds.
        """
        from datastore.infrastructure.engines.bigquery.search import (
            build_count,
            build_search,
            needs_count_query,
        )

        if self.metadata is None:
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

        schema = self.metadata.get(resource_id)
        if schema is None:
            raise NotFoundError(
                f"resource {resource_id!r} is not declared; call "
                "datastore_create first"
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
        count_job = None
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
            count_job = self.client.query(count_sql, job_config=count_cfg)

        search_job = self.client.query(sql, job_config=job_config)

        try:
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
        """Execute a vetted SELECT/WITH statement and stream tuples.

        Safety relies on three layers, none of which this method itself
        re-checks (validation already happened upstream):
          1. The request schema rejects non-SELECT / multi-statement
             / unparseable SQL (`schemas/request.py:DatastoreSearchSQLRequest`).
          2. The endpoint authorises every referenced table against
             CKAN as a resource_id, and the service rejects function
             calls outside the engine's allow-list.
          3. **The load-bearing guard:** this engine is built with the
             read-only credential (`mode="ro"` selects `BIGQUERY_CREDENTIALS_RO`),
             so BigQuery IAM physically refuses any DML / DDL even if
             upstream checks were bypassed. The assertion below catches
             the dev mistake of dispatching `search_sql` through the
             rw engine.

        Result schema is read from BigQuery's job schema (column types
        come back as BQ types and are mapped to Frictionless via
        `frictionless_type_from_bigquery`). Row output is bounded by
        `limit` via `itertools.islice` so a runaway SELECT without an
        embedded LIMIT can't pin the streaming response open forever.
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

        # Pick the cheapest viable path for `total`:
        #
        #   1. Plain `SELECT cols FROM table [LIMIT/OFFSET]` (no
        #      WHERE/GROUP/JOIN/aggregate) → read `total_rows` from
        #      `INFORMATION_SCHEMA.TABLE_STORAGE`. Free metadata query,
        #      no bytes scanned.
        #
        #   2. Anything that filters, joins, aggregates, or otherwise
        #      changes row count → wrap the user's SQL (LIMIT/OFFSET
        #      stripped) in `SELECT COUNT(*) FROM (...)`. Same pattern
        #      datastore_search uses for filtered/distinct queries.
        #
        # `RowIterator.total_rows` alone won't do — it's the row count
        # of the destination temp table (post-LIMIT page size), so
        # building pagination from it would always say "last page".
        count_sql: str | None
        count_params: list = []
        try:
            table = unfiltered_table_name(qualified_sql)
            if table is not None:
                count_sql = (
                    "SELECT total_rows AS n FROM "
                    f"`{self.config.BIGQUERY_PROJECT}."
                    f"{self.config.BIGQUERY_DATASET}."
                    "INFORMATION_SCHEMA.TABLE_STORAGE` "
                    "WHERE table_name = @table_name"
                )
                from google.cloud import bigquery
                count_params = [
                    bigquery.ScalarQueryParameter(
                        "table_name", "STRING", table,
                    ),
                ]
            else:
                inner = strip_limit_offset(qualified_sql)
                count_sql = f"SELECT COUNT(*) AS n FROM ({inner})"
        except Exception as e:
            log.warning(
                "search_sql: could not build COUNT query (%s); "
                "total will be omitted",
                e,
            )
            count_sql = None

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
                qualified_sql, job_config=self._read_job_config(),
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

    def delete(
        self,
        resource_id: str,
        filters: dict[str, Any] | None,
        fields: list[str] | None = None,
    ) -> WriteResult:
        """Drop the table (both None), delete rows by `filters`, or
        drop columns by `fields`. Schema layer enforces mutual
        exclusivity."""
        if self.metadata is None:
            return WriteResult()

        schema = self.metadata.get(resource_id)
        if schema is None:
            raise NotFoundError(
                f"resource {resource_id!r} is not declared; nothing to delete"
            )

        if fields is not None:
            self._drop_columns(resource_id, schema, fields)
            return WriteResult()

        if filters is None:
            self._drop_data_table(resource_id)
            self.metadata.delete(resource_id)
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
    ) -> None:
        """``ALTER TABLE DROP COLUMN …`` + rewrite the stored schema.
        Rejects system columns, unknown columns, and PK columns."""
        assert self.metadata is not None

        existing = {
            f["name"]
            for f in schema.get("fields", [])
            if f.get("name")
        }
        pk_raw = schema.get("primaryKey")
        pk: set[str] = (
            {pk_raw} if isinstance(pk_raw, str)
            else set(pk_raw or [])
        )

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
        self.metadata.update(resource_id, new_schema)
        log.info(
            "BigQuery columns dropped: %s (%s)", resource_id, sorted(fields),
        )

    def info(self, resource_id: str) -> InfoResult:
        """Return the table schema + row stats for a resource.

        Reads `schema` from the engine-managed `_table_metadata` (not
        BigQuery's `INFORMATION_SCHEMA`) so the `primaryKey` and per-
        field `info` data dictionary round-trip exactly as declared at
        `datastore_create`. Row count comes from a `COUNT(*)` on the
        data table.

        Placeholder mode (no metadata store) returns a stub so the unit
        suite can exercise the call path without GCP creds.
        """
        if self.metadata is None:
            return InfoResult(
                schema={"fields": []},
                meta={"resource_id": resource_id, "total": 0},
            )

        schema = self.metadata.get(resource_id)
        if schema is None:
            raise NotFoundError(
                f"resource {resource_id!r} is not declared; call "
                "datastore_create first"
            )

        total = self._count_rows(resource_id)

        pk_raw = schema.get("primaryKey")
        pk: list[str] = (
            [pk_raw] if isinstance(pk_raw, str) else list(pk_raw or [])
        )

        return InfoResult(
            schema=schema,
            meta={
                "resource_id": resource_id,
                "total": total,
                "primary_key": pk,
            },
        )

    def _count_rows(self, resource_id: str) -> int:
        """`COUNT(*)` against the data table; returns 0 on missing table.

        A missing data table while metadata exists is an inconsistent
        state (manual cleanup, partial drop). Logging it as a warning
        and returning 0 keeps `datastore_info` informative rather than
        500-ing the whole call.
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

    def get_columns(self, resource_id: str) -> list[str]:
        """Return column names for a table.

        Placeholder — replaced when real `search` lands. Empty list
        keeps callers from crashing on the dead code path.
        """
        return []

    def healthcheck(self) -> bool:
        """Probe the BigQuery client with `SELECT 1`. Returns False on
        any failure so `/ready` can return 503 instead of crashing.
        """
        if self.client is None:
            return False
        if (
            self.config is not None
            and self.config.BIGQUERY_PROJECT.strip()
            and self.metadata is None
        ):
            log.warning(
                "BigQuery healthcheck failed (mode=%s): metadata store "
                "unavailable — set BIGQUERY_DATASET.",
                self.mode,
            )
            return False
        try:
            self.client.query("SELECT 1").result()
            return True
        except Exception as e:
            log.warning(
                "BigQuery healthcheck failed (mode=%s): %s", self.mode, e
            )
            return False


def _translate_bigquery_error(
    exc: ServerError, resource_id: str, action: str
) -> Exception:
    """Map known BigQuery error signatures (raised on INSERT / MERGE /
    UPDATE against the JSON-array source) to clear `ValidationError`s.

    BigQuery's raw messages are technically accurate but unhelpful —
    e.g. *"Scalar subquery produced more than one element"* really
    means "your records have duplicate primary keys" and *"Bad double
    value: jk"* means "you sent the string 'jk' for a `number`
    column". Both surface as 400 ValidationError with a message that
    names the actual problem.

    Patterns handled:
      - duplicate primaryKey rows in the batch;
      - per-column type mismatches (`Bad <type> value: …`,
        `Could not cast …`, `Could not parse …`);
      - out-of-range numeric values (`Value out of range …`);
      - bad date / time / timestamp literals (`Invalid <type>: …`).

    Other errors pass through unchanged so the caller can re-raise as
    a generic `ServerError`.
    """
    import re

    from datastore.core.exceptions import ValidationError

    msg = str(exc)

    if "Scalar subquery produced more than one element" in msg:
        return ValidationError(
            "Found duplicated rows with the same primary key. "
            f"Deduplicate the input batch and retry the {action} operation."
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

