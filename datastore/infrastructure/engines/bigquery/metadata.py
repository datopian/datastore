"""BigQuery implementation of the `MetadataStore` Protocol.

Stores one row per `resource_id` in a hidden `_table_metadata` table
that lives alongside the user data tables in `BIGQUERY_DATASET`. The
row carries the Frictionless schema declared at `datastore_create`
time plus `created_at` / `updated_at` timestamps so callers can
reconstruct the column declaration without re-parsing user tables.

The table is created on engine startup (`initialize()`) and updated via
parameterised `MERGE` from `create()`. Other engines (DuckLake,
Postgres, …) provide their own implementation of the same Protocol —
the backend layer only depends on the methods declared in
`engines/base.py:MetadataStore`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import orjson

from datastore.core.exceptions import ServerError

if TYPE_CHECKING:
    from google.cloud import bigquery

log = logging.getLogger(__name__)

# Hidden by convention: BigQuery treats leading-underscore tables as
# internal, hiding them from default list / autocomplete in most UIs.
METADATA_TABLE_NAME = "_table_metadata"


class BigQueryMetadataStore:
    """`MetadataStore` backed by a BigQuery table.

    Schema (DDL applied by `initialize`):

        resource_id  STRING     NOT NULL
        schema       JSON       NOT NULL
        created_at   TIMESTAMP  NOT NULL
        updated_at   TIMESTAMP  NOT NULL

    The table is keyed on `resource_id` at the application layer
    (BigQuery has no enforced PK / unique constraints); the `MERGE` in
    `upsert()` provides single-row semantics.
    """

    def __init__(
        self,
        *,
        client: bigquery.Client,
        project: str,
        dataset: str,
        table_name: str = METADATA_TABLE_NAME,
    ) -> None:
        self.client = client
        self.project = project
        self.dataset = dataset
        self.table_name = table_name

    @property
    def table_ref(self) -> str:
        """Fully-qualified `project.dataset.table` reference for SQL."""
        return f"`{self.project}.{self.dataset}.{self.table_name}`"

    def initialize(self) -> None:
        """Create the metadata table if it doesn't exist. Idempotent.

        Uses `CREATE TABLE IF NOT EXISTS` so concurrent pods racing to
        start up don't trip over each other. The dataset itself is
        assumed to exist — creating datasets is an out-of-band ops task,
        not something the application does at request time.
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_ref} (
            resource_id STRING NOT NULL,
            schema      JSON   NOT NULL,
            created_at  TIMESTAMP NOT NULL,
            updated_at  TIMESTAMP NOT NULL
        )
        """
        self._run(ddl, op="metadata CREATE TABLE", resource_id=None)
        log.info(
            "BigQuery metadata table ready: %s.%s.%s",
            self.project, self.dataset, self.table_name,
        )

    def insert(self, resource_id: str, schema: dict) -> None:
        """Insert a new metadata row for `resource_id`.

        Sets `created_at` / `updated_at` to now. Fails if a row already
        exists for this `resource_id` — that's a genuine conflict
        (duplicate `datastore_create`) that callers should surface.
        """
        sql = f"""
        INSERT INTO {self.table_ref}
            (resource_id, schema, created_at, updated_at)
        VALUES (
            @resource_id,
            PARSE_JSON(@schema),
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """
        self._run(
            sql,
            op="metadata INSERT",
            resource_id=resource_id,
            job_config=self._schema_params(resource_id, schema),
        )

    def update(self, resource_id: str, schema: dict) -> None:
        """Update the metadata row keyed by `resource_id`.

        Replaces `schema` and bumps `updated_at`; `created_at` is
        preserved. Plain `UPDATE` — no MERGE, no insert fallback. When
        no row matches the predicate the statement is a no-op.
        """
        sql = f"""
        UPDATE {self.table_ref}
        SET schema = PARSE_JSON(@schema),
            updated_at = CURRENT_TIMESTAMP()
        WHERE resource_id = @resource_id
        """
        self._run(
            sql,
            op="metadata UPDATE",
            resource_id=resource_id,
            job_config=self._schema_params(resource_id, schema),
        )

    def _schema_params(
        self, resource_id: str, schema: dict
    ) -> "bigquery.QueryJobConfig":
        """Build the `(resource_id, schema)` parameter set shared by
        `insert` and `update`. Keeps the SQL strings free of inline
        values and the marshalling rule in one place."""
        from google.cloud import bigquery

        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("resource_id", "STRING", resource_id),
                bigquery.ScalarQueryParameter(
                    "schema", "STRING", orjson.dumps(schema).decode("utf-8")
                ),
            ]
        )

    def get(self, resource_id: str) -> dict | None:
        """Return the stored Frictionless schema for `resource_id`,
        or `None` when no row exists."""
        sql = f"""
        SELECT TO_JSON_STRING(schema) AS schema_json
        FROM {self.table_ref}
        WHERE resource_id = @resource_id
        LIMIT 1
        """
        rows = list(
            self._run(
                sql,
                op="metadata SELECT",
                resource_id=resource_id,
                job_config=self._resource_id_params(resource_id),
            )
        )
        if not rows:
            return None
        raw = rows[0]["schema_json"]
        parsed: Any = orjson.loads(raw)
        return parsed if isinstance(parsed, dict) else None

    def delete(self, resource_id: str) -> None:
        """Remove the metadata row for `resource_id`. No-op when absent."""
        sql = f"DELETE FROM {self.table_ref} WHERE resource_id = @resource_id"
        self._run(
            sql,
            op="metadata DELETE",
            resource_id=resource_id,
            job_config=self._resource_id_params(resource_id),
        )

    def _run(
        self,
        sql: str,
        *,
        op: str,
        resource_id: str | None,
        job_config: "bigquery.QueryJobConfig | None" = None,
    ) -> Any:
        """Run a metadata SQL statement and wait for completion.

        Wraps every `client.query` so transport / SQL failures arrive at
        callers as `ServerError` with the operation name + the
        `resource_id` being touched (or `<init>` for `initialize`),
        rather than raw `google.api_core` exceptions.
        """
        try:
            return self.client.query(sql, job_config=job_config).result()
        except Exception as e:
            target = resource_id if resource_id is not None else "<init>"
            raise ServerError(
                f"BigQuery {op} failed for resource {target!r}: {e}"
            ) from e

    def _resource_id_params(
        self, resource_id: str
    ) -> "bigquery.QueryJobConfig":
        """Job config carrying just the `resource_id` parameter (for
        `get` and `delete` which don't bind a schema)."""
        from google.cloud import bigquery

        return bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "resource_id", "STRING", resource_id
                ),
            ]
        )
