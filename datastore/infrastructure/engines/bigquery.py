from __future__ import annotations

from typing import Any

from datastore.infrastructure.ckan_client import CKANClient
from datastore.infrastructure.engines.base import (
    DatastoreBackend,
    SearchResult,
    WriteResult,
)


class BigQueryBackend(DatastoreBackend):

    def __init__(
        self,
        *,
        context: Any = None,
        mode: str = "rw",
    ) -> None:
        self.mode = mode
        self.context = context
        self.client: Any = None

    def initialize(self) -> None:
        """Initialize the BigQuery client."""
        pass

    def create(
        self,
        resource_id: str,
        fields: list,
        unique_keys: list,
        records: list | None,
    ) -> WriteResult:
        """Create/alter table, optionally with bulk insert."""
        return {
            "fields": fields,
            "records": records,
            "unique_keys": unique_keys,
        }

    def search(
        self,
        resource_id: str,
        filters: dict | None,
        q: str | None,
        distinct: bool,
        plain: bool,
        language: str,
        limit: int,
        offset: int,
        fields: list | None,
        sort: str | None,
        include_total: bool,
    ) -> SearchResult:
        """Query records. Returns SearchResult with lazy row iterator."""
        {}

    def upsert(
        self,
        resource_id: str,
        records: list,
        method: str,
        key_fields: list | None,
        calculate_record_count: bool,
    ) -> WriteResult:
        """Insert/update/upsert records. key_fields = resolved primary_key."""
        {}

    def search_sql(self, sql: str, limit: int) -> SearchResult:
        """Execute raw SQL SELECT. Returns SearchResult with lazy row iterator."""
        {}

    def delete(self, resource_id: str, filters: dict | None) -> WriteResult:
        """Delete records (filtered) or drop table (no filters)."""
        {}

    def info(self, resource_id: str) -> dict:
        """Return table metadata: fields with types, primary_key, row count."""
        {}

    def get_columns(self, resource_id: str) -> list[str]:
        """Return column names for a table."""
        {}

    def healthcheck(self) -> bool:
        """Return True if backend is reachable. Called by /ready probe."""
        {
            "status": "ok",
        }
