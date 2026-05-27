from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any


@dataclass
class SearchResult:
    """Lightweight result container.

    `records` is a lazy iterator of tuples — no dicts, no Pydantic.
    The API layer streams it straight to the response body so peak
    memory stays ≈ 1 row regardless of how many rows the engine
    returns. Don't materialise this iterator anywhere except inside
    the streaming serialiser.
    """

    schema: dict[str, Any]  # {"fields": [{"name": "col", "type": "string"}, ...]}
    records: Iterator[tuple[Any, ...]]
    total: int | None = None
    records_truncated: bool = False

    @property
    def columns(self) -> list[str]:
        return [f["name"] for f in self.schema.get("fields", [])]


@dataclass(slots=True)
class WriteResult:
    rows_written: int = 0
    total: int | None = None
    # Resulting Frictionless schema after the write — populated by
    # `delete()`'s column-drop path so the response can echo the table's
    # shape minus the dropped columns. `None` for other write paths.
    schema: dict[str, Any] | None = None


@dataclass
class InfoResult:
    """Table metadata returned by `datastore_info`.


    """

    schema: dict[str, Any]
    meta: dict[str, Any]


class DatastoreBackend(ABC):
    @abstractmethod
    def initialize(self) -> None:
        """Called on app startup to set up connections."""

    @abstractmethod
    def create(
        self,
        resource_id: str,
        schema: dict[str, Any],
        records: list[dict[str, Any]] | None,
        include_total: bool,
    ) -> WriteResult:
        """Create/alter table, optionally with bulk insert.

        `schema` is a Frictionless Table Schema descriptor — the service
        normalises both the legacy `fields` input and a caller-supplied
        Frictionless schema down to this shape before dispatch. Engines
        read columns from `schema["fields"]` and the unique key from
        `schema.get("primaryKey")`.

        `include_total=True` → after the insert, recompute and return the
        total row count via `WriteResult.total`. `False` → leave it `None`.
        """

    @abstractmethod
    def search(
        self,
        resource_id: str,
        filters: dict[str, Any] | None,
        q: str | dict[str, Any] | None,
        distinct: bool,
        plain: bool,
        language: str,
        limit: int,
        offset: int,
        fields: list[str] | None,
        sort: str | None,
        include_total: bool,
    ) -> SearchResult:
        """Query records. Returns SearchResult with lazy row iterator.

        `q` is a CKAN-style full-text query: `str` scans every text column,
        `dict[col, term]` scans the named columns. `include_total=True`
        runs a `COUNT(*)` and sets `SearchResult.total`.
        """

    @abstractmethod
    def upsert(
        self,
        resource_id: str,
        records: list[dict[str, Any]],
        method: str,
        include_total: bool,
    ) -> WriteResult:
        """Insert / update / upsert records.

        `include_total=True` → after the write, recompute and return the
        total row count via `WriteResult.total`. `False` → leave it `None`.
        """

    @abstractmethod
    def search_sql(self, sql: str, limit: int) -> SearchResult:
        """Execute raw SQL SELECT. Returns SearchResult with lazy row iterator."""

    @abstractmethod
    def delete(
        self,
        resource_id: str,
        filters: dict[str, Any] | None,
        fields: list[str] | None = None,
    ) -> WriteResult:
        """Drop the table (both None), delete rows by `filters`, or
        drop columns by `fields`. `filters` and `fields` are mutually
        exclusive."""

    @abstractmethod
    def info(self, resource_id: str) -> InfoResult:
        """Return table metadata: column schema + free-form `meta` dict."""

    @abstractmethod
    async def dump(self, resource_id: str, fmt: str) -> list[str]:
        """Download a table as CSV/NDJSON/Parquet. 
        """

    @abstractmethod
    def get_columns(self, resource_id: str) -> list[str]:
        """Return column names for a table (needed for full-text search across all columns)."""

    @abstractmethod
    def healthcheck(self) -> bool:
        """Return True if backend is reachable. Called by /ready probe."""
