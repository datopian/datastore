from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator


@dataclass
class SearchResult:
    """Lightweight result container.

    `records` is a lazy iterator of tuples — no dicts, no Pydantic.
    The API layer streams it straight to the response body so peak
    memory stays ≈ 1 row regardless of how many rows the engine
    returns. Don't materialise this iterator anywhere except inside
    the streaming serialiser.
    """
    fields: list[dict]  # [{"id": "col_name", "type": "text"}, ...]
    records: Iterator[tuple]
    total: int | None = None
    records_truncated: bool = False

    @property
    def columns(self) -> list[str]:
        return [f["id"] for f in self.fields]


@dataclass(slots=True)
class WriteResult:
    rows_written: int = 0
    total: int | None = None


class DatastoreBackend(ABC):

    @abstractmethod
    def initialize(self) -> None:
        """Called on app startup to set up connections."""

    @abstractmethod
    def create(self, resource_id: str, fields: list, unique_keys: list,
               records: list | None, include_total: bool) -> WriteResult:
        """Create/alter table, optionally with bulk insert.

        `include_total=True` → after the insert, recompute and return the
        total row count via `WriteResult.total`. `False` → leave it `None`.
        """

    @abstractmethod
    def search(self, resource_id: str, filters: dict | None,
               q: str | dict | None,
               distinct: bool, plain: bool, language: str, limit: int,
               offset: int, fields: list | None, sort: str | None,
               include_total: bool) -> SearchResult:
        """Query records. Returns SearchResult with lazy row iterator.

        `q` is a CKAN-style full-text query: `str` scans every text column,
        `dict[col, term]` scans the named columns. `include_total=True`
        runs a `COUNT(*)` and sets `SearchResult.total`.
        """

    @abstractmethod
    def upsert(self, resource_id: str, records: list, method: str,
               include_total: bool) -> WriteResult:
        """Insert / update / upsert records.

        `include_total=True` → after the write, recompute and return the
        total row count via `WriteResult.total`. `False` → leave it `None`.
        """

    @abstractmethod
    def search_sql(self, sql: str, limit: int) -> SearchResult:
        """Execute raw SQL SELECT. Returns SearchResult with lazy row iterator."""

    @abstractmethod
    def delete(self, resource_id: str, filters: dict | None) -> WriteResult:
        """Delete records (filtered) or drop table (no filters)."""

    @abstractmethod
    def info(self, resource_id: str) -> dict:
        """Return table metadata: fields with types, primary_key, row count."""

    @abstractmethod
    def get_columns(self, resource_id: str) -> list[str]:
        """Return column names for a table (needed for full-text search across all columns)."""

    @abstractmethod
    def healthcheck(self) -> bool:
        """Return True if backend is reachable. Called by /ready probe."""
