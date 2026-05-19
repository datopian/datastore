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

    schema: dict  # {"fields": [{"name": "col", "type": "string"}, ...]}
    records: Iterator[tuple]
    total: int | None = None
    records_truncated: bool = False

    @property
    def columns(self) -> list[str]:
        return [f["name"] for f in self.schema.get("fields", [])]


@dataclass(slots=True)
class WriteResult:
    rows_written: int = 0
    total: int | None = None


@dataclass
class InfoResult:
    """Table metadata returned by `datastore_info`.

    `fields` is the legacy CKAN column shape (`[{"id", "type", ...}]`).
    `schema` is a Frictionless Table Schema (`{"fields": [...],
    "primaryKey": [...], ...}`). Engines populate both so callers on
    either side of the migration see what they expect — the service
    just passes them through.

    `meta` is a free-form dict for engine-specific extras (row count,
    table size, last modified, indexes, …) — the endpoint pipes it
    through verbatim, so engines can grow new keys without a schema
    change.
    """

    fields: list[dict]
    schema: dict
    meta: dict


class DatastoreBackend(ABC):
    @abstractmethod
    def initialize(self) -> None:
        """Called on app startup to set up connections."""

    @abstractmethod
    def create(
        self, resource_id: str, schema: dict, records: list | None, include_total: bool
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

        `q` is a CKAN-style full-text query: `str` scans every text column,
        `dict[col, term]` scans the named columns. `include_total=True`
        runs a `COUNT(*)` and sets `SearchResult.total`.
        """

    @abstractmethod
    def upsert(
        self, resource_id: str, records: list, method: str, include_total: bool
    ) -> WriteResult:
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
    def info(self, resource_id: str) -> InfoResult:
        """Return table metadata: column schema + free-form `meta` dict."""

    @abstractmethod
    def get_columns(self, resource_id: str) -> list[str]:
        """Return column names for a table (needed for full-text search across all columns)."""

    @abstractmethod
    def healthcheck(self) -> bool:
        """Return True if backend is reachable. Called by /ready probe."""
