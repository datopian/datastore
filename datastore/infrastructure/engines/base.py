from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Protocol, runtime_checkable


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


@runtime_checkable
class MetadataStore(Protocol):
    """Per-engine storage for table-level metadata.

    Holds one row per `resource_id`, keyed by the resource_id itself. The
    canonical column shape is `(resource_id, schema, created_at,
    updated_at)` where `schema` is a Frictionless Table Schema dict.

    Each engine subpackage provides a concrete implementation
    (e.g. `bigquery/metadata.py: BigQueryMetadataStore`) so the SQL
    dialect, connection management, and column types stay engine-private.
    The backend constructs its store in `__init__`, calls `initialize()`
    once at startup to create the underlying table, and calls `upsert`
    from `create()` whenever a caller declares a new resource.

    Adding a new engine = drop a sibling `metadata.py` implementing this
    Protocol; the backend wires it in by holding `self.metadata`.
    """

    def initialize(self) -> None:
        """Create the metadata table if it doesn't exist. Idempotent."""

    def insert(self, resource_id: str, schema: dict) -> None:
        """Insert a new metadata row for `resource_id`.

        Sets `created_at` and `updated_at` to now. Fails if a row with
        the same `resource_id` already exists — that's a real conflict
        that callers should surface (a second `datastore_create` for an
        already-declared resource).
        """

    def update(self, resource_id: str, schema: dict) -> None:
        """Update the metadata row for `resource_id`.

        Replaces `schema` and bumps `updated_at`; `created_at` is
        preserved. Keyed on `resource_id`; no-op when the row is absent.
        """

    def get(self, resource_id: str) -> dict | None:
        """Return the stored Frictionless schema for `resource_id`,
        or `None` when no row exists."""

    def delete(self, resource_id: str) -> None:
        """Remove the metadata row for `resource_id`. No-op when absent."""


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
