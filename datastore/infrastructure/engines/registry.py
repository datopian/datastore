"""Engine selection.
`get_datastore_engine(context, mode)` picks a backend based on
`context.config.DATASTORE_ENGINE` and returns an initialised instance.
The factory is intentionally pure — the FastAPI lifespan owns the
lifecycle, this module just decides which class to construct.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from datastore.infrastructure.engines.base import DatastoreBackend

if TYPE_CHECKING:  # type-only — no runtime import from api/
    from datastore.api.context import RequestContext

Mode = Literal["rw", "ro"]


def get_datastore_engine(
    context: RequestContext,
    *,
    mode: Mode = "rw",
) -> DatastoreBackend:
    """Return an engine instance for the requested mode.

    mode:
        "rw" — read-write: uses the primary credential set. Used by
               create / upsert / delete.
        "ro" — read-only: uses the `*_RO` credential variants, falling back
               to the RW ones when unset. Used by search / search_sql / info.

    The mode-based split exists so a compromised read path cannot be turned
    into a write path: least-privilege enforcement happens at the credential
    layer, not at a code-level flag we might forget to check.

    `context` is threaded through to the backend constructor so adapters
    can reach the per-request handles (config, the bound CKAN client)
    without re-resolving them. The factory itself only reads
    `context.config.DATASTORE_ENGINE` to pick a class.
    """
    engine = context.config.DATASTORE_ENGINE

    if engine == "bigquery":
        from datastore.infrastructure.engines.bigquery import BigQueryBackend
        return BigQueryBackend(context=context, mode=mode)
    if engine == "ducklake":
        raise NotImplementedError("DuckLake backend is not implemented yet")
    raise ValueError(f"Unknown DATASTORE_ENGINE: {engine!r}")
