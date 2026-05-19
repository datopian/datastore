"""Engine selection.
`get_datastore_engine(context, mode)` picks a backend based on
`context.config.DATASTORE_ENGINE` and returns an initialised instance.
The factory is intentionally pure — the FastAPI lifespan owns the
lifecycle, this module just decides which class to construct.
"""

from __future__ import annotations

import functools
import importlib
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from datastore.infrastructure.engines.base import DatastoreBackend

if TYPE_CHECKING:  # type-only — no runtime import from api/
    from datastore.api.context import RequestContext

Mode = Literal["rw", "ro"]

_ENGINES_DIR = Path(__file__).parent


@functools.lru_cache(maxsize=8)
def get_allowed_sql_functions(
    engine: str, *, override_path: str | None = None
) -> frozenset[str]:
    """Per-engine `datastore_search_sql` function allow-list.

    Default path: `<engine>/allowed_functions.txt` inside this directory
    (each engine ships its own list). `override_path` (typically from
    `Config.SQL_FUNCTIONS_ALLOW_FILE`) replaces that — useful for ops to
    tighten or loosen the list per deployment without code changes.

    `override_path` is `str` so pydantic-settings can deserialize the
    env var without forward-ref issues across pydantic versions; we
    convert to `Path` here.

    File format: one function name per line; lines starting with `#` and
    blank lines are ignored. Names are lower-cased to match the output
    of `parse_sql_references`.

    Returns an empty frozenset if no file exists at the resolved path —
    callers should still rely on per-table auth + read-only credentials
    as the load-bearing safety layer.

    Cached per (engine, override_path) pair so file I/O happens once.
    """
    path = Path(override_path) if override_path else (
        _ENGINES_DIR / engine / "allowed_functions.txt"
    )
    if not path.exists():
        return frozenset()

    names: set[str] = set()
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            names.add(line.lower())
    return frozenset(names)



_INSTANCES: dict[tuple[str, Mode], DatastoreBackend] = {}

def _build_engine(engine: str, mode: Mode, *, config, context=None):
    """Import the engine package and instantiate its `Backend` class.

    Engine packages expose a `Backend` symbol pointing at their concrete
    `DatastoreBackend` subclass (e.g. `BigQueryBackend`, future
    `DucklakeBackend`, `PostgresBackend`). Decoupling the registry from
    any specific class name keeps the dispatch engine-agnostic — adding
    a new backend is a folder drop with a `Backend = …` re-export.
    """
    try:
        module = importlib.import_module(
            f"datastore.infrastructure.engines.{engine}"
        )
    except ImportError as e:
        raise NotImplementedError(
            f"engine package not available: {engine!r}"
        ) from e

    backend_cls = getattr(module, "Backend", None)
    if backend_cls is None:
        raise NotImplementedError(
            f"engine {engine!r} has no `Backend` export — engine packages "
        )
    backend = backend_cls(context=context, config=config, mode=mode)
    backend.initialize()
    return backend


def warmup_engines(config) -> None:
    """Build + initialise rw and ro engine instances. Called from the
    FastAPI lifespan so credential errors surface at startup."""
    engine = config.DATASTORE_ENGINE
    for mode in ("rw", "ro"):
        _INSTANCES[(engine, mode)] = _build_engine(engine, mode, config=config)


def reset_engine_cache() -> None:
    """Drop cached instances. Used by lifespan teardown + test fixtures."""
    _INSTANCES.clear()


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
    key = (engine, mode)
    if key not in _INSTANCES:
        _INSTANCES[key] = _build_engine(
            engine, mode, config=context.config, context=context
        )
    return _INSTANCES[key]
