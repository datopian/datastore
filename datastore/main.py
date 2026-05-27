from __future__ import annotations

import importlib.metadata
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware

from datastore.api.error_handlers import register_exception_handlers
from datastore.api.middleware import BodySizeLimitMiddleware
from datastore.api.responses import ORJSONResponse
from datastore.api.routes import api_router
from datastore.auth.registry import get_auth_provider
from datastore.core.config import get_config
from datastore.infrastructure.cache import InMemoryCache, RedisCache
from datastore.infrastructure.ckan_client import CKANClient
from datastore.infrastructure.engines.registry import (
    reset_engine_cache,
    warmup_engines,
)

log = logging.getLogger("uvicorn.error")

API_DESCRIPTION = """\
A **CKAN-compatible datastore API** — tabular CRUD + search over a pluggable
storage backend (BigQuery today; DuckLake planned).

### Response envelope
Every `/api/3/action/*` response uses the CKAN envelope:

```json
{ "help": "<request URL>", "success": true, "result": { ... } }
```

On failure `success` is `false` and `error` carries a `__type` label
(`Validation Error` · `Authorization Error` · `Not Found Error` ·
`Conflict Error` · `Internal Error`) plus a human `message`.

### Authentication
Send your token in the **`Authorization`** header — click **Authorize** above
to set it once for every call. The active provider is chosen by `AUTH_TYPE`
(`ckan` / `jwt` / `anonymous`); under `anonymous` no header is required.

### Search & streaming
`datastore_search` and `datastore_search_sql` **stream** their response
(peak memory ≈ one row, regardless of result size) and support
`records_format` = `objects` · `lists` · `csv` · `tsv`. Page through results
with the `_links.next` URL returned in `result`.
"""

OPENAPI_TAGS = [
    {
        "name": "datastore",
        "description": (
            "CKAN `datastore_*` actions — create, upsert, delete, search, "
            "search_sql, and info."
        ),
    },
    {
        "name": "health",
        "description": (
            "Liveness (`/health`) and readiness (`/ready`) probes for "
            "orchestration."
        ),
    },
    {
        "name": "dump",
        "description": "Bulk download of an entire resource (CSV / JSON / Parquet).",
    },
]


def _api_version() -> str:
    """Installed package version, so `/docs` tracks releases automatically."""
    try:
        return importlib.metadata.version("datastore")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"


def _strip_default_422(app: FastAPI) -> None:
    """Drop FastAPI's auto-generated 422 from the schema.

    `RequestValidationError` is remapped to a 400 CKAN error envelope (see
    `error_handlers`), so a documented 422 never actually occurs — the real
    4xx shapes are declared via `ERROR_RESPONSES`.
    """
    default_openapi = app.openapi

    def openapi() -> dict[str, Any]:
        schema = default_openapi()
        for path_item in schema.get("paths", {}).values():
            for operation in path_item.values():
                if isinstance(operation, dict):
                    operation.get("responses", {}).pop("422", None)
        components = schema.get("components", {}).get("schemas", {})
        components.pop("HTTPValidationError", None)
        components.pop("ValidationError", None)
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Per-process startup/shutdown.
    Resources are entered into an `AsyncExitStack`
    """
    config = get_config()
    async with AsyncExitStack() as stack:
        http = await stack.enter_async_context(
            httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS)
        )
        app.state.http = http
        ckan: CKANClient | None = (
            CKANClient(base_url=config.CKAN_URL, http=http)
            if config.AUTH_TYPE == "ckan"
            else None
        )
        app.state.ckan = ckan

        cache = RedisCache(config.REDIS_URL) if config.REDIS_URL else InMemoryCache()
        if hasattr(cache, "close"):
            stack.push_async_callback(cache.close)
        app.state.cache = cache
        
        app.state.auth_provider = get_auth_provider(
            config, ckan=ckan, cache=cache, cache_ttl=config.AUTH_CACHE_TTL,
        )

        # Build + initialise rw/ro engines once; surface credential
        # errors at startup, not on the first request.
        warmup_engines(config)
        stack.callback(reset_engine_cache)

        log.info(
            "datastore ready: Engine=%r Auth=%r Cache=%s",
            config.DATASTORE_ENGINE,
            config.AUTH_TYPE,
            "redis" if config.REDIS_URL else "memory",
        )

        yield


def create_app() -> FastAPI:
    config = get_config()
    app = FastAPI(
        title="Datastore API",
        version=_api_version(),
        summary=(
            "CKAN-compatible tabular CRUD + search over a pluggable storage "
            "backend."
        ),
        description=API_DESCRIPTION,
        openapi_tags=OPENAPI_TAGS,
        contact={"name": "Datopian", "url": "https://github.com/datopian/datastore"},
        lifespan=lifespan,
        default_response_class=ORJSONResponse,
    )

    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(
        BodySizeLimitMiddleware,
        max_bytes=config.MAX_REQUEST_BODY_MB * 1024 * 1024,
    )

    register_exception_handlers(app)
    app.include_router(api_router)
    _strip_default_422(app)
    return app


app = create_app()
