from __future__ import annotations

import importlib.metadata
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from starlette.requests import Request
from starlette.responses import HTMLResponse

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


OPENAPI_TAGS = [
     {
        "name": "Health",
        "description": (
            "Liveness (`/health`) and readiness (`/ready`) probes for "
            "orchestration."
        ),
    },
    {
        "name": "Datastore",
        "description": (
            "A `datastore` API endpoint - create, upsert, delete, search, "
            "search_sql, and info."
        ),
    },

    {
        "name": "Datastore Download",
        "description": "Bulk download of an entire resource (CSV / JSON / Parquet).",
    },
]


def _api_version() -> str:
    """Installed package version, so `/docs` tracks releases automatically."""
    try:
        return importlib.metadata.version("datastore")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"


# Swagger UI's Authorize modal caps its token input at ~230px — too short
# to see a pasted JWT or API key. Swagger UI exposes no option for this,
# so the docs page is served with a CSS override instead of FastAPI's
# stock HTML (see `_register_swagger_docs`).
_SWAGGER_UI_CSS_OVERRIDES = """
.swagger-ui .dialog-ux .modal-ux { max-width: 900px; }
.swagger-ui .auth-container input[type=text],
.swagger-ui .auth-container input[type=password] { width: 100%; }
"""


def _register_swagger_docs(app: FastAPI, docs_url: str) -> None:
    """Serve Swagger UI with `_SWAGGER_UI_CSS_OVERRIDES` injected.

    Mirrors the stock FastAPI docs route (same title convention,
    root_path-aware spec URL); the only delta is the `<style>` block
    added to `<head>`.
    """

    @app.get(docs_url, include_in_schema=False)
    async def swagger_ui_html(request: Request) -> HTMLResponse:
        root_path = request.scope.get("root_path", "").rstrip("/")
        page = get_swagger_ui_html(
            openapi_url=root_path + str(app.openapi_url),
            title=f"{app.title} - Swagger UI",
        )
        html = page.body.decode("utf-8").replace(
            "</head>", f"<style>{_SWAGGER_UI_CSS_OVERRIDES}</style></head>"
        )
        return HTMLResponse(html)


# Swagger "Authorize" description per built-in AUTH_TYPE. A provider not
# listed here (a third-party drop-in under `datastore/auth/<name>/`) keeps
# the generic description declared on the scheme in `api/context.py`.
_AUTH_SCHEME_DESCRIPTIONS = {
    "ckan": "A CKAN API key.",
    "jwt": "A signed JWT. Accepts the raw token or `Bearer <token>`.",
}


def _tailor_auth_scheme(app: FastAPI, auth_type: str) -> None:
    """Shape the OpenAPI security scheme to the active AUTH_TYPE.

    The `Authorization` scheme is declared once in `api/context.py`, at
    import time — before config is read — so its description can't know
    which provider runs. The app factory does, and rewrites the schema
    here: provider-specific wording for `ckan` / `jwt`; under `anonymous`
    the scheme and every operation's `security` entry are removed, so the
    docs render no Authorize button at all.
    """
    default_openapi = app.openapi

    def openapi() -> dict[str, Any]:
        schema = default_openapi()
        schemes = schema.get("components", {}).get("securitySchemes", {})
        if auth_type == "anonymous":
            schemes.pop("Authorization", None)
            if not schemes:
                schema.get("components", {}).pop("securitySchemes", None)
            for path_item in schema.get("paths", {}).values():
                for operation in path_item.values():
                    if not isinstance(operation, dict):
                        continue
                    security = [
                        requirement
                        for requirement in operation.get("security", [])
                        if "Authorization" not in requirement
                    ]
                    if security:
                        operation["security"] = security
                    else:
                        operation.pop("security", None)
        elif auth_type in _AUTH_SCHEME_DESCRIPTIONS and "Authorization" in schemes:
            description = _AUTH_SCHEME_DESCRIPTIONS[auth_type]
            schemes["Authorization"]["description"] = description
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


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
        summary="A datastore API for managing tabular data resources.",
        description=(
            "📮 **Postman collection** — import the "
            "[Datastore API collection]"
            "(https://raw.githubusercontent.com/datopian/datastore/main/"
            "postman/collection.json) via Postman's **Import → Link** to "
            "exercise every endpoint with worked examples."
        ),
        openapi_tags=OPENAPI_TAGS,
        contact={"name": "Datopian", "url": "https://www.datopian.com/"},
        # Mount the interactive docs (and the spec they fetch) under the
        # service's path prefix so this API doesn't compete with an upstream
        # proxy or sibling service for the bare `/docs`. Swagger UI is
        # served by `_register_swagger_docs` (CSS overrides), not FastAPI's
        # stock route.
        docs_url=None,
        redoc_url="/datastore/api/redoc",
        openapi_url="/datastore/api/openapi.json",
        lifespan=lifespan,
        default_response_class=ORJSONResponse,
    )

    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(
        BodySizeLimitMiddleware,
        max_bytes=config.MAX_REQUEST_BODY_MB * 1024 * 1024,
    )
    # Added last = outermost, so 4xx/5xx envelopes carry CORS headers too.
    # `CORS_ORIGINS=*` allows every origin, a comma-separated list allows
    # only those domains, empty skips the middleware entirely.
    if config.cors_origin_list:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=config.cors_origin_list,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    register_exception_handlers(app)
    app.include_router(api_router)
    _register_swagger_docs(app, docs_url="/datastore/api/docs")
    _strip_default_422(app)
    _tailor_auth_scheme(app, config.AUTH_TYPE)
    return app


app = create_app()
