from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

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
        title=config.APP_MESSAGE,
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
    return app


app = create_app()
