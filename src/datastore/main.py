from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

from datastore.api.errors import register_exception_handlers
from datastore.api.routes import datastore_router, router
from datastore.config import get_settings


class BodySizeLimitMiddleware:
    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            for name, value in scope["headers"]:
                if name == b"content-length":
                    try:
                        size = int(value)
                    except ValueError:
                        break
                    if size > self.max_bytes:
                        await send(
                            {
                                "type": "http.response.start",
                                "status": 413,
                                "headers": [(b"content-type", b"application/json")],
                            }
                        )
                        await send(
                            {
                                "type": "http.response.body",
                                "body": b'{"detail":"request body too large"}',
                            }
                        )
                        return
                    break
        await self.app(scope, receive, send)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.APP_MESSAGE, lifespan=lifespan)

    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(
        BodySizeLimitMiddleware,
        max_bytes=settings.MAX_REQUEST_BODY_MB * 1024 * 1024,
    )

    register_exception_handlers(app)
    app.include_router(router)
    app.include_router(datastore_router, prefix="/api/3")
    return app


app = create_app()
