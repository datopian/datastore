from __future__ import annotations

from starlette.types import ASGIApp, Receive, Scope, Send


class BodySizeLimitMiddleware:
    """Reject requests whose `Content-Length` exceeds `max_bytes` with 413.

    Streaming bodies that omit `Content-Length` are not inspected here; cap
    those at the ASGI server (uvicorn `--limit-max-request-size`) or behind
    an ingress layer.
    """

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
