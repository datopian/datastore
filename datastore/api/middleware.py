from __future__ import annotations

import orjson
from starlette.types import ASGIApp, Receive, Scope, Send


class BodySizeLimitMiddleware:
    """Reject requests whose `Content-Length` exceeds `max_bytes` with 413.


    Streaming bodies that omit `Content-Length` are not inspected here;
    cap those at the ASGI server (uvicorn `--limit-max-request-size`)
    or behind an ingress layer.
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
                        await self._reject(scope, send, size)
                        return
                    break
        await self.app(scope, receive, send)

    async def _reject(self, scope: Scope, send: Send, size: int) -> None:
        limit_mb = self.max_bytes / (1024 * 1024)
        size_mb = size / (1024 * 1024)
        path = scope.get("path", "")
        body = orjson.dumps(
            {
                "help": path,
                "success": False,
                "error": {
                    "__type": "Payload Too Large",
                    "message": (
                        f"Request body is {size_mb:.2f} MB; limit is "
                        f"{limit_mb:.0f} MB. Single write "
                        "at 10 MB — split the records into smaller "
                        "batches and retry."
                    ),
                },
            }
        )
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
