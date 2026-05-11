from __future__ import annotations

from typing import Any

import orjson
from fastapi import Request, Response
from fastapi.responses import StreamingResponse


def json_response(payload: dict[str, Any]) -> Response:
    return Response(content=orjson.dumps(payload), media_type="application/json")


def streaming_json(payload: dict[str, Any]) -> StreamingResponse:
    return StreamingResponse(iter([orjson.dumps(payload)]), media_type="application/json")


def success_envelope(request: Request, result: dict[str, Any]) -> dict[str, Any]:
    return {"help": str(request.url), "success": True, "result": result}
