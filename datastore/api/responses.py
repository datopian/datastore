from __future__ import annotations

from typing import Any

import orjson
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse


def _orjson_default(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, default=_orjson_default)


def _help(request: Request) -> str:
    return str(request.url)


def ckan_success(
    request: Request,
    result: BaseModel | dict[str, Any],
    *,
    status_code: int = 200,
) -> ORJSONResponse:
    # `result` may be a Pydantic model or a plain dict; orjson's default
    # handler in `_orjson_default` dumps Pydantic models via `model_dump()`.
    return ORJSONResponse(
        {"help": _help(request), "success": True, "result": result},
        status_code=status_code,
    )


def ckan_error(
    request: Request,
    *,
    status_code: int,
    type_label: str,
    message: str,
    fields: dict[str, list[str]] | None = None,
) -> ORJSONResponse:
    error: dict[str, Any] = {"__type": type_label, "message": message}
    if fields:
        error["fields"] = fields
    return ORJSONResponse(
        {"help": _help(request), "success": False, "error": error},
        status_code=status_code,
    )
