from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request

from datastore.api.responses import ORJSONResponse, _error_response
from datastore.core.exceptions import HTTP_STATUS_TO_TYPE_LABEL, APIError

log = logging.getLogger(__name__)


def _format_loc(loc: tuple[str | int, ...]) -> str:
    """Convert Pydantic loc tuple to a dotted path: ('body', 'fields', 0, 'id') → 'fields[0].id'."""
    parts: list[str] = []
    for item in loc:
        if isinstance(item, int):
            if parts:
                parts[-1] = f"{parts[-1]}[{item}]"
            else:
                parts.append(f"[{item}]")
        elif item in ("body", "query", "path", "header", "cookie"):
            continue
        else:
            parts.append(str(item))
    return ".".join(parts) or "(root)"


def _group_errors(errors: list[dict[str, Any]]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for err in errors:
        path = _format_loc(tuple(err.get("loc", ())))
        msg = err.get("msg") or "invalid value"
        grouped[path].append(msg)
    return dict(grouped)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(APIError)
    async def _api_error(request: Request, exc: APIError) -> ORJSONResponse:
        log.debug(
            "APIError: %s -> %d (%s) at %s %s",
            type(exc).__name__, exc.status_code, exc.type_label,
            request.method, request.url.path,
        )
        return _error_response(
            request,
            status_code=exc.status_code,
            type_label=exc.type_label,
            message=exc.message,
            fields=exc.fields,
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> ORJSONResponse:
        errors = list(exc.errors())
        fields = _group_errors(errors)
        first = errors[0] if errors else {"msg": "invalid request"}
        message = f"{_format_loc(tuple(first.get('loc', ())))}: {first.get('msg')}"
        log.debug(
            "RequestValidationError at %s %s: %d field(s) failed; first=%s",
            request.method, request.url.path, len(fields), message,
        )
        return _error_response(
            request,
            status_code=400,
            type_label="Validation Error",
            message=message,
            fields=fields,
        )

    @app.exception_handler(HTTPException)
    async def _http(request: Request, exc: HTTPException) -> ORJSONResponse:
        label = HTTP_STATUS_TO_TYPE_LABEL.get(exc.status_code, "Internal Error")
        message = exc.detail if isinstance(exc.detail, str) else "request failed"
        log.debug(
            "HTTPException %d (%s) at %s %s: %s",
            exc.status_code, label, request.method, request.url.path, message,
        )
        return _error_response(
            request,
            status_code=exc.status_code,
            type_label=label,
            message=message,
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> ORJSONResponse:
        log.exception(
            "unhandled exception at %s %s", request.method, request.url.path,
            exc_info=exc,
        )
        return _error_response(
            request,
            status_code=500,
            type_label="Internal Error",
            message="internal_error",
        )
