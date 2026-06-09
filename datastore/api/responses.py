from __future__ import annotations

import base64
from decimal import Decimal
from typing import Any

import orjson
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse

from datastore.schemas.responses import ErrorEnvelope

# Shared OpenAPI doc for the CKAN error envelope, attached at router level
# so every action documents the real 4xx / 5xx shape instead of FastAPI's
# default 422. The taxonomy matches `core.exceptions.APIError`.
ERROR_RESPONSES: dict[int, dict[str, Any]] = {
    400: {"model": ErrorEnvelope, "description": "Validation Error"},
    403: {"model": ErrorEnvelope, "description": "Authorization Error"},
    404: {"model": ErrorEnvelope, "description": "Not Found Error"},
    409: {"model": ErrorEnvelope, "description": "Conflict Error"},
    500: {"model": ErrorEnvelope, "description": "Internal Error"},
}


def _orjson_default(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    # BigQuery `NUMERIC` / `BIGNUMERIC` columns come back as Decimal —
    # JSON has no native form, and orjson refuses by default. Emit as a
    # JSON number so clients can do arithmetic without parsing a string;
    # values past ~15 significant digits round to the nearest IEEE-754
    # double (full-precision callers should CAST to STRING in SQL).
    if isinstance(obj, Decimal):
        return float(obj)
    # `BYTES` columns come back as raw `bytes`; base64-encode so the
    # response stays UTF-8 and round-trippable.
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, default=_orjson_default)


def _help(request: Request) -> str:
    return str(request.url)


def _deprecation_warnings(payload: BaseModel) -> list[str]:
    """Build body-level warnings from `Field(deprecated=...)` metadata.

    For every field the caller explicitly provided (`model_fields_set`)
    whose declaration carries a `deprecated` string, emit one warning of
    the form ``"'<field>' is deprecated: <message>"``. Pulling the
    message off the model keeps the wording in one place — the field's
    own declaration — so endpoints never duplicate it.

    `model_fields_set` is used instead of reading the value: it answers
    "did the caller send this?" without invoking the field accessor,
    which would itself emit a `DeprecationWarning` we don't want at
    runtime.
    """
    out: list[str] = []
    for name in payload.model_fields_set:
        msg = type(payload).model_fields[name].deprecated
        if isinstance(msg, str) and msg:
            out.append(f"'{name}' is deprecated — {msg}.")
    return out


def _success_response(
    request: Request,
    result: BaseModel | dict[str, Any],
    *,
    status_code: int = 200,
    warnings: list[str] | None = None,
) -> ORJSONResponse:
    # `result` may be a Pydantic model or a plain dict; orjson's default
    # handler in `_orjson_default` dumps Pydantic models via `model_dump()`.
    # `warnings` is non-fatal advisory text (e.g. deprecated-input notices) —
    # surfaced at envelope level so any client reading the body sees them
    # without having to parse the result block. Omitted when empty.
    body: dict[str, Any] = {
        "help": _help(request),
        "success": True,
        "result": result,
    }
    if warnings:
        body["warnings"] = warnings
    return ORJSONResponse(body, status_code=status_code)


def _error_response(
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
