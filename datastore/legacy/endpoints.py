"""Legacy CKAN-compatible HTTP endpoints.

DEPRECATED — see `datastore/legacy/__init__.py` for the removal plan.

The one FastAPI-aware module in the legacy package: everything that
touches `Request`, `Response`, `StreamingResponse`, routing, or
`Depends` lives here. The streaming writers and the request schema
subclass are in sibling modules (`streaming.py`, `schemas.py`) so
future maintainers don't accidentally couple business logic to the
legacy wrapper.

Mounted at `/api/3/action/` and `/datastore/dump/` by
`datastore/api/routes.py`. Hidden from the v2 OpenAPI document via
`include_in_schema=False` so Swagger users see only v2 — the legacy
URLs keep working for scripts in the wild, but new integrators are
steered to the current contract.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Annotated, Any

from fastapi import APIRouter, Body, Query
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from datastore.api.context import Context
from datastore.api.error_handlers import _format_loc, _group_errors
from datastore.api.responses import ERROR_RESPONSES, ORJSONResponse, _success_response
from datastore.core.exceptions import APIError, ValidationError
from datastore.core.helper import content_disposition, dump_filename_base
from datastore.infrastructure.engines import get_datastore_engine
from datastore.infrastructure.engines.registry import get_allowed_sql_functions
from datastore.legacy.constants import (
    LEGACY_DUMP_EXTENSIONS,
    LEGACY_DUMP_MEDIA_TYPES,
    LEGACY_FULL_TEXT_FIELD,
    LegacyDumpFormat,
)
from datastore.legacy.schemas import DatastoreLegacySearchSQLRequest
from datastore.legacy.streaming import (
    build_legacy_pagination_links,
    dump_csv,
    dump_json,
    dump_tsv,
    dump_xml,
    legacy_info_result,
    stream_legacy_search,
    stream_legacy_search_sql,
)
from datastore.schemas.request import DatastoreInfoRequest, DatastoreSearchRequest
from datastore.schemas.validators import (
    ResourceId,
    frictionless_schema_to_fields,
    to_csv_list,
    to_json_object,
    to_str_or_json_object,
)
from datastore.services.read import enrich_schema

# Signed-int32 max — well under BigQuery's LIMIT ceiling, lets a legacy
# dump without an explicit `?limit=` drain the whole table while still
# passing a concrete int to `engine.search` (which takes `limit: int`,
# not `int | None`). Callers wanting a smaller cap pass `?limit=N`.
_LEGACY_DUMP_UNBOUNDED = 2**31 - 1


_log = logging.getLogger(__name__)


def _legacy_help_url(request: Request) -> str:
    """CKAN legacy help URL for a legacy action request."""
    action = request.url.path.rsplit("/", 1)[-1]
    return f"{request.url.scheme}://{request.url.netloc}/api/3/action/help_show?name={action}"


# HTTP status code the legacy surface should emit for each logical error
# type. These match what the legacy CKAN API returns on the same shapes
# (verified end-to-end):
#
#     - unknown resource via datastore_search       → 404
#     - unknown resource via datastore_search_sql   → 409
#     - validation error on action args / SQL       → 409
#     - validation on datastore_search bad body     → 409
#
# so a client script doing
#
#     if 200 <= status < 300:  ...   # success
#     elif status == 404:      ...   # resource gone
#     elif status == 409:      ...   # validation / query error
#
# behaves identically against our service and the legacy CKAN. Previously
# we masked everything as 200 (classic upstream CKAN parity), which broke
# clients that mirror the legacy CKAN's status-code taxonomy.
_LEGACY_STATUS_BY_TYPE: dict[str, int] = {
    "Validation Error": 409,
    "Conflict Error": 409,
    "Not Found Error": 404,
    "Authorization Error": 403,
    "Internal Error": 500,
}


class _CKANCompatRoute(APIRoute):
    """FastAPI route that mimics the legacy CKAN API's action-API error
    status codes exactly.

    Historical note: upstream CKAN's action API returns **HTTP 200** with
    `success:false` for every logical error. The legacy CKAN API we're
    replacing sits behind a gateway that promotes those back to real HTTP
    codes — 409 for Validation / Conflict, 404 for Not Found, 403 for
    Auth. We match the legacy shape rather than the classic CKAN one, so
    scripts tested against the legacy API behave identically against us.

    Body shape stays the one real CKAN emits — `{help, success:false,
    error: {<fields>, __type}}` with per-field details spread directly
    on `error` (not nested under `error.fields`). Only the HTTP status
    moves.

    Applied via `APIRouter(route_class=_CKANCompatRoute)` on the legacy
    action router only. The dump router keeps standard HTTP semantics
    because it serves raw files, not the action envelope.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original = super().get_route_handler()

        async def ckan_handler(request: Request) -> Response:
            is_sql_path = request.url.path.endswith("/datastore_search_sql")
            try:
                return await original(request)
            except APIError as exc:
                if exc.status_code >= 500:
                    # Server fault — let the global 5xx handler log it
                    # and emit a 5xx so monitoring stays honest. CKAN
                    # itself does the same.
                    raise
                type_label = exc.type_label
                fields = exc.fields or _fields_from_engine_message(exc.message)
                if is_sql_path:
                    # Any error on `datastore_search_sql` is reported by
                    # CKAN under `error.query` as a 409 Validation Error.
                    # Covers unknown table (which the engine raises as
                    # NotFoundError), engine-level DB errors, and the
                    # schema-level `sql` field key alike.
                    type_label = "Validation Error"
                    fields = {"query": [exc.message]}
                status = _LEGACY_STATUS_BY_TYPE.get(type_label, 409)
                _log.debug(
                    "legacy APIError %s %s -> %s (HTTP %d)",
                    request.method,
                    request.url.path,
                    type_label,
                    status,
                )
                return _legacy_error_envelope(
                    request,
                    type_label=type_label,
                    message=exc.message,
                    fields=fields,
                    status_code=status,
                )
            except RequestValidationError as exc:
                errors = list(exc.errors())
                fields = _group_errors(errors)
                first = errors[0] if errors else {"msg": "invalid request"}
                message = f"{_format_loc(tuple(first.get('loc', ())))}: {first.get('msg')}"
                if is_sql_path:
                    # CKAN keys SQL-content errors (bad SQL, non-SELECT,
                    # multi-statement, …) under `error.query`, but keeps
                    # a plain missing-value error under `error.sql`.
                    # Mirror that: rename `sql` → `query` only for
                    # errors that aren't the "field required" kind.
                    sql_errors_are_content = any(
                        e.get("type") != "missing"
                        for e in errors
                        if e.get("loc") and e["loc"][-1] == "sql"
                    )
                    if sql_errors_are_content and "sql" in fields:
                        fields = {
                            ("query" if k == "sql" else k): v for k, v in fields.items()
                        }
                _log.debug(
                    "legacy RequestValidationError %s %s -> HTTP 409",
                    request.method,
                    request.url.path,
                )
                return _legacy_error_envelope(
                    request,
                    type_label="Validation Error",
                    message=message,
                    fields=fields,
                    status_code=_LEGACY_STATUS_BY_TYPE["Validation Error"],
                )

        return ckan_handler


_ENGINE_FIELD_KEYED_PARAMS = ("filters", "q", "fields", "sort")


def _with_full_text_field(
    fields: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return a copy of `fields` with `_full_text` spliced in right
    after `_id` (or at the head when `_id` isn't present), matching
    the ordering CKAN emits for `SELECT *` responses.
    """
    pos = 0
    for i, f in enumerate(fields):
        if f.get("id") == "_id":
            pos = i + 1
            break
    return fields[:pos] + [LEGACY_FULL_TEXT_FIELD] + fields[pos:]


def _fields_from_engine_message(message: str) -> dict[str, list[str]] | None:
    """Parse `"<param> references unknown column 'X'"` into a legacy
    field-keyed detail map so the error envelope matches CKAN:
    `{"<param>": ["invalid value \"X\""]}`. Returns None otherwise.
    """
    for param in _ENGINE_FIELD_KEYED_PARAMS:
        prefix = f"{param} references unknown column "
        if message.startswith(prefix):
            col = message[len(prefix):].strip().strip("'\"")
            return {param: [f'invalid value "{col}"']}
    return None


def _legacy_error_envelope(
    request: Request,
    *,
    type_label: str,
    message: str,
    fields: dict[str, list[str]] | None = None,
    status_code: int = 409,
) -> ORJSONResponse:
    """CKAN-shaped error envelope at the HTTP status the legacy CKAN API
    emits.

    Matches the exact body shape the legacy CKAN API returns, verified
    end-to-end:

        {
          "help": "...",
          "success": false,
          "error": {
            "<field>": ["<msg>", ...],   # per-field keys at ERROR ROOT
            "__type": "..."
          }
        }

    Specifics preserved for 1:1 parity:

      - No top-level `message` key. CKAN encodes the human-readable
        reason inside the per-field arrays (or, when there's no field
        context, under a generic `message` key — but only when fields
        are absent).
      - Per-field details are spread **directly on the error dict**
        (not nested under `fields`). A caller doing
        `error["sql"]` or `error["query"]` must find them there.
      - `__type` is the last key by convention; `orjson` preserves
        insertion order, so we write it last.

    `help` echoes the request URL — same thing the success envelope
    does, so a legacy caller can't tell the two envelopes apart by
    looking at the help key.

    `status_code` defaults to 409 (what prod emits for Validation /
    Conflict). `_CKANCompatRoute` picks the right code per error type;
    callers that build an envelope directly can override as needed.
    """
    error: dict[str, Any] = {}
    if fields:
        # Per-field details go at the error-dict root (CKAN shape).
        # Field keys never collide with `__type` because we never
        # declare a field literally named `__type`; a defensive guard
        # would be strict-but-wrong (CKAN itself doesn't do it).
        error.update(fields)
    else:
        # No field context — surface the human message under `message`.
        # Dropping it entirely would make the error unparseable for
        # callers that only look at the error dict.
        error["message"] = message
    error["__type"] = type_label

    return ORJSONResponse(
        {"help": str(request.url), "success": False, "error": error},
        status_code=status_code,
    )


# `include_in_schema=False` keeps the legacy surface out of the v2
# OpenAPI document. Legacy exists purely for backwards compatibility —
# surfacing it in Swagger would advertise URLs we actively want callers
# to migrate off. Scripts targeting the legacy paths keep working; new
# integrators see only v2.
#
# `route_class=_CKANCompatRoute` makes the actions return HTTP 200 on
# logical errors (CKAN semantics). The dump router stays on the default
# route class — it serves raw files, not the JSON envelope, and HTTP
# error codes are the right thing for a browser hitting a bad download
# URL.
router = APIRouter(
    tags=["Datastore (Legacy)"],
    responses=ERROR_RESPONSES,
    include_in_schema=False,
    route_class=_CKANCompatRoute,
)
dump_router = APIRouter(
    tags=["Datastore (Legacy)"],
    responses=ERROR_RESPONSES,
    include_in_schema=False,
)


# --- datastore_search ------------------------------------------------------


@router.get("/datastore_search", summary="Legacy CKAN datastore_search (GET)")
async def datastore_search_get(
    request: Request,
    context: Context,
    params: Annotated[DatastoreSearchRequest, Query()],
) -> StreamingResponse:
    return await _legacy_search_impl(request, context, params)


@router.post("/datastore_search", summary="Legacy CKAN datastore_search (POST)")
async def datastore_search_post(
    request: Request,
    context: Context,
    payload: Annotated[DatastoreSearchRequest, Body()],
) -> StreamingResponse:
    return await _legacy_search_impl(request, context, payload)


async def _legacy_search_impl(
    request: Request,
    context: Context,
    params: DatastoreSearchRequest,
) -> StreamingResponse:
    """Shared body: authorize, run the engine, stream the legacy envelope."""
    auth_result = await context.authorize(resource_id=params.resource_id, permission="read")

    engine = get_datastore_engine(context, mode="ro")
    result = await asyncio.to_thread(
        engine.search,
        resource_id=params.resource_id,
        filters=to_json_object(params.filters),
        q=to_str_or_json_object(params.q),
        fields=to_csv_list(params.fields),
        distinct=params.distinct,
        plain=params.plain,
        language=params.language,
        limit=params.limit,
        offset=params.offset,
        sort=params.sort,
        include_total=params.include_total,
    )

    schema = enrich_schema(result.schema, (auth_result or {}).get("resource"))
    fields, _ = frictionless_schema_to_fields(schema)
    body_iter = stream_legacy_search(
        help_url=_legacy_help_url(request),
        resource_id=params.resource_id,
        fields=fields,
        records=result.records,
        limit=params.limit,
        offset=params.offset,
        total=result.total,
        include_total=params.include_total,
        records_format=params.records_format,
        links=build_legacy_pagination_links(
            str(request.url),
            limit=params.limit,
            offset=params.offset,
        ),
    )
    return StreamingResponse(body_iter, media_type="application/json")


# --- datastore_search_sql --------------------------------------------------


@router.get("/datastore_search_sql", summary="Legacy CKAN datastore_search_sql (GET)")
async def datastore_search_sql_get(
    request: Request,
    context: Context,
    params: Annotated[DatastoreLegacySearchSQLRequest, Query()],
) -> StreamingResponse:
    return await _legacy_search_sql_impl(request, context, params)


@router.post("/datastore_search_sql", summary="Legacy CKAN datastore_search_sql (POST)")
async def datastore_search_sql_post(
    request: Request,
    context: Context,
    payload: Annotated[DatastoreLegacySearchSQLRequest, Body()],
) -> StreamingResponse:
    return await _legacy_search_sql_impl(request, context, payload)


async def _legacy_search_sql_impl(
    request: Request,
    context: Context,
    params: DatastoreLegacySearchSQLRequest,
) -> StreamingResponse:
    """Authorize every referenced table, enforce the function allow-list,
    run the SQL and stream the legacy SQL envelope.

    LIMIT is optional (legacy CKAN behaviour). **No row cap applies**:
    `datastore_search_sql` is where CKAN historically let callers pull
    arbitrary query results, so a legacy script doing
    `SELECT * FROM "<rid>"` on a 10M-row table streams the whole set.
    `SEARCH_RESULT_ROWS_MAX` still gates `datastore_search`; it does
    not apply here. The engine streams row-by-row so peak memory is
    bounded regardless of result size.
    """
    # Collect each referenced resource's authorize payload — under
    # AUTH_TYPE=ckan it carries the resource record (with its synced
    # schema), merged into the output below so computed-but-same-named
    # columns inherit CKAN-side data-dictionary edits.
    resources: list[dict[str, Any]] = []
    for resource_id in params.resource_ids:
        auth_result = await context.authorize(resource_id=resource_id, permission="read")
        resource = (auth_result or {}).get("resource")
        if isinstance(resource, dict):
            resources.append(resource)

    allowed = get_allowed_sql_functions(
        context.config.DATASTORE_ENGINE,
        override_path=context.config.SQL_FUNCTIONS_ALLOW_FILE,
    )
    disallowed = sorted(set(params.function_names) - allowed)
    if disallowed:
        raise ValidationError(
            f"sql uses disallowed function(s): {', '.join(disallowed)}",
            fields={"sql": [f"disallowed: {', '.join(disallowed)}"]},
        )

    # No cap: when the caller omits LIMIT we pass int32-max so the
    # engine's `limit: int` signature stays non-optional, matching the
    # dump endpoint's approach. The engine streams; it does not
    # materialise the result set.
    engine = get_datastore_engine(context, mode="ro")
    effective_limit = params.limit if params.limit is not None else _LEGACY_DUMP_UNBOUNDED
    result = await asyncio.to_thread(
        engine.search_sql,
        sql=params.sql,
        limit=effective_limit,
    )
    schema = enrich_schema(result.schema, resources)
    fields, _ = frictionless_schema_to_fields(schema)
    if params.has_select_star:
        fields = _with_full_text_field(fields)
    body_iter = stream_legacy_search_sql(
        help_url=_legacy_help_url(request),
        sql=params.sql,
        fields=fields,
        records=result.records,
    )
    return StreamingResponse(body_iter, media_type="application/json")


# --- datastore_info --------------------------------------------------------


@router.get("/datastore_info", summary="Legacy CKAN datastore_info (GET)")
async def datastore_info_get(
    request: Request,
    context: Context,
    params: Annotated[DatastoreInfoRequest, Query()],
):
    return await _legacy_info_impl(request, context, params)


@router.post("/datastore_info", summary="Legacy CKAN datastore_info (POST)")
async def datastore_info_post(
    request: Request,
    context: Context,
    payload: Annotated[DatastoreInfoRequest, Body()],
):
    return await _legacy_info_impl(request, context, payload)


async def _legacy_info_impl(
    request: Request,
    context: Context,
    params: DatastoreInfoRequest,
):
    """Legacy info: `result.{resource_id, fields, meta}`, fields filtered."""
    # `_require_resource_id_or_id` on `DatastoreInfoRequest` normalises
    # `id` → `resource_id` and rejects the all-None case, so by this point
    # `resource_id` is always set. Narrow the type for mypy.
    assert params.resource_id is not None
    resource_id: str = params.resource_id
    data_dict = await context.authorize(resource_id=resource_id, permission="read")
    engine = get_datastore_engine(context, mode="ro")
    info = await asyncio.to_thread(engine.info, resource_id=resource_id)

    schema = info.schema
    if context.config.AUTH_TYPE == "ckan":
        ckan_schema = (data_dict.get("resource") or {}).get("schema")
        if ckan_schema:
            schema = ckan_schema
    fields, _ = frictionless_schema_to_fields(schema)

    return _success_response(
        request,
        legacy_info_result(
            resource_id=resource_id,
            fields=fields,
            meta=info.meta,
        ),
    )


# --- dump ------------------------------------------------------------------


_DUMP_WRITERS = {
    "csv": dump_csv,
    "tsv": dump_tsv,
    "json": dump_json,
    "xml": dump_xml,
}


@dump_router.get(
    "/{resource_id}",
    summary="Legacy CKAN datastore dump",
)
async def legacy_dump(
    request: Request,
    context: Context,
    resource_id: ResourceId,
    fmt: Annotated[LegacyDumpFormat, Query(alias="format")] = "csv",
    bom: bool = False,
    # datastore_search filter / projection params — all optional.
    filters: str | None = None,
    q: str | None = None,
    distinct: bool = False,
    plain: bool = True,
    language: str = "english",
    limit: int | None = None,
    offset: int = 0,
    fields: str | None = None,
    sort: str | None = None,
) -> StreamingResponse:
    """Export a resource as a file. Mirrors CKAN's `/datastore/dump/{id}`.

    Formats: `csv` (default) · `tsv` · `json` (JSON array) · `xml`.

    All downloads stream API-side rather than redirecting to GCS — the
    engine's native GCS export doesn't do row-level filtering (hidden
    `_id` / `_updated_at` columns, user `filters` / `q`) and only
    produces csv/ndjson/parquet. One streaming path across formats keeps
    the response behaviour identical for every caller.

    All of `datastore_search`'s filter / projection params are accepted
    (`filters`, `q`, `distinct`, `plain`, `language`, `limit`, `offset`,
    `fields`, `sort`). Omitting `limit` streams the whole table.

    `bom=true` prepends the UTF-8 byte-order mark for Excel-on-Windows
    compatibility.
    """
    await context.authorize(resource_id=resource_id, permission="read")

    # Validate filter payloads up front — same parsers v2 search uses.
    parsed_filters = to_json_object(filters)
    parsed_q = to_str_or_json_object(q)
    parsed_fields = to_csv_list(fields)

    if offset < 0:
        raise ValidationError("offset must be >= 0", fields={"offset": ["must be >= 0"]})
    if limit is not None and limit < 0:
        raise ValidationError("limit must be >= 0", fields={"limit": ["must be >= 0"]})

    effective_limit = limit if limit is not None else _LEGACY_DUMP_UNBOUNDED

    engine = get_datastore_engine(context, mode="ro")
    result = await asyncio.to_thread(
        engine.search,
        resource_id=resource_id,
        filters=parsed_filters,
        q=parsed_q,
        fields=parsed_fields,
        distinct=distinct,
        plain=plain,
        language=language,
        limit=effective_limit,
        offset=offset,
        sort=sort,
        # Row counts are expensive on a full-table dump and the output
        # format carries no envelope to put them in.
        include_total=False,
    )

    legacy_fields, _ = frictionless_schema_to_fields(result.schema)
    writer = _DUMP_WRITERS[fmt]
    body_iter = writer(fields=legacy_fields, records=result.records, bom=bom)

    filename_base = dump_filename_base(None, LEGACY_DUMP_EXTENSIONS[fmt]) or resource_id
    filename = f"{filename_base}.{LEGACY_DUMP_EXTENSIONS[fmt]}"
    return StreamingResponse(
        body_iter,
        media_type=LEGACY_DUMP_MEDIA_TYPES[fmt],
        headers={"Content-Disposition": content_disposition(filename)},
    )
