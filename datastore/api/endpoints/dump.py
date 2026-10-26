"""Download endpoints: `<DUMP_PREFIX>/{resource_id}` + `<DUMP_PREFIX>/query`,
plus a resource's data dictionary and metadata at
`<DUMP_PREFIX>/{resource_id}/dictionary` and `…/metadata`.

csv / gzip / ndjson shards are composed into one GCS object, so those
always redirect — the server never touches the bytes. Parquet can't be
composed (footer + magic bytes), so a parquet export that shards is
zipped on the way out: the API fetches the parts and streams one
archive, which keeps every download a single file at one URL.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Query
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response, StreamingResponse

from datastore.api.context import Context
from datastore.api.responses import ERROR_RESPONSES
from datastore.core.constants import (
    DOCUMENT_MEDIA_TYPES,
    DUMP_EXTENSIONS,
    DUMP_MEDIA_TYPES,
    DocumentFormat,
    DumpFormat,
)
from datastore.core.exceptions import ServerError
from datastore.core.helper import content_disposition, dump_filename_base
from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.request import DatastoreDumpSQLRequest
from datastore.schemas.validators import ResourceId
from datastore.services.metadata import (
    dictionary_csv,
    dictionary_document,
    json_bytes,
    metadata_csv,
    metadata_record,
)
from datastore.services.read import dump_sql_datastore
from datastore.services.streaming import zip_archive_writer

router = APIRouter(tags=["Datastore Downloads"], responses=ERROR_RESPONSES)


# What a download actually returns, for OpenAPI. Without an explicit
# `response_class` FastAPI documents a JSON body on the 200 — wrong for
# every path here, and it would have a generated client parsing a parquet
# file as an envelope. The 302 is the usual answer; the 200 only happens
# when a parquet export shards.
_DOWNLOAD_RESPONSES: dict[int | str, dict[str, Any]] = {
    302: {
        "description": (
            "The download. Redirects to a short-lived signed GCS URL — the "
            "bytes stream from GCS, not through this API, so the transfer "
            "is resumable. Content type follows `format`: "
            + ", ".join(f"`{f}` → `{m}`" for f, m in DUMP_MEDIA_TYPES.items())
            + ". The URL carries a `Content-Disposition` naming the file."
        ),
        "headers": {
            "Location": {
                "description": "Signed GCS URL holding the exported file.",
                "schema": {"type": "string", "format": "uri"},
            },
        },
    },
    200: {
        "description": (
            "A parquet export that sharded: one streamed zip of the parts, "
            "served by this API rather than redirected. Chunked, so there "
            "is no `Content-Length` and no range support."
        ),
        "content": {"application/zip": {}},
    },
}


def download_response(
    request: Request,
    urls: list[str],
    fmt: DumpFormat,
    filename_base: str,
) -> RedirectResponse | StreamingResponse:
    """Shape the engine's signed URL(s) into a response:

      - one file   → 302 to the signed URL; bytes go GCS → client and
                     the download is resumable
      - many files → 200 streaming one zip of the parts (parquet only —
                     every other format composes to a single object)
      - none       → 500; the engine isn't configured

    The zip is the only path where the server carries the bytes, and it
    exists so a caller always gets one file from one URL. `Content-
    Length` is unknowable mid-stream, so the response is chunked: no
    progress bar, and no resuming a dropped connection.
    """
    if not urls:
        raise ServerError(
            "export produced no downloadable files (datastore engine is not configured)"
        )
    if len(urls) == 1:
        return RedirectResponse(url=urls[0], status_code=302)

    ext = DUMP_EXTENSIONS[fmt]
    members = [(f"{filename_base}_{i + 1:02d}.{ext}", url) for i, url in enumerate(urls)]
    return StreamingResponse(
        zip_archive_writer(request.app.state.http, members),
        media_type="application/zip",
        headers={
            "Content-Disposition": content_disposition(f"{filename_base}.zip"),
        },
    )


@router.get(
    "/query",
    summary="Download the result of a SQL SELECT",
    response_class=RedirectResponse,
    status_code=302,
    responses=_DOWNLOAD_RESPONSES,
)
async def dump_sql(
    request: Request,
    context: Context,
    params: Annotated[DatastoreDumpSQLRequest, Query()],
):
    """Download query result as a file, named
    `<download_filename>_filtered_<hash>` when the query reads a single
    resource that has a download name (see `dump`), else `query_<hash>`."""
    resources = [
        (await context.authorize(resource_id=resource_id, permission="read")).get("resource")
        for resource_id in params.resource_ids
    ]
    name = (
        dump_filename_base(resources[0], DUMP_EXTENSIONS[params.format])
        if len(resources) == 1
        else None
    )
    filename_prefix = f"{name}_filtered" if name else None

    urls = await dump_sql_datastore(
        context,
        {
            "sql": params.sql,
            "fmt": params.format,
            "resource_ids": params.resource_ids,
            "function_names": params.function_names,
            "filename_prefix": filename_prefix,
        },
    )
    return download_response(request, urls, params.format, filename_prefix or "query")


@router.get(
    "/{resource_id}",
    summary="Download an entire table",
    response_class=RedirectResponse,
    status_code=302,
    responses=_DOWNLOAD_RESPONSES,
)
async def dump(
    request: Request,
    context: Context,
    resource_id: ResourceId,
    fmt: Annotated[DumpFormat, Query(alias="format")] = "csv",
):
    """Download an entire resource; pick the format with `?format=`.

    The file is named after the uploaded file for `url_type: upload`
    resources, else the resource's `download_filename`; when neither is
    available, after the resource id."""
    authorized = await context.authorize(resource_id=resource_id, permission="read")
    filename_base = (
        dump_filename_base(authorized.get("resource"), DUMP_EXTENSIONS[fmt]) or resource_id
    )
    engine = get_datastore_engine(context, mode="ro")

    urls = await engine.dump(resource_id, fmt, filename_base=filename_base)
    return download_response(request, urls, fmt, filename_base)


# What a dictionary / metadata download returns, for OpenAPI: the file
# itself, served by this API (no redirect — the documents are small).
_DOCUMENT_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "description": (
            "The file, named by `Content-Disposition`. Content type follows "
            "`format`: "
            + ", ".join(f"`{f}` → `{m}`" for f, m in DOCUMENT_MEDIA_TYPES.items())
            + "."
        ),
        "content": {media: {} for media in DOCUMENT_MEDIA_TYPES.values()},
    },
}


def document_response(
    body: bytes, fmt: DocumentFormat, authorized: dict[str, Any], resource_id: str, suffix: str
) -> Response:
    """Serve an in-memory document, named like the resource's dump
    (`<name>_<suffix>.<fmt>`)."""
    base = dump_filename_base(authorized.get("resource"), fmt) or resource_id
    return Response(
        content=body,
        media_type=DOCUMENT_MEDIA_TYPES[fmt],
        headers={"Content-Disposition": content_disposition(f"{base}_{suffix}.{fmt}")},
    )


@router.get(
    "/{resource_id}/dictionary",
    summary="Download a resource's data dictionary",
    response_class=Response,
    responses=_DOCUMENT_RESPONSES,
)
async def dump_dictionary(
    context: Context,
    resource_id: ResourceId,
    fmt: Annotated[DocumentFormat, Query(alias="format")] = "csv",
):
    """Download the resource's table schema (data dictionary).

    `json` is the Frictionless Table Schema as `datastore_info` returns it.
    `csv` has one row per column and one column per field property
    (`name`, `type`, `title`, `description`, then any others); nested
    values such as `constraints` are written as JSON in their cell."""
    authorized = await context.authorize(resource_id=resource_id, permission="read")
    schema = await dictionary_document(
        context, {"resource_id": resource_id, "resource": authorized.get("resource")}
    )
    body = dictionary_csv(schema) if fmt == "csv" else json_bytes(schema)
    return document_response(body, fmt, authorized, resource_id, "data_dictionary")


@router.get(
    "/{resource_id}/metadata",
    summary="Download a resource's and its dataset's metadata",
    response_class=Response,
    responses=_DOCUMENT_RESPONSES,
)
async def dump_metadata(
    context: Context,
    resource_id: ResourceId,
    fmt: Annotated[DocumentFormat, Query(alias="format")] = "csv",
):
    """Download the resource's metadata alongside its dataset's: names,
    ids, description, organization, update frequencies, timestamps,
    licence, tags, format, download URL, record count, temporal and
    spatial coverage.

    `json` is one object; `csv` is a header row and one value row, with
    tags comma-joined. Requires `AUTH_TYPE=ckan`."""
    authorized = await context.authorize(resource_id=resource_id, permission="read")
    record = await metadata_record(context, resource_id, authorized)
    body = metadata_csv(record) if fmt == "csv" else json_bytes(record)
    return document_response(body, fmt, authorized, resource_id, "metadata")
