"""Download endpoints: `<DUMP_PREFIX>/{resource_id}` + `<DUMP_PREFIX>/query`.

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
from starlette.responses import RedirectResponse, StreamingResponse

from datastore.api.context import Context
from datastore.api.responses import ERROR_RESPONSES
from datastore.core.constants import DUMP_EXTENSIONS, DUMP_MEDIA_TYPES, DumpFormat
from datastore.core.exceptions import ServerError
from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.request import DatastoreDumpSQLRequest
from datastore.services.read import dump_sql_datastore
from datastore.services.streaming import zip_archive_writer

router = APIRouter(tags=["Datastore Download"], responses=ERROR_RESPONSES)


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
            "Content-Disposition": (f'attachment; filename="{filename_base}.zip"'),
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
    """Download query result as a file."""
    for resource_id in params.resource_ids:
        await context.authorize(resource_id=resource_id, permission="read")

    urls = await dump_sql_datastore(
        context,
        {
            "sql": params.sql,
            "fmt": params.format,
            "resource_ids": params.resource_ids,
            "function_names": params.function_names,
        },
    )
    return download_response(request, urls, params.format, "query")


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
    resource_id: str,
    fmt: Annotated[DumpFormat, Query(alias="format")] = "csv",
):
    """Download an entire resource; pick the format with `?format=`."""
    await context.authorize(resource_id=resource_id, permission="read")
    engine = get_datastore_engine(context, mode="ro")

    urls = await engine.dump(resource_id, fmt)
    return download_response(request, urls, fmt, resource_id)
