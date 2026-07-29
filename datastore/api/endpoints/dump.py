"""Download endpoints: `/datastore/dump/{resource_id}` + `/datastore/dump/query`.

csv / gzip / ndjson shards are composed into one GCS object, so those
always redirect — the server never touches the bytes. Parquet can't be
composed (footer + magic bytes), so a parquet export that shards is
zipped on the way out: the API fetches the parts and streams one
archive, which keeps every download a single file at one URL.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from starlette.requests import Request
from starlette.responses import RedirectResponse, StreamingResponse

from datastore.api.context import Context
from datastore.api.responses import ERROR_RESPONSES
from datastore.core.constants import DUMP_EXTENSIONS, DumpFormat
from datastore.core.exceptions import ServerError
from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.request import DatastoreDumpSQLRequest
from datastore.services.read import dump_sql_datastore
from datastore.services.streaming import zip_archive_writer

router = APIRouter(tags=["Datastore Download"], responses=ERROR_RESPONSES)


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
            "export produced no downloadable files "
            "(datastore engine is not configured)"
        )
    if len(urls) == 1:
        return RedirectResponse(url=urls[0], status_code=302)

    ext = DUMP_EXTENSIONS[fmt]
    members = [
        (f"{filename_base}_{i + 1:02d}.{ext}", url)
        for i, url in enumerate(urls)
    ]
    return StreamingResponse(
        zip_archive_writer(request.app.state.http, members),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{filename_base}.zip"'
            ),
        },
    )


@router.get(
    "/datastore/dump/query",
    summary="Download the result of a SQL SELECT (CSV / gzip CSV / NDJSON / Parquet)",
    responses={
        302: {"description": "Redirect to the signed download URL."},
        200: {
            "description": (
                "Sharded parquet export — one streamed zip of the parts."
            ),
            "content": {"application/zip": {}},
        },
    },
)
async def dump_sql(
    request: Request,
    context: Context,
    params: Annotated[DatastoreDumpSQLRequest, Query()],
):
    """Download a vetted SQL SELECT's result as one file (no envelope).

    Same validation as `datastore_search_sql`; `LIMIT` optional and
    uncapped. Declared before `/{resource_id}` → `query` is reserved.
    """
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
    "/datastore/dump/{resource_id}",
    summary="Download an entire table (CSV / gzip CSV / NDJSON / Parquet)",
    responses={
        302: {"description": "Redirect to the signed Download URL."},
        200: {
            "description": (
                "Multi-file parquet export — one streamed zip."
            ),
            "content": {"application/zip": {}},
        },
    },
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
