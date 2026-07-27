"""Download endpoints: `/datastore/dump/{resource_id}` + `/datastore/dump/query`.

csv / gzip / ndjson shards are composed into one GCS object, so those
always redirect — the server never touches the bytes. Parquet can't be
composed (footer + magic bytes), so a >1 GB parquet export stays several
files and is returned as a JSON list of signed URLs, which parquet
readers open as a single dataset.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from starlette.responses import RedirectResponse

from datastore.api.context import Context
from datastore.api.responses import ERROR_RESPONSES, ORJSONResponse
from datastore.core.constants import DumpFormat
from datastore.core.exceptions import ServerError
from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.request import DatastoreDumpSQLRequest
from datastore.services.read import dump_sql_datastore

router = APIRouter(tags=["Datastore Download"], responses=ERROR_RESPONSES)


def download_response(
    urls: list[str], fmt: DumpFormat,
) -> RedirectResponse | ORJSONResponse:
    """Shape the engine's signed URL(s) into a response:

      - one file   → 302 to the signed URL (every format but big parquet)
      - many files → JSON list of signed URLs (multi-file parquet)
      - none       → 500; the engine isn't configured
    """
    if not urls:
        raise ServerError(
            "export produced no downloadable files "
            "(datastore engine is not configured)"
        )
    if len(urls) == 1:
        return RedirectResponse(url=urls[0], status_code=302)
    return ORJSONResponse(
        {"format": fmt, "count": len(urls), "files": urls},
    )


@router.get(
    "/datastore/dump/query",
    summary="Download the result of a SQL SELECT (CSV / gzip CSV / NDJSON / Parquet)",
    responses={
        302: {"description": "Redirect to the signed download URL."},
        200: {
            "description": (
                "Multi-file parquet export — JSON list of signed URLs."
            ),
        },
    },
)
async def dump_sql(
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
    return download_response(urls, params.format)


@router.get(
    "/datastore/dump/{resource_id}",
    summary="Download an entire table (CSV / gzip CSV / NDJSON / Parquet)",
    responses={
        302: {"description": "Redirect to the signed Download URL."},
        200: {
            "description": (
                "Multi-file parquet export — JSON list of signed URLs."
            ),
        },
    },
)
async def dump(
    context: Context,
    resource_id: str,
    fmt: Annotated[DumpFormat, Query(alias="format")] = "csv",
):
    """Download an entire resource; pick the format with `?format=`."""
    await context.authorize(resource_id=resource_id, permission="read")
    engine = get_datastore_engine(context, mode="ro")

    urls = await engine.dump(resource_id, fmt)
    return download_response(urls, fmt)
