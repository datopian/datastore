from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from datastore.infrastructure.engines import get_datastore_engine
from datastore.schemas.validators import (
    to_csv_list,
    to_json_object,
    to_str_or_json_object,
)
from datastore.services.streaming import (
    stream_csv,
    stream_lists,
    stream_objects,
    stream_tsv,
)

if TYPE_CHECKING:
    from datastore.api.context import RequestContext


_WRITERS = {
    "csv":     stream_csv,
    "tsv":     stream_tsv,
    "lists":   stream_lists,
    "objects": stream_objects,
}


async def search_datastore(
    context: RequestContext,
    data_dict: dict[str, Any],
    *,
    request_url: str,
) -> Iterator[bytes]:
    """Run the search and return a lazy `bytes` iterator over the JSON
    response body.

    All four `records_format` writers emit the same CKAN envelope and
    Content-Type (`application/json`); only the shape of `records` inside
    differs, so the endpoint hardcodes the media type and just wraps the
    returned iterator in a `StreamingResponse`.

    `data_dict` is the auth result merged with `params.model_dump()`, so
    every schema field is present with its default. `request_url` is
    passed in as a string because services can't import Starlette — we
    parse it with `urllib.parse` to build the relative `_links`.

    The returned iterator pulls rows from the engine one at a time;
    peak memory ≈ 1 row regardless of result size.
    """
    engine = get_datastore_engine(context, mode="ro")
    result = engine.search(
        resource_id=data_dict["resource_id"],
        filters=to_json_object(data_dict["filters"]),
        q=to_str_or_json_object(data_dict["q"]),
        fields=to_csv_list(data_dict["fields"]),
        distinct=data_dict["distinct"],
        plain=data_dict["plain"],
        language=data_dict["language"],
        limit=data_dict["limit"],
        offset=data_dict["offset"],
        sort=data_dict["sort"],
        include_total=data_dict["include_total"],
    )

    envelope_kwargs = dict(
        help_url=request_url,
        resource_id=data_dict["resource_id"],
        fields=result.fields,
        records=result.records,
        limit=data_dict["limit"],
        offset=data_dict["offset"],
        total=result.total,
        include_total=data_dict["include_total"],
        links=_build_pagination_links(
            request_url,
            limit=data_dict["limit"],
            offset=data_dict["offset"],
        ),
    )

    return _WRITERS[data_dict["records_format"]](**envelope_kwargs)


def _build_pagination_links(
    url: str, *, limit: int, offset: int
) -> dict[str, str]:
    """CKAN-style pagination links.

    `start` strips `offset` (it defaults to 0). `next` appends
    `offset = offset + limit`. All other params ride along on both
    links so the caller can paginate without re-assembling the URL.

    Scheme + host are preserved from the input URL when present, so
    `http://host/path?x=1` produces `http://host/path?...` links and
    a bare `/path?x=1` produces bare-path links. `urllib.parse` is
    used instead of Starlette's `URL` helpers because services don't
    import Starlette (CLAUDE.md §3 layer rule).
    """
    parsed = urlparse(url)
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    start_pairs = [(k, v) for k, v in pairs if k != "offset"]
    next_pairs = start_pairs + [("offset", str(offset + limit))]

    def _qs(pairs: list[tuple[str, str]]) -> str:
        return urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            "", urlencode(pairs), "",
        ))

    return {"start": _qs(start_pairs), "next": _qs(next_pairs)}
