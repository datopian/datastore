"""Data dictionary + metadata documents for `<DUMP_PREFIX>/{rid}/…`.

Both are small, so they're built in memory and handed to the endpoint as
bytes — no export pipeline, no GCS. CSV and JSON carry the same content;
CSV flattens anything nested into a single cell.
"""

from __future__ import annotations

import asyncio
import csv
import io
from typing import TYPE_CHECKING, Any

import orjson

from datastore.core.exceptions import NotFoundError, ValidationError
from datastore.core.helper import dump_url
from datastore.infrastructure.engines import get_datastore_engine
from datastore.services.read import info_datastore

if TYPE_CHECKING:  # type-only — no runtime import from api/
    from datastore.api.context import RequestContext


# Frictionless field properties that lead the dictionary CSV, in this
# order; any other property a field carries follows in first-seen order.
_DICTIONARY_LEADING_COLUMNS = ("name", "type", "title", "description")


async def dictionary_document(context: RequestContext, data_dict: dict[str, Any]) -> dict[str, Any]:
    """The resource's Frictionless Table Schema — the same `schema`
    `datastore_info` returns (CKAN's copy under `AUTH_TYPE=ckan`, else
    the engine's)."""
    result = await info_datastore(context, data_dict)
    return result.schema


def dictionary_csv(schema: dict[str, Any]) -> bytes:
    """One row per field, one column per field property."""
    fields = [f for f in schema.get("fields") or [] if isinstance(f, dict)]
    columns = [c for c in _DICTIONARY_LEADING_COLUMNS if any(c in f for f in fields)]
    for f in fields:
        columns.extend(key for key in f if key not in columns)
    return _csv_bytes(columns, fields)


async def metadata_record(
    context: RequestContext, resource_id: str, authorized: dict[str, Any]
) -> dict[str, Any]:
    """The resource's metadata plus its dataset's, one flat record keyed
    by the labels the website shows.

    Reads the dataset through `package_show` rather than the dict
    `datastore_authorize` returned: that one is cached by the auth
    provider and isn't guaranteed to carry the organization, tags or
    scheming extras.
    """
    if context.config.AUTH_TYPE != "ckan" or context.ckan is None:
        raise ValidationError("Metadata download requires AUTH_TYPE=ckan")

    authorized_resource = authorized.get("resource") or {}
    package_id = authorized_resource.get("package_id") or (authorized.get("package") or {}).get(
        "id"
    )
    if not package_id:
        raise NotFoundError(f"resource {resource_id!r} has no dataset")

    package = await context.ckan.package_show(package_id=str(package_id))
    resource = next(
        (r for r in package.get("resources") or [] if r.get("id") == resource_id),
        authorized_resource,
    )
    temporal_from, temporal_to = _split_temporal_coverage(resource.get("temporal_coverage"))

    return {
        "Dataset name": package.get("title"),
        "Dataset id": package.get("id"),
        "Dataset description": package.get("notes"),
        "Data group": (package.get("organization") or {}).get("title"),
        "Dataset update frequency": package.get("update_frequency"),
        "Dataset created": package.get("metadata_created"),
        "Dataset last updated": package.get("metadata_modified"),
        "Licence": package.get("license_title") or package.get("license_id"),
        "Tags": [t["name"] for t in package.get("tags") or [] if t.get("name")],
        "Resource name": resource.get("name"),
        "Resource id": resource_id,
        "Format": resource.get("format"),
        "Resource created": resource.get("created"),
        # Same fallback the frontend shows as "updated", so the file and
        # the page agree.
        "Resource last updated": resource.get("metadata_modified") or resource.get("last_modified"),
        "Download URL": dump_url(context.config, resource_id),
        "Total record count": await _record_count(context, resource_id),
        "Resource update frequency": resource.get("update_frequency"),
        "Temporal coverage from": temporal_from,
        "Temporal coverage to": temporal_to,
        "Spatial coverage": resource.get("spatial_coverage"),
    }


def metadata_csv(record: dict[str, Any]) -> bytes:
    """A header row and one value row."""
    return _csv_bytes(list(record), [record])


def json_bytes(document: Any) -> bytes:
    return orjson.dumps(document, option=orjson.OPT_INDENT_2)


async def _record_count(context: RequestContext, resource_id: str) -> int | None:
    """Row count from the engine; None when the resource has no table
    yet — the rest of the metadata is still worth downloading."""
    engine = get_datastore_engine(context, mode="ro")
    try:
        info = await asyncio.to_thread(engine.info, resource_id=resource_id)
    except NotFoundError:
        return None
    total = info.meta.get("total_records")
    return total if isinstance(total, int) else None


def _split_temporal_coverage(value: Any) -> tuple[str | None, str | None]:
    """`YYYY-MM-DD/YYYY-MM-DD` → (from, to). The validator enforces that
    shape, but a legacy value without the `/` lands whole in `from`."""
    if not isinstance(value, str) or not value.strip():
        return None, None
    start, sep, end = value.partition("/")
    return start.strip() or None, (end.strip() or None) if sep else None


def _csv_bytes(columns: list[str], rows: list[dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([_csv_cell(row.get(c)) for c in columns])
    return buf.getvalue().encode("utf-8")


def _csv_cell(value: Any) -> str:
    """Scalars as-is, a list of scalars comma-joined (tags), anything
    else nested as compact JSON."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return str(value)
    if isinstance(value, list) and all(isinstance(v, (str, int, float)) for v in value):
        return ", ".join(str(v) for v in value)
    return orjson.dumps(value).decode()
