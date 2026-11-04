"""Data dictionary document for `<DUMP_PREFIX>/{rid}/dictionary`.

Small, so it's built in memory and handed to the endpoint as bytes — no
export pipeline, no GCS. CSV and JSON carry the same content; CSV
flattens anything nested into a single cell. (Resource/dataset metadata
is not a datastore concern: it is served by CKAN.)
"""

from __future__ import annotations

import csv
import io
from typing import TYPE_CHECKING, Any

import orjson

from datastore.core.constants import RESERVED_SYSTEM_COLUMN_NAMES
from datastore.services.read import info_datastore

if TYPE_CHECKING:  # type-only — no runtime import from api/
    from datastore.api.context import RequestContext


# Frictionless field properties that lead the dictionary CSV, in this
# order; any other property a field carries follows in first-seen order.
_DICTIONARY_LEADING_COLUMNS = ("name", "type", "title", "description")

# Engine-managed columns every table carries but no user schema declares.
# The dictionary lists them where downloads and `datastore_search` put
# them: `_id` first, `_updated_at` last.
_ID_FIELD = {
    "name": "_id",
    "type": "integer",
    "title": "Row ID",
    "description": "System generated. Row identifier assigned by the datastore.",
}
_UPDATED_AT_FIELD = {
    "name": "_updated_at",
    "type": "datetime",
    "title": "Row last updated",
    "description": "System generated. When the row was last inserted or changed (UTC).",
}


async def dictionary_document(context: RequestContext, data_dict: dict[str, Any]) -> dict[str, Any]:
    """The resource's Frictionless Table Schema — the same `schema`
    `datastore_info` returns (CKAN's copy under `AUTH_TYPE=ckan`, else
    the engine's), plus the system columns."""
    result = await info_datastore(context, data_dict)
    return _with_system_fields(result.schema, context.config.INCLUDE_UPDATED_AT)


def dictionary_csv(schema: dict[str, Any]) -> bytes:
    """One row per field, one column per field property."""
    fields = [f for f in schema.get("fields") or [] if isinstance(f, dict)]
    columns = [c for c in _DICTIONARY_LEADING_COLUMNS if any(c in f for f in fields)]
    for f in fields:
        columns.extend(key for key in f if key not in columns)
    return _csv_bytes(columns, fields)


def json_bytes(document: Any) -> bytes:
    return orjson.dumps(document, option=orjson.OPT_INDENT_2)


def _with_system_fields(schema: dict[str, Any], include_updated_at: bool) -> dict[str, Any]:
    """`schema` with `_id` prepended and `_updated_at` (when the engine
    adds it) appended. A schema with no fields means no table to
    describe, so it's returned as is."""
    fields = [f for f in schema.get("fields") or [] if isinstance(f, dict)]
    user = [f for f in fields if f.get("name") not in RESERVED_SYSTEM_COLUMN_NAMES]
    if not user:
        return schema
    system_tail = [_UPDATED_AT_FIELD] if include_updated_at else []
    return {**schema, "fields": [_ID_FIELD, *user, *system_tail]}


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
