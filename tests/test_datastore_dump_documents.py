"""Tests for `GET <DUMP_PREFIX>/{resource_id}/table_schema`.

The document is built in memory and served as a 200 file: the resource's
schema (CKAN's copy under `AUTH_TYPE=ckan`) plus the system columns.
"""

from __future__ import annotations

import csv
import io
import json

import pytest
from datastore.analytics import action_name
from datastore.core.constants import DUMP_PREFIX
from datastore.services.metadata import (
    _ID_FIELD,
    _UPDATED_AT_FIELD,
    _csv_cell,
    _with_system_fields,
)
from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

RESOURCE_ID = "balancing_auction_results_2025"
TABLE_SCHEMA_URL = f"{DUMP_PREFIX}/{RESOURCE_ID}/table_schema"

SCHEMA = {
    "fields": [
        {
            "name": "auction_id",
            "type": "integer",
            "title": "Auction ID",
            "description": "Unique auction identifier.",
            "constraints": {"required": True},
        },
        {"name": "product_code", "type": "string", "unit": "N/A"},
    ],
    "primaryKey": ["auction_id"],
}


@pytest.fixture
def seeded(fake_ckan: FakeCKAN) -> FakeCKAN:
    """A resource + dataset carrying every field the metadata reads."""
    fake_ckan.resources[RESOURCE_ID].update(
        schema=SCHEMA,
        format="CSV",
        created="2025-01-01T09:00:00",
        last_modified="2025-03-01T09:00:00",
        update_frequency="daily",
        temporal_coverage="2025-01-01/2025-12-31",
        spatial_coverage="GB",
        download_filename="Auction Results",
    )
    fake_ckan.packages["pkg-balancing-2025"].update(
        notes="Balancing auctions.",
        organization={"name": "neso", "title": "NESO"},
        update_frequency="monthly",
        metadata_created="2024-12-01T09:00:00",
        metadata_modified="2025-02-01T09:00:00",
        license_title="NESO Open Data Licence",
        tags=[{"name": "balancing"}, {"name": "auctions"}],
    )
    return fake_ckan


def _csv_rows(body: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(body.decode("utf-8"))))


# --- table_schema -----------------------------------------------------------


def test_table_schema_csv_has_a_row_per_field(client: TestClient, seeded: FakeCKAN) -> None:
    response = client.get(TABLE_SCHEMA_URL)

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    assert response.headers["content-disposition"] == (
        'attachment; filename="Auction Results_table_schema.csv"'
    )
    header, *rows = _csv_rows(response.content)
    assert header == ["name", "type", "title", "description", "constraints", "unit"]
    assert rows == [
        [
            "_id",
            "integer",
            "Row ID",
            "System generated. Row identifier assigned by the datastore.",
            "",
            "",
        ],
        [
            "auction_id",
            "integer",
            "Auction ID",
            "Unique auction identifier.",
            '{"required":true}',
            "",
        ],
        ["product_code", "string", "", "", "", "N/A"],
        [
            "_updated_at",
            "datetime",
            "Row last updated",
            "System generated. When the row was last inserted or changed (UTC).",
            "",
            "",
        ],
    ]


def test_table_schema_json_is_the_schema_plus_system_columns(
    client: TestClient, seeded: FakeCKAN
) -> None:
    response = client.get(TABLE_SCHEMA_URL, params={"format": "json"})

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.headers["content-disposition"].endswith('_table_schema.json"')
    assert response.json() == {
        **SCHEMA,
        "fields": [_ID_FIELD, *SCHEMA["fields"], _UPDATED_AT_FIELD],
    }


def test_system_fields_follow_include_updated_at() -> None:
    assert _with_system_fields(SCHEMA, include_updated_at=False)["fields"] == [
        _ID_FIELD,
        *SCHEMA["fields"],
    ]


def test_system_fields_replace_declared_ones() -> None:
    """A schema already naming a system column (e.g. inferred from an
    unmanaged table) doesn't list it twice."""
    schema = {"fields": [{"name": "_id", "type": "string"}, {"name": "a", "type": "string"}]}

    names = [f["name"] for f in _with_system_fields(schema, include_updated_at=True)["fields"]]

    assert names == ["_id", "a", "_updated_at"]


def test_table_schema_without_schema_is_header_only(client: TestClient) -> None:
    """Placeholder engine + no CKAN schema → no fields, still a valid file."""
    response = client.get(TABLE_SCHEMA_URL)

    assert response.status_code == 200
    assert response.content == b"\r\n"


# --- shared -----------------------------------------------------------------


def test_unknown_format_is_rejected(client: TestClient) -> None:
    response = client.get(TABLE_SCHEMA_URL, params={"format": "parquet"})

    assert response.status_code == 400
    assert response.json()["success"] is False


def test_unknown_resource_is_404(client: TestClient) -> None:
    response = client.get(f"{DUMP_PREFIX}/no_such_resource/table_schema")

    assert response.status_code == 404


def test_denied_key_is_403(client: TestClient, fake_ckan: FakeCKAN) -> None:
    fake_ckan.deny("test-token")

    response = client.get(f"{DUMP_PREFIX}/{RESOURCE_ID}/table_schema")

    assert response.status_code == 403


def test_analytics_names_the_table_schema_download() -> None:
    assert action_name(f"{DUMP_PREFIX}/res-1/table_schema") == "datastore_dump_table_schema"
    # Metadata is served by CKAN, so it is not a distinct datastore action.
    assert action_name(f"{DUMP_PREFIX}/res-1/metadata") == "datastore_dump"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ""),
        (True, "true"),
        (3, "3"),
        (["a", "b"], "a, b"),
        ({"k": [1]}, json.dumps({"k": [1]}, separators=(",", ":"))),
        ([{"k": 1}], '[{"k":1}]'),
    ],
)
def test_csv_cell_flattens_nested_values(value: object, expected: str) -> None:
    assert _csv_cell(value) == expected
