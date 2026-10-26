"""Tests for `GET <DUMP_PREFIX>/{resource_id}/dictionary` and `…/metadata`.

Both documents are built in memory and served as a 200 file. The
dictionary is the resource's schema (CKAN's copy under `AUTH_TYPE=ckan`);
the metadata joins the resource with its dataset from `package_show`
plus the engine's row count (0 from the placeholder BigQuery engine).
"""

from __future__ import annotations

import csv
import io
import json

import pytest
from datastore.analytics import action_name
from datastore.core.constants import DUMP_PREFIX
from datastore.services.metadata import _csv_cell, _split_temporal_coverage
from fastapi.testclient import TestClient

from tests.conftest import FakeCKAN

RESOURCE_ID = "balancing_auction_results_2025"
DICTIONARY_URL = f"{DUMP_PREFIX}/{RESOURCE_ID}/dictionary"
METADATA_URL = f"{DUMP_PREFIX}/{RESOURCE_ID}/metadata"

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


# --- dictionary -------------------------------------------------------------


def test_dictionary_csv_has_a_row_per_field(client: TestClient, seeded: FakeCKAN) -> None:
    response = client.get(DICTIONARY_URL)

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    assert response.headers["content-disposition"] == (
        'attachment; filename="Auction Results_data_dictionary.csv"'
    )
    header, *rows = _csv_rows(response.content)
    assert header == ["name", "type", "title", "description", "constraints", "unit"]
    assert rows == [
        [
            "auction_id",
            "integer",
            "Auction ID",
            "Unique auction identifier.",
            '{"required":true}',
            "",
        ],
        ["product_code", "string", "", "", "", "N/A"],
    ]


def test_dictionary_json_is_the_schema(client: TestClient, seeded: FakeCKAN) -> None:
    response = client.get(DICTIONARY_URL, params={"format": "json"})

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.headers["content-disposition"].endswith('_data_dictionary.json"')
    assert response.json() == SCHEMA


def test_dictionary_without_schema_is_header_only(client: TestClient) -> None:
    """Placeholder engine + no CKAN schema → no fields, still a valid file."""
    response = client.get(DICTIONARY_URL)

    assert response.status_code == 200
    assert response.content == b"\r\n"


# --- metadata ---------------------------------------------------------------


def test_metadata_json_carries_every_field(client: TestClient, seeded: FakeCKAN) -> None:
    response = client.get(METADATA_URL, params={"format": "json"})

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.headers["content-disposition"] == (
        'attachment; filename="Auction Results_metadata.json"'
    )
    assert response.json() == {
        "Dataset name": "Balancing Market 2025",
        "Dataset id": "pkg-balancing-2025",
        "Dataset description": "Balancing auctions.",
        "Data group": "NESO",
        "Dataset update frequency": "monthly",
        "Dataset created": "2024-12-01T09:00:00",
        "Dataset last updated": "2025-02-01T09:00:00",
        "Licence": "NESO Open Data Licence",
        "Tags": ["balancing", "auctions"],
        "Resource name": "balancing-auction-results-2025",
        "Resource id": RESOURCE_ID,
        "Format": "CSV",
        "Resource created": "2025-01-01T09:00:00",
        # No `metadata_modified` on the resource → falls back to `last_modified`.
        "Resource last updated": "2025-03-01T09:00:00",
        "Download URL": f"https://example.com{DUMP_PREFIX}/{RESOURCE_ID}",
        "Total record count": 0,
        "Resource update frequency": "daily",
        "Temporal coverage from": "2025-01-01",
        "Temporal coverage to": "2025-12-31",
        "Spatial coverage": "GB",
    }


def test_metadata_prefers_resource_metadata_modified(client: TestClient, seeded: FakeCKAN) -> None:
    seeded.resources[RESOURCE_ID]["metadata_modified"] = "2025-04-01T09:00:00"

    record = client.get(METADATA_URL, params={"format": "json"}).json()

    assert record["Resource last updated"] == "2025-04-01T09:00:00"


def test_metadata_csv_is_header_plus_one_row(client: TestClient, seeded: FakeCKAN) -> None:
    response = client.get(METADATA_URL)

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    header, *rows = _csv_rows(response.content)
    assert len(rows) == 1
    row = dict(zip(header, rows[0], strict=True))
    assert header[0] == "Dataset name"
    assert row["Tags"] == "balancing, auctions"
    assert row["Data group"] == "NESO"
    assert row["Total record count"] == "0"


def test_metadata_reads_the_dataset_through_package_show(
    client: TestClient, seeded: FakeCKAN
) -> None:
    """The dataset comes from `package_show`, not the (cached, possibly
    partial) dict `datastore_authorize` returned."""
    record = client.get(METADATA_URL, params={"format": "json"}).json()

    assert seeded.package_show_calls == 1
    assert record["Dataset id"] == "pkg-balancing-2025"


def test_metadata_requires_ckan_auth(fake_ckan: FakeCKAN) -> None:
    from datastore.api.context import get_auth_provider, get_ckan_client
    from datastore.auth.anonymous import Provider as AnonymousProvider
    from datastore.core.config import Config, get_config
    from datastore.main import create_app

    app = create_app()
    app.dependency_overrides[get_config] = lambda: Config(AUTH_TYPE="anonymous", CKAN_URL="")
    app.dependency_overrides[get_ckan_client] = lambda: fake_ckan
    app.dependency_overrides[get_auth_provider] = lambda: AnonymousProvider()

    with TestClient(app) as c:
        response = c.get(METADATA_URL)

    assert response.status_code == 400
    assert "AUTH_TYPE=ckan" in response.json()["error"]["message"]


# --- shared -----------------------------------------------------------------


@pytest.mark.parametrize("url", [DICTIONARY_URL, METADATA_URL])
def test_unknown_format_is_rejected(client: TestClient, url: str) -> None:
    response = client.get(url, params={"format": "parquet"})

    assert response.status_code == 400
    assert response.json()["success"] is False


@pytest.mark.parametrize("document", ["dictionary", "metadata"])
def test_unknown_resource_is_404(client: TestClient, document: str) -> None:
    response = client.get(f"{DUMP_PREFIX}/no_such_resource/{document}")

    assert response.status_code == 404


@pytest.mark.parametrize("document", ["dictionary", "metadata"])
def test_denied_key_is_403(client: TestClient, fake_ckan: FakeCKAN, document: str) -> None:
    fake_ckan.deny("test-token")

    response = client.get(f"{DUMP_PREFIX}/{RESOURCE_ID}/{document}")

    assert response.status_code == 403


@pytest.mark.parametrize("document", ["dictionary", "metadata"])
def test_analytics_names_each_document(document: str) -> None:
    assert action_name(f"{DUMP_PREFIX}/res-1/{document}") == f"datastore_dump_{document}"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2025-01-01/2025-12-31", ("2025-01-01", "2025-12-31")),
        ("2025-01-01", ("2025-01-01", None)),
        ("", (None, None)),
        (None, (None, None)),
    ],
)
def test_split_temporal_coverage(value: str | None, expected: tuple[str | None, ...]) -> None:
    assert _split_temporal_coverage(value) == expected


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
