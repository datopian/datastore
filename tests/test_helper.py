"""Tests for `datastore.core.helper` download-filename helpers."""

from __future__ import annotations

from typing import Any

import pytest
from datastore.core.helper import (
    content_disposition,
    download_filename_base,
    dump_filename_base,
)


@pytest.mark.parametrize(
    "resource",
    [
        None,
        {},
        {"download_filename": None},
        {"download_filename": ""},
        {"download_filename": " . "},
    ],
)
def test_download_filename_base_is_none_when_unset(resource: dict[str, Any] | None) -> None:
    assert download_filename_base(resource, "csv") is None


@pytest.mark.parametrize(
    ("value", "ext", "expected"),
    [
        ("  Viking Link Capacity ", "csv", "Viking Link Capacity"),
        ("Flows.CSV", "csv", "Flows"),
        ("Flows.csv", "csv.gz", "Flows"),
        ("Flows.csv.gz", "csv.gz", "Flows"),
        ("Report v1.2", "parquet", "Report v1.2"),
        ('a/b\\c"d\r\n', "json", "abcd"),
    ],
)
def test_download_filename_base_cleans_value(value: str, ext: str, expected: str) -> None:
    assert download_filename_base({"download_filename": value}, ext) == expected


def test_content_disposition_keeps_plain_form_for_ascii() -> None:
    assert (
        content_disposition("Auction Results.csv") == 'attachment; filename="Auction Results.csv"'
    )


def test_content_disposition_adds_utf8_name_for_non_ascii() -> None:
    assert content_disposition("Données.csv") == (
        "attachment; filename=\"Donnees.csv\"; filename*=UTF-8''Donn%C3%A9es.csv"
    )


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://ckan/dataset/d/resource/r/download/my-data.csv", "my-data"),
        ("https://ckan/dataset/d/resource/r/download/My%20Data.v2.csv", "My Data.v2"),
        ("https://ckan/dataset/d/resource/r/download/flows.csv.gz", "flows"),
        ("https://ckan/dataset/d/resource/r/download/", None),
        (None, None),
    ],
)
def test_dump_filename_base_names_uploads_after_url(url: str | None, expected: str | None) -> None:
    resource = {"url_type": "upload", "url": url, "download_filename": "Ignored"}
    assert dump_filename_base(resource, "csv") == expected


def test_dump_filename_base_uses_download_filename_for_other_resources() -> None:
    resource = {"url_type": "datastore", "url": "https://x/dump/r", "download_filename": "Flows"}
    assert dump_filename_base(resource, "csv") == "Flows"
