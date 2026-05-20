"""Unit tests for `BigQueryMetadataStore`.

The store talks to BigQuery via `client.query(sql, job_config=...)`. We
mock the client so tests can pin:
  - what SQL the store issues (DDL on `initialize`, INSERT on `insert`,
    UPDATE on `update`, SELECT on `get`, DELETE on `delete`);
  - what query parameters travel alongside the SQL.

No real BigQuery is contacted — these are pure unit tests over the
SQL the store generates.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from datastore.infrastructure.engines.bigquery.metadata import (
    METADATA_TABLE_NAME,
    BigQueryMetadataStore,
)


@pytest.fixture
def mock_client() -> MagicMock:
    """A `bigquery.Client` stand-in that records `.query(...)` calls.

    `client.query(sql, job_config=...)` returns a job whose `.result()`
    yields whatever the test arranges via `mock_client.set_rows([...])`.
    """
    client = MagicMock()
    job = MagicMock()
    job.result.return_value = []
    client.query.return_value = job

    def _set_rows(rows: list[dict[str, Any]]) -> None:
        job.result.return_value = rows

    client.set_rows = _set_rows  # type: ignore[attr-defined]
    return client


@pytest.fixture
def store(mock_client: MagicMock) -> BigQueryMetadataStore:
    return BigQueryMetadataStore(
        client=mock_client,
        project="proj-1",
        dataset="ds-1",
    )


# --- initialize ------------------------------------------------------------


def test_initialize_issues_create_table_if_not_exists(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    store.initialize()

    assert mock_client.query.call_count == 1
    sql = mock_client.query.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "`proj-1.ds-1._table_metadata`" in sql
    # Schema columns are declared.
    for col in (
        "resource_id STRING",
        "schema      JSON",
        "created_at  TIMESTAMP",
        "updated_at  TIMESTAMP",
    ):
        assert col in sql


def test_initialize_is_idempotent(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    """Two `initialize()` calls — both safe because the DDL uses
    `IF NOT EXISTS`."""
    store.initialize()
    store.initialize()

    assert mock_client.query.call_count == 2


# --- insert ----------------------------------------------------------------


def test_insert_issues_parameterised_insert(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    schema = {
        "fields": [{"name": "a", "type": "integer"}],
        "primaryKey": ["a"],
    }
    store.insert("res-1", schema)

    assert mock_client.query.call_count == 1
    sql, kwargs = mock_client.query.call_args
    sql_text = sql[0]
    assert "INSERT INTO" in sql_text
    assert "MERGE" not in sql_text  # no upsert semantics
    assert "PARSE_JSON(@schema)" in sql_text
    assert "CURRENT_TIMESTAMP()" in sql_text

    params = {p.name: p.value for p in kwargs["job_config"].query_parameters}
    assert params["resource_id"] == "res-1"
    assert json.loads(params["schema"]) == schema


# --- update ----------------------------------------------------------------


def test_update_issues_parameterised_update(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    schema = {"fields": [{"name": "b", "type": "string"}]}
    store.update("res-1", schema)

    assert mock_client.query.call_count == 1
    sql, kwargs = mock_client.query.call_args
    sql_text = sql[0]
    assert "UPDATE" in sql_text
    assert "SET schema = PARSE_JSON(@schema)" in sql_text
    assert "WHERE resource_id = @resource_id" in sql_text
    # `created_at` must NOT be reassigned by update.
    assert "created_at" not in sql_text

    params = {p.name: p.value for p in kwargs["job_config"].query_parameters}
    assert params["resource_id"] == "res-1"
    assert json.loads(params["schema"]) == schema


# --- get -------------------------------------------------------------------


def test_get_returns_parsed_schema(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    schema = {"fields": [{"name": "a", "type": "integer"}]}
    mock_client.set_rows([{"schema_json": json.dumps(schema)}])

    out = store.get("res-1")

    assert out == schema


def test_get_returns_none_when_no_row(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    mock_client.set_rows([])

    assert store.get("does-not-exist") is None


# --- delete ----------------------------------------------------------------


def test_delete_issues_parameterised_delete(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    store.delete("res-1")

    assert mock_client.query.call_count == 1
    sql, kwargs = mock_client.query.call_args
    assert "DELETE FROM" in sql[0]
    assert "WHERE resource_id = @resource_id" in sql[0]
    params = {p.name: p.value for p in kwargs["job_config"].query_parameters}
    assert params["resource_id"] == "res-1"


# --- table reference -------------------------------------------------------

def test_table_ref_format(store: BigQueryMetadataStore) -> None:
    assert store.table_ref == "`proj-1.ds-1._table_metadata`"
    assert store.table_name == METADATA_TABLE_NAME


# --- error wrapping --------------------------------------------------------


def test_insert_wraps_bigquery_errors_as_server_error(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    """Raw BigQuery exceptions are wrapped as `ServerError` carrying
    the operation name + resource_id."""
    from datastore.core.exceptions import ServerError

    mock_client.query.return_value.result.side_effect = RuntimeError(
        "quota exceeded"
    )

    with pytest.raises(ServerError) as exc:
        store.insert("res-1", {"fields": [{"name": "a", "type": "integer"}]})

    msg = str(exc.value)
    assert "metadata INSERT" in msg
    assert "'res-1'" in msg
    assert "quota exceeded" in msg


def test_update_wraps_bigquery_errors_as_server_error(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    from datastore.core.exceptions import ServerError

    mock_client.query.return_value.result.side_effect = RuntimeError(
        "bigquery is sad"
    )

    with pytest.raises(ServerError) as exc:
        store.update("res-1", {"fields": [{"name": "a", "type": "integer"}]})

    assert "metadata UPDATE" in str(exc.value)
    assert "'res-1'" in str(exc.value)


def test_initialize_wraps_bigquery_errors_as_server_error(
    store: BigQueryMetadataStore, mock_client: MagicMock
) -> None:
    """Init has no resource_id; the error message uses `<init>` so the
    target is still labelled."""
    from datastore.core.exceptions import ServerError

    mock_client.query.return_value.result.side_effect = RuntimeError(
        "permission denied"
    )

    with pytest.raises(ServerError) as exc:
        store.initialize()

    msg = str(exc.value)
    assert "metadata CREATE TABLE" in msg
    assert "<init>" in msg
