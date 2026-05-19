"""Unit tests for the BigQuery DDL paths (`_create_data_table`,
`_alter_data_table`) and the Frictionless→BigQuery type map.

We bypass `initialize()` (which builds a real `bigquery.Client`) by
constructing a `BigQueryBackend` directly and plugging in a mock
client, mock metadata store, and the config fields the DDL helpers
read. No real BigQuery is contacted.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from datastore.infrastructure.engines.bigquery.backend import BigQueryBackend
from datastore.infrastructure.engines.bigquery.types import (
    FRICTIONLESS_TO_BIGQUERY,
    bigquery_type,
    can_widen,
)


# --- types.py --------------------------------------------------------------


def test_bigquery_type_map_covers_canonical_frictionless_types() -> None:
    """Every canonical Frictionless type resolves to a concrete BQ type
    (never the fallback)."""
    for fr_type in (
        "integer", "number", "string", "boolean",
        "date", "time", "datetime",
        "object", "array", "geojson", "geopoint",
        "duration", "year", "yearmonth", "any",
    ):
        assert bigquery_type(fr_type) == FRICTIONLESS_TO_BIGQUERY[fr_type]


def test_bigquery_type_unknown_falls_back_to_string() -> None:
    assert bigquery_type("definitely-not-a-type") == "STRING"
    assert bigquery_type(None) == "STRING"
    assert bigquery_type("") == "STRING"


def test_bigquery_type_integer_maps_to_int64() -> None:
    assert bigquery_type("integer") == "INT64"


def test_bigquery_type_datetime_maps_to_timestamp() -> None:
    assert bigquery_type("datetime") == "TIMESTAMP"


# --- can_widen -------------------------------------------------------------


def test_can_widen_identity() -> None:
    """No-op transition (X → X) is always allowed."""
    assert can_widen("INT64", "INT64") is True
    assert can_widen("STRING", "STRING") is True


def test_can_widen_supported_numeric_chain() -> None:
    """BigQuery numeric widening: INT64 → NUMERIC/BIGNUMERIC/FLOAT64."""
    assert can_widen("INT64", "FLOAT64") is True
    assert can_widen("INT64", "NUMERIC") is True
    assert can_widen("INT64", "BIGNUMERIC") is True
    assert can_widen("NUMERIC", "FLOAT64") is True


def test_can_widen_date_to_datetime_and_timestamp() -> None:
    assert can_widen("DATE", "DATETIME") is True
    assert can_widen("DATE", "TIMESTAMP") is True


def test_can_widen_rejects_unsupported_transitions() -> None:
    """Anything outside the allowed widening map is rejected."""
    assert can_widen("INT64", "STRING") is False
    assert can_widen("STRING", "INT64") is False
    assert can_widen("BOOL", "STRING") is False
    assert can_widen("FLOAT64", "INT64") is False  # narrowing
    assert can_widen("TIMESTAMP", "DATE") is False  # narrowing


# --- DDL helpers (backend) -------------------------------------------------


def _backend(client: MagicMock) -> BigQueryBackend:
    """Build a backend with the mocked client + config plumbing the
    DDL helpers need, skipping `initialize()`."""
    backend = BigQueryBackend(mode="rw")
    backend.client = client
    backend.config = MagicMock()
    backend.config.BIGQUERY_PROJECT = "proj-1"
    backend.config.BIGQUERY_DATASET = "ds-1"
    return backend


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.query.return_value.result.return_value = []
    return client


def test_data_table_ref_uses_backticks_for_uuid_like_ids(
    mock_client: MagicMock,
) -> None:
    """Resource IDs with hyphens (CKAN UUIDs) need backticks to parse."""
    backend = _backend(mock_client)
    ref = backend._data_table_ref("res-abc-123")
    assert ref == "`proj-1.ds-1.res-abc-123`"


def test_create_data_table_emits_create_table_if_not_exists(
    mock_client: MagicMock,
) -> None:
    backend = _backend(mock_client)
    schema = {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "label", "type": "string"},
            {"name": "ts", "type": "datetime"},
        ]
    }

    backend._create_data_table("res-1", schema)

    assert mock_client.query.call_count == 1
    sql = mock_client.query.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "`proj-1.ds-1.res-1`" in sql
    assert "`id` INT64" in sql
    assert "`label` STRING" in sql
    assert "`ts` TIMESTAMP" in sql


def test_create_data_table_with_empty_schema_skips_ddl(
    mock_client: MagicMock,
) -> None:
    """An empty `schema.fields` is a no-op rather than an SQL error
    (CREATE TABLE with no columns is invalid in BQ)."""
    backend = _backend(mock_client)

    backend._create_data_table("res-1", {"fields": []})

    assert mock_client.query.call_count == 0


def test_alter_adds_only_new_columns(mock_client: MagicMock) -> None:
    backend = _backend(mock_client)
    old = {"fields": [{"name": "a", "type": "integer"}]}
    new = {
        "fields": [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "boolean"},
        ]
    }

    backend._alter_data_table("res-1", old, new)

    assert mock_client.query.call_count == 1
    sql = mock_client.query.call_args[0][0]
    assert "ALTER TABLE `proj-1.ds-1.res-1`" in sql
    assert "ADD COLUMN IF NOT EXISTS `b` STRING" in sql
    assert "ADD COLUMN IF NOT EXISTS `c` BOOL" in sql
    # Existing column must not appear.
    assert "ADD COLUMN IF NOT EXISTS `a`" not in sql


def test_alter_no_diff_is_noop(mock_client: MagicMock) -> None:
    backend = _backend(mock_client)
    schema = {"fields": [{"name": "a", "type": "integer"}]}

    backend._alter_data_table("res-1", schema, schema)

    assert mock_client.query.call_count == 0


def test_alter_applies_supported_type_widening(
    mock_client: MagicMock,
) -> None:
    """`integer` → `number` is a supported BQ widening (INT64 →
    FLOAT64). It should land as an `ALTER COLUMN SET DATA TYPE` clause."""
    backend = _backend(mock_client)
    old = {"fields": [{"name": "a", "type": "integer"}]}
    new = {"fields": [{"name": "a", "type": "number"}]}

    backend._alter_data_table("res-1", old, new)

    assert mock_client.query.call_count == 1
    sql = mock_client.query.call_args[0][0]
    assert "ALTER COLUMN `a` SET DATA TYPE FLOAT64" in sql


def test_alter_raises_conflict_on_unsupported_type_change(
    mock_client: MagicMock,
) -> None:
    """`integer` → `string` is NOT a supported BQ widening — the
    request must surface as a 409 ConflictError, not a silent skip."""
    from datastore.core.exceptions import ConflictError

    backend = _backend(mock_client)
    old = {"fields": [{"name": "a", "type": "integer"}]}
    new = {"fields": [{"name": "a", "type": "string"}]}

    with pytest.raises(ConflictError) as exc:
        backend._alter_data_table("res-1", old, new)

    msg = str(exc.value)
    assert "Cannot change column type" in msg
    assert "'a'" in msg
    assert "integer → string" in msg
    assert "recreate the resource" in msg
    # No DDL should have been issued — validation happens before any
    # statement runs so partial application is impossible.
    assert mock_client.query.call_count == 0


def test_alter_add_and_widen_in_single_statement(
    mock_client: MagicMock,
) -> None:
    """When a schema edit both adds a column and widens an existing
    column, both clauses go into one `ALTER TABLE` statement."""
    backend = _backend(mock_client)
    old = {"fields": [{"name": "a", "type": "integer"}]}
    new = {
        "fields": [
            {"name": "a", "type": "number"},  # widen
            {"name": "b", "type": "string"},  # add
        ]
    }

    backend._alter_data_table("res-1", old, new)

    assert mock_client.query.call_count == 1
    sql = mock_client.query.call_args[0][0]
    assert sql.count("ALTER TABLE") == 1
    assert "ADD COLUMN IF NOT EXISTS `b` STRING" in sql
    assert "ALTER COLUMN `a` SET DATA TYPE FLOAT64" in sql


def test_alter_does_not_drop_columns(
    mock_client: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Removing a column from the schema must not issue DDL — that
    would lose user data on a metadata edit."""
    backend = _backend(mock_client)
    old = {
        "fields": [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "string"},
        ]
    }
    new = {"fields": [{"name": "a", "type": "integer"}]}

    backend._alter_data_table("res-1", old, new)

    assert mock_client.query.call_count == 0


# --- create() wiring -------------------------------------------------------


def test_create_inserts_metadata_and_creates_table_for_new_resource(
    mock_client: MagicMock,
) -> None:
    """First `create()` call: metadata.insert + CREATE TABLE."""
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None  # not yet declared

    schema: dict[str, Any] = {
        "fields": [{"name": "a", "type": "integer"}]
    }
    backend.create("res-1", schema=schema, records=None, include_total=False)

    backend.metadata.insert.assert_called_once_with("res-1", schema)
    backend.metadata.update.assert_not_called()
    # client.query was called once for the CREATE TABLE DDL.
    assert mock_client.query.call_count == 1
    assert "CREATE TABLE IF NOT EXISTS" in mock_client.query.call_args[0][0]


def test_create_updates_metadata_and_alters_table_for_existing_resource(
    mock_client: MagicMock,
) -> None:
    """Second `create()` on the same resource: metadata.update + ALTER
    (when the schema added a column)."""
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = {
        "fields": [{"name": "a", "type": "integer"}]
    }

    new_schema = {
        "fields": [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "string"},
        ]
    }
    backend.create(
        "res-1", schema=new_schema, records=None, include_total=False
    )

    backend.metadata.update.assert_called_once_with("res-1", new_schema)
    backend.metadata.insert.assert_not_called()
    assert mock_client.query.call_count == 1
    sql = mock_client.query.call_args[0][0]
    assert "ALTER TABLE" in sql
    assert "ADD COLUMN IF NOT EXISTS `b` STRING" in sql


def test_create_rolls_back_metadata_on_alter_failure(
    mock_client: MagicMock,
) -> None:
    """If `_alter_data_table` raises (unsupported type change, BQ
    error, ...) the metadata row must NOT be updated — otherwise the
    `_table_metadata` row would describe a schema the actual table
    doesn't have."""
    from datastore.core.exceptions import ConflictError

    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = {
        "fields": [{"name": "a", "type": "integer"}]
    }

    # Unsupported widening — `_alter_data_table` raises before any DDL.
    new_schema = {"fields": [{"name": "a", "type": "string"}]}

    with pytest.raises(ConflictError):
        backend.create(
            "res-1", schema=new_schema, records=None, include_total=False
        )

    backend.metadata.update.assert_not_called()
    backend.metadata.insert.assert_not_called()


def test_create_rolls_back_metadata_on_create_table_failure(
    mock_client: MagicMock,
) -> None:
    """Same atomicity guarantee on the new-resource path: if `CREATE
    TABLE` fails, no metadata row gets inserted pointing to a missing
    table. Underlying BigQuery error surfaces as `ServerError` with
    operation + resource_id context — never as the raw exception."""
    from datastore.core.exceptions import ServerError

    mock_client.query.return_value.result.side_effect = RuntimeError("bq fail")

    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None  # new resource

    with pytest.raises(ServerError) as exc:
        backend.create(
            "res-1",
            schema={"fields": [{"name": "a", "type": "integer"}]},
            records=None,
            include_total=False,
        )

    assert "CREATE TABLE" in str(exc.value)
    assert "'res-1'" in str(exc.value)
    backend.metadata.insert.assert_not_called()
    backend.metadata.update.assert_not_called()


# --- error wrapping (run_query / run_insert_rows) -------------------------


def test_alter_wraps_bigquery_errors_as_server_error(
    mock_client: MagicMock,
) -> None:
    """A failure on the ALTER `client.query` call surfaces as
    `ServerError` with operation + resource_id context, not as the raw
    BigQuery exception."""
    from datastore.core.exceptions import ServerError

    mock_client.query.return_value.result.side_effect = RuntimeError(
        "Insufficient permissions"
    )

    backend = _backend(mock_client)
    old = {"fields": [{"name": "a", "type": "integer"}]}
    new = {
        "fields": [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "string"},
        ]
    }

    with pytest.raises(ServerError) as exc:
        backend._alter_data_table("res-1", old, new)

    msg = str(exc.value)
    assert "ALTER TABLE" in msg
    assert "'res-1'" in msg
    assert "Insufficient permissions" in msg


def test_insert_records_wraps_client_exception_as_server_error(
    mock_client: MagicMock,
) -> None:
    """If `insert_rows_json` itself raises (transport / setup error)
    the failure surfaces as `ServerError`, not as the raw exception."""
    from datastore.core.exceptions import ServerError

    mock_client.insert_rows_json.side_effect = RuntimeError("network down")

    backend = _backend(mock_client)

    with pytest.raises(ServerError) as exc:
        backend._insert_records(
            "res-1",
            {"fields": [{"name": "a", "type": "integer"}]},
            [{"a": 1}],
        )

    msg = str(exc.value)
    assert "INSERT" in msg
    assert "'res-1'" in msg
    assert "network down" in msg


# --- records insert --------------------------------------------------------


def test_insert_records_calls_insert_rows_json(
    mock_client: MagicMock,
) -> None:
    backend = _backend(mock_client)
    mock_client.insert_rows_json.return_value = []  # no errors
    schema = {
        "fields": [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "string"},
        ]
    }
    records = [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
    ]

    backend._insert_records("res-1", schema, records)

    mock_client.insert_rows_json.assert_called_once_with(
        "proj-1.ds-1.res-1", records
    )


def test_insert_records_empty_list_is_noop(mock_client: MagicMock) -> None:
    backend = _backend(mock_client)

    backend._insert_records("res-1", {"fields": []}, [])

    mock_client.insert_rows_json.assert_not_called()


def test_insert_records_raises_on_bigquery_errors(
    mock_client: MagicMock,
) -> None:
    """Any non-empty error list from BigQuery surfaces as ServerError —
    rows must not be silently dropped."""
    from datastore.core.exceptions import ServerError

    backend = _backend(mock_client)
    mock_client.insert_rows_json.return_value = [
        {"index": 0, "errors": [{"reason": "invalid"}]}
    ]

    with pytest.raises(ServerError) as exc:
        backend._insert_records(
            "res-1",
            {"fields": [{"name": "a", "type": "integer"}]},
            [{"a": 1}],
        )

    assert "BigQuery refused" in str(exc.value)
    assert "'res-1'" in str(exc.value)


def test_insert_records_serialises_object_columns_to_json_strings(
    mock_client: MagicMock,
) -> None:
    """BigQuery `JSON` columns accept JSON strings on the wire, not
    native dicts. Frictionless `object` → BQ `JSON`, so dict values
    must be serialised before `insert_rows_json`."""
    import json

    backend = _backend(mock_client)
    mock_client.insert_rows_json.return_value = []
    schema = {
        "fields": [
            {"name": "auction_id", "type": "integer"},
            {"name": "bidder_metadata", "type": "object"},
        ]
    }
    records = [
        {
            "auction_id": 144,
            "bidder_metadata": {"unit_id": "DRAX-1", "submission_lag_ms": 412},
        }
    ]

    backend._insert_records("res-1", schema, records)

    sent = mock_client.insert_rows_json.call_args[0][1]
    assert sent[0]["auction_id"] == 144  # scalar untouched
    # `bidder_metadata` arrives as a JSON string, not a dict.
    assert isinstance(sent[0]["bidder_metadata"], str)
    assert json.loads(sent[0]["bidder_metadata"]) == {
        "unit_id": "DRAX-1",
        "submission_lag_ms": 412,
    }


def test_insert_records_serialises_array_and_geojson_columns(
    mock_client: MagicMock,
) -> None:
    """`array` and `geojson` also map to BQ `JSON` — same treatment."""
    import json

    backend = _backend(mock_client)
    mock_client.insert_rows_json.return_value = []
    schema = {
        "fields": [
            {"name": "tags", "type": "array"},
            {"name": "where", "type": "geojson"},
        ]
    }
    records = [
        {
            "tags": ["a", "b"],
            "where": {"type": "Point", "coordinates": [1, 2]},
        }
    ]

    backend._insert_records("res-1", schema, records)

    sent = mock_client.insert_rows_json.call_args[0][1]
    assert json.loads(sent[0]["tags"]) == ["a", "b"]
    assert json.loads(sent[0]["where"]) == {
        "type": "Point", "coordinates": [1, 2],
    }


def test_insert_records_passes_through_string_json_values(
    mock_client: MagicMock,
) -> None:
    """A caller who already sends a pre-serialised JSON string for an
    `object` column shouldn't get it double-encoded."""
    backend = _backend(mock_client)
    mock_client.insert_rows_json.return_value = []
    schema = {"fields": [{"name": "meta", "type": "object"}]}
    records = [{"meta": '{"already": "json"}'}]

    backend._insert_records("res-1", schema, records)

    sent = mock_client.insert_rows_json.call_args[0][1]
    assert sent[0]["meta"] == '{"already": "json"}'


def test_insert_records_none_for_object_column_passes_through(
    mock_client: MagicMock,
) -> None:
    """`None` is a valid value for a nullable JSON column — must not be
    serialised to the literal string `"null"`."""
    backend = _backend(mock_client)
    mock_client.insert_rows_json.return_value = []
    schema = {"fields": [{"name": "meta", "type": "object"}]}
    records = [{"meta": None}]

    backend._insert_records("res-1", schema, records)

    sent = mock_client.insert_rows_json.call_args[0][1]
    assert sent[0]["meta"] is None


def test_create_writes_metadata_only_after_data_ops_succeed(
    mock_client: MagicMock,
) -> None:
    """`create()` end-to-end on the new-resource path: DDL → records
    insert → metadata. The metadata row is the *last* thing written so
    a failure in either data op leaves the metadata store untouched."""
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None
    mock_client.insert_rows_json.return_value = []
    schema: dict[str, Any] = {"fields": [{"name": "a", "type": "integer"}]}
    records = [{"a": 1}, {"a": 2}]

    # Records insert must happen before `metadata.insert` — record the
    # call ordering on the parent mock to verify it.
    parent = MagicMock()
    parent.attach_mock(mock_client.insert_rows_json, "insert_rows_json")
    parent.attach_mock(backend.metadata.insert, "metadata_insert")

    backend.create(
        "res-1", schema=schema, records=records, include_total=False
    )

    backend.metadata.insert.assert_called_once_with("res-1", schema)
    mock_client.insert_rows_json.assert_called_once_with(
        "proj-1.ds-1.res-1", records
    )
    # Order: records insert first, metadata.insert second.
    call_names = [c[0] for c in parent.mock_calls]
    assert call_names.index("insert_rows_json") < call_names.index(
        "metadata_insert"
    )


def test_create_rolls_back_metadata_on_records_insert_failure(
    mock_client: MagicMock,
) -> None:
    """If `insert_rows_json` reports errors on the new-resource path,
    `metadata.insert` must NOT run — otherwise metadata would declare
    a resource whose seed rows never landed."""
    from datastore.core.exceptions import ServerError

    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None
    mock_client.insert_rows_json.return_value = [
        {"index": 0, "errors": [{"reason": "invalid"}]}
    ]

    with pytest.raises(ServerError):
        backend.create(
            "res-1",
            schema={"fields": [{"name": "a", "type": "integer"}]},
            records=[{"a": "not-an-int"}],
            include_total=False,
        )

    backend.metadata.insert.assert_not_called()
    backend.metadata.update.assert_not_called()


def test_create_existing_rolls_back_metadata_on_records_insert_failure(
    mock_client: MagicMock,
) -> None:
    """Same rule on the existing-resource path: if alter succeeds but
    records insert fails, `metadata.update` must NOT run — the metadata
    store stays at the previous schema version."""
    from datastore.core.exceptions import ServerError

    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = {
        "fields": [{"name": "a", "type": "integer"}]
    }
    # The first `client.query` call is the ALTER (success). The second
    # path is `insert_rows_json` which returns errors.
    mock_client.query.return_value.result.return_value = []
    mock_client.insert_rows_json.return_value = [
        {"index": 0, "errors": [{"reason": "invalid"}]}
    ]
    new_schema = {
        "fields": [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "string"},
        ]
    }

    with pytest.raises(ServerError):
        backend.create(
            "res-1",
            schema=new_schema,
            records=[{"a": 1, "b": object()}],  # invalid serialisation
            include_total=False,
        )

    backend.metadata.update.assert_not_called()
    backend.metadata.insert.assert_not_called()


def test_create_with_no_records_skips_insert(mock_client: MagicMock) -> None:
    """`records=None` or `records=[]` → no streaming insert call.
    Resource is declared (DDL + metadata) but no rows seeded."""
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None

    backend.create(
        "res-1",
        schema={"fields": [{"name": "a", "type": "integer"}]},
        records=None,
        include_total=False,
    )

    mock_client.insert_rows_json.assert_not_called()


def test_create_placeholder_mode_skips_ddl_and_metadata(
    mock_client: MagicMock,
) -> None:
    """No metadata store → no DDL, no metadata calls. Lets the unit
    suite run without GCP creds."""
    backend = _backend(mock_client)
    backend.metadata = None  # placeholder mode

    backend.create(
        "res-1",
        schema={"fields": [{"name": "a", "type": "integer"}]},
        records=None,
        include_total=False,
    )

    assert mock_client.query.call_count == 0
