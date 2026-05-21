"""Unit tests for the BigQuery write paths — DDL, records insert,
MERGE/UPDATE, and the `upsert` action dispatch.

A mocked `bigquery.Client` is plugged into a backend whose
`initialize()` we skip — no real BigQuery is contacted. The tests
pin SQL shape, parameter binding, error wrapping, and the
metadata/DDL atomicity contract.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from datastore.core.exceptions import (
    ConflictError,
    NotFoundError,
    ServerError,
    ValidationError,
)
from datastore.infrastructure.engines.bigquery.backend import BigQueryBackend
from datastore.infrastructure.engines.bigquery.lib import (
    merge_sql,
    update_sql,
)
from datastore.infrastructure.engines.bigquery.types import (
    bigquery_type,
    can_widen,
)

# --- fixtures --------------------------------------------------------------


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    # Default: every `client.query(...).result()` yields no rows.
    client.query.return_value.result.return_value = []
    return client


def _backend(client: MagicMock) -> BigQueryBackend:
    """Backend wired with a mocked client + config; skips `initialize()`."""
    b = BigQueryBackend(mode="rw")
    b.client = client
    b.config = MagicMock()
    b.config.BIGQUERY_PROJECT = "proj-1"
    b.config.BIGQUERY_DATASET = "ds-1"
    return b


# --- types.py --------------------------------------------------------------


def test_bigquery_type_resolves_canonical_and_falls_back_to_string() -> None:
    assert bigquery_type("integer") == "INT64"
    assert bigquery_type("datetime") == "TIMESTAMP"
    assert bigquery_type("object") == "JSON"
    assert bigquery_type("unknown-type") == "STRING"
    assert bigquery_type(None) == "STRING"


def test_can_widen_allows_supported_and_rejects_others() -> None:
    assert can_widen("INT64", "INT64") is True  # identity
    assert can_widen("INT64", "FLOAT64") is True  # supported widening
    assert can_widen("DATE", "TIMESTAMP") is True
    assert can_widen("INT64", "STRING") is False
    assert can_widen("FLOAT64", "INT64") is False  # narrowing


# --- DDL helpers -----------------------------------------------------------


def test_data_table_ref_uses_backticks(mock_client: MagicMock) -> None:
    """Backticks let CKAN UUID-like ids parse without further escaping."""
    assert _backend(mock_client)._data_table_ref("res-abc-123") == (
        "`proj-1.ds-1.res-abc-123`"
    )


def test_create_data_table_emits_create_table_if_not_exists(
    mock_client: MagicMock,
) -> None:
    backend = _backend(mock_client)
    backend._create_data_table(
        "res-1",
        {"fields": [
            {"name": "id", "type": "integer"},
            {"name": "label", "type": "string"},
        ]},
    )
    sql = mock_client.query.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS `proj-1.ds-1.res-1`" in sql
    # User columns.
    assert "`id` INT64" in sql
    assert "`label` STRING" in sql
    # System columns auto-prepended.
    assert "`_id` INT64" in sql
    assert "`_updated_at` TIMESTAMP" in sql


def test_alter_adds_new_columns_and_widens_supported_types(
    mock_client: MagicMock,
) -> None:
    backend = _backend(mock_client)
    old = {"fields": [{"name": "a", "type": "integer"}]}
    new = {"fields": [
        {"name": "a", "type": "number"},   # widen INT64 → FLOAT64
        {"name": "b", "type": "string"},   # add
    ]}

    backend._alter_data_table("res-1", old, new)

    sql = mock_client.query.call_args[0][0]
    assert "ALTER TABLE `proj-1.ds-1.res-1`" in sql
    assert "ADD COLUMN IF NOT EXISTS `b` STRING" in sql
    assert "ALTER COLUMN `a` SET DATA TYPE FLOAT64" in sql


def test_alter_rejects_unsupported_type_change_before_any_ddl(
    mock_client: MagicMock,
) -> None:
    """`integer` → `string` isn't a BigQuery-allowed widening — raise
    ConflictError up front, never issue partial DDL."""
    backend = _backend(mock_client)
    with pytest.raises(ConflictError, match="Cannot change column type"):
        backend._alter_data_table(
            "res-1",
            {"fields": [{"name": "a", "type": "integer"}]},
            {"fields": [{"name": "a", "type": "string"}]},
        )
    mock_client.query.assert_not_called()


# --- records insert --------------------------------------------------------


def test_insert_records_issues_dml_insert_with_rows_param(
    mock_client: MagicMock,
) -> None:
    """`_insert_records` runs a DML `INSERT INTO ... SELECT FROM
    UNNEST(@rows)` — not the streaming `insert_rows_json` API — so
    rows go straight to storage and subsequent MERGE/UPDATE can touch
    them immediately."""
    backend = _backend(mock_client)
    schema = {"fields": [
        {"name": "auction_id", "type": "integer"},
        {"name": "bidder_metadata", "type": "object"},
    ]}
    records = [
        {"auction_id": 144, "bidder_metadata": {"unit_id": "X"}},
        {"auction_id": 145, "bidder_metadata": {"unit_id": "Y"}},
    ]

    backend._insert_records("res-1", schema, records)

    # No streaming insert.
    mock_client.insert_rows_json.assert_not_called()
    # Single DML statement — `MAX(_id)` is inlined as a scalar
    # subquery, so we don't pay a separate round-trip for the probe.
    assert mock_client.query.call_count == 1
    sql_arg, kwargs = mock_client.query.call_args
    sql = sql_arg[0]
    assert sql.startswith("INSERT INTO `proj-1.ds-1.res-1` ")
    assert "FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS r" in sql
    # JSON columns extracted via PARSE_JSON inside SQL.
    assert "PARSE_JSON(JSON_QUERY(r, '$.bidder_metadata'))" in sql
    # System columns auto-injected — `_id` from the inlined MAX subquery
    # + ROW_NUMBER(), `_updated_at` from CURRENT_TIMESTAMP().
    assert "`_id`, `_updated_at`" in sql
    assert (
        "(SELECT IFNULL(MAX(`_id`), 0) FROM `proj-1.ds-1.res-1`) "
        "+ ROW_NUMBER() OVER ()"
    ) in sql
    assert "CURRENT_TIMESTAMP()" in sql
    # Only `@rows` is passed as a parameter now — no separate probe.
    params = {p.name: p.value for p in kwargs["job_config"].query_parameters}
    assert list(params.keys()) == ["rows"]
    assert json.loads(params["rows"]) == records


# --- error wrapping --------------------------------------------------------


def test_client_query_errors_surface_as_server_error_with_context(
    mock_client: MagicMock,
) -> None:
    """Raw BQ exceptions on `client.query` are wrapped as ServerError
    carrying op + resource_id — never leak as `RuntimeError`."""
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "Insufficient permissions"
    )
    backend = _backend(mock_client)
    with pytest.raises(ServerError) as exc:
        backend._create_data_table(
            "res-1", {"fields": [{"name": "a", "type": "integer"}]}
        )
    assert "CREATE TABLE" in str(exc.value)
    assert "'res-1'" in str(exc.value)


# --- create() orchestration -----------------------------------------------


def test_create_new_resource_runs_ddl_records_then_metadata_in_order(
    mock_client: MagicMock,
) -> None:
    """New resource path: CREATE TABLE → INSERT INTO → metadata.insert.
    Two BigQuery round-trips (`MAX(_id)` is inlined into the INSERT
    statement). Metadata is the last write so any failure earlier
    leaves it untouched."""
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None

    parent = MagicMock()
    parent.attach_mock(mock_client.query, "query")
    parent.attach_mock(backend.metadata.insert, "metadata_insert")

    backend.create(
        "res-1",
        schema={"fields": [{"name": "a", "type": "integer"}]},
        records=[{"a": 1}],
        include_total=False,
    )

    sql_calls = [c for c in parent.mock_calls if c[0] == "query"]
    assert len(sql_calls) == 2  # no separate MAX(_id) probe
    assert sql_calls[0].args[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert sql_calls[1].args[0].startswith("INSERT INTO ")
    # The inlined MAX(_id) subquery rides inside the INSERT itself.
    assert "SELECT IFNULL(MAX(`_id`), 0)" in sql_calls[1].args[0]
    # metadata.insert came last.
    assert parent.mock_calls[-1][0] == "metadata_insert"


def test_create_skips_metadata_when_records_insert_fails(
    mock_client: MagicMock,
) -> None:
    """Atomicity: a DML INSERT failure leaves metadata untouched.

    Create flow: CREATE TABLE → INSERT. Fail the second query (the
    INSERT); the first (CREATE TABLE) succeeds.
    """
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None

    success_job = MagicMock()
    success_job.result.return_value = []
    fail_job = MagicMock()
    fail_job.result.side_effect = RuntimeError("insert failed")
    mock_client.query.side_effect = [success_job, fail_job]

    with pytest.raises(ServerError):
        backend.create(
            "res-1",
            schema={"fields": [{"name": "a", "type": "integer"}]},
            records=[{"a": 1}],
            include_total=False,
        )
    backend.metadata.insert.assert_not_called()
    backend.metadata.update.assert_not_called()


def test_create_placeholder_mode_skips_everything(
    mock_client: MagicMock,
) -> None:
    """No metadata store → no DDL, no metadata calls. Lets the unit
    suite run without GCP creds."""
    backend = _backend(mock_client)
    backend.metadata = None

    backend.create(
        "res-1",
        schema={"fields": [{"name": "a", "type": "integer"}]},
        records=[{"a": 1}],
        include_total=False,
    )
    mock_client.query.assert_not_called()


# --- merge_sql / update_sql (lib) ------------------------------------------


def test_merge_sql_renders_typed_extractors_on_match_update_no_match_insert() -> None:
    sql = merge_sql(
        "`p.d.r`",
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "label", "type": "string"},
                {"name": "meta", "type": "object"},
            ],
            "primaryKey": ["id"],
        },
    )
    assert sql.startswith("MERGE `p.d.r` T")
    assert "CAST(JSON_VALUE(r, '$.id') AS INT64) AS `id`" in sql
    assert "JSON_VALUE(r, '$.label') AS `label`" in sql
    assert "PARSE_JSON(JSON_QUERY(r, '$.meta')) AS `meta`" in sql
    # USING attaches ROW_NUMBER() as _rn for auto-`_id` on NOT MATCHED.
    assert "ROW_NUMBER() OVER () AS _rn" in sql
    assert "ON T.`id` = S.`id`" in sql
    # WHEN MATCHED only fires when some non-PK column actually differs
    # — `_updated_at` advances on real changes, not on no-op upserts.
    # Diff predicate uses IS DISTINCT FROM (NULL-safe) for scalars and
    # TO_JSON_STRING(...) wrap for JSON columns.
    assert (
        "WHEN MATCHED AND ("
        "T.`label` IS DISTINCT FROM S.`label` OR "
        "TO_JSON_STRING(T.`meta`) IS DISTINCT FROM TO_JSON_STRING(S.`meta`)"
        ")"
    ) in sql
    assert "T.`label` = S.`label`" in sql
    assert "T.`_updated_at` = CURRENT_TIMESTAMP()" in sql
    # NOT MATCHED inserts system columns + user columns. `_id` is
    # `(SELECT MAX(_id) FROM tbl) + S._rn` — inlined to avoid a
    # separate probe round-trip.
    assert (
        "WHEN NOT MATCHED THEN INSERT (`_id`, `_updated_at`, "
        "`id`, `label`, `meta`)"
    ) in sql
    assert (
        "(SELECT IFNULL(MAX(`_id`), 0) FROM `p.d.r`) + S._rn"
    ) in sql


def test_update_sql_renders_dml_update_keyed_on_primary_key() -> None:
    sql = update_sql(
        "`p.d.r`",
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "label", "type": "string"},
            ],
            "primaryKey": ["id"],
        },
    )
    assert sql.startswith("UPDATE `p.d.r` T ")
    assert "T.`label` = S.`label`" in sql
    # `_updated_at` is always bumped, even when there are non-PK fields.
    assert "T.`_updated_at` = CURRENT_TIMESTAMP()" in sql
    assert "WHERE T.`id` = S.`id`" in sql
    assert "MERGE" not in sql  # plain DML, not MERGE


def test_merge_and_update_sql_reject_missing_primary_key() -> None:
    schema = {"fields": [{"name": "id", "type": "integer"}]}
    with pytest.raises(ValueError, match="primaryKey"):
        merge_sql("`p.d.r`", schema)
    with pytest.raises(ValueError, match="primaryKey"):
        update_sql("`p.d.r`", schema)


# --- upsert() dispatch ----------------------------------------------------


def _backend_with_schema(
    mock_client: MagicMock, schema: dict[str, Any]
) -> BigQueryBackend:
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = schema
    return backend


def test_upsert_method_upsert_issues_merge_with_rows_param(
    mock_client: MagicMock,
) -> None:
    backend = _backend_with_schema(
        mock_client,
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "label", "type": "string"},
            ],
            "primaryKey": ["id"],
        },
    )
    records = [{"id": 1, "label": "x"}, {"id": 2, "label": "y"}]

    backend.upsert("res-1", records, method="upsert", include_total=False)

    sql, kwargs = mock_client.query.call_args
    assert "MERGE `proj-1.ds-1.res-1` T" in sql[0]
    params = {p.name: p.value for p in kwargs["job_config"].query_parameters}
    assert json.loads(params["rows"]) == records


def test_upsert_method_insert_issues_dml_insert(
    mock_client: MagicMock,
) -> None:
    """`method='insert'` runs DML `INSERT INTO ... SELECT FROM UNNEST`,
    not the streaming insert API — same path as `_insert_records` on
    the create flow."""
    backend = _backend_with_schema(
        mock_client,
        {"fields": [{"name": "id", "type": "integer"}], "primaryKey": ["id"]},
    )

    backend.upsert("res-1", [{"id": 1}], method="insert", include_total=False)

    mock_client.insert_rows_json.assert_not_called()
    sql = mock_client.query.call_args[0][0]
    assert sql.startswith("INSERT INTO `proj-1.ds-1.res-1` ")
    assert "FROM UNNEST(JSON_QUERY_ARRAY(@rows)) AS r" in sql


def test_upsert_method_update_issues_dml_update(
    mock_client: MagicMock,
) -> None:
    backend = _backend_with_schema(
        mock_client,
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "label", "type": "string"},
            ],
            "primaryKey": ["id"],
        },
    )
    mock_client.query.return_value.num_dml_affected_rows = 2

    backend.upsert(
        "res-1",
        [{"id": 1, "label": "x"}, {"id": 2, "label": "y"}],
        method="update",
        include_total=False,
    )

    sql = mock_client.query.call_args[0][0]
    assert sql.startswith("UPDATE `proj-1.ds-1.res-1` T ")
    assert "WHERE T.`id` = S.`id`" in sql


def test_upsert_method_update_raises_not_found_when_pk_missing(
    mock_client: MagicMock,
) -> None:
    """Affected-row count < input row count → some PKs didn't match.
    DML UPDATE silently no-ops on misses; we surface NotFoundError."""
    backend = _backend_with_schema(
        mock_client,
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "label", "type": "string"},
            ],
            "primaryKey": ["id"],
        },
    )
    mock_client.query.return_value.num_dml_affected_rows = 1  # 2 missing

    with pytest.raises(NotFoundError, match="2 of 3"):
        backend.upsert(
            "res-1",
            [
                {"id": 1, "label": "x"},
                {"id": 2, "label": "y"},
                {"id": 3, "label": "z"},
            ],
            method="update",
            include_total=False,
        )


def test_upsert_undeclared_resource_raises_not_found(
    mock_client: MagicMock,
) -> None:
    """`upsert` before `create` → NotFoundError. Metadata store is the
    source of truth for whether a resource exists."""
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None

    with pytest.raises(NotFoundError, match="not declared"):
        backend.upsert(
            "ghost", [{"a": 1}], method="upsert", include_total=False
        )


def test_upsert_missing_primary_key_raises_validation(
    mock_client: MagicMock,
) -> None:
    """`upsert`/`update` need a primaryKey — ValueError from the SQL
    helpers is re-raised as ValidationError, never reaches BigQuery."""
    backend = _backend_with_schema(
        mock_client,
        {"fields": [{"name": "id", "type": "integer"}]},  # no primaryKey
    )
    with pytest.raises(ValidationError, match="primaryKey"):
        backend.upsert(
            "res-1", [{"id": 1}], method="upsert", include_total=False
        )
    mock_client.query.assert_not_called()


def test_upsert_unknown_method_raises_validation(
    mock_client: MagicMock,
) -> None:
    backend = _backend_with_schema(
        mock_client,
        {"fields": [{"name": "id", "type": "integer"}], "primaryKey": ["id"]},
    )
    with pytest.raises(ValidationError, match="unknown upsert method"):
        backend.upsert(
            "res-1", [], method="merge", include_total=False  # bogus
        )


def test_upsert_translates_bigquery_scalar_subquery_error_to_duplicate_pk(
    mock_client: MagicMock,
) -> None:
    """When `records` contain duplicate PK tuples, BigQuery's MERGE
    fails with 'Scalar subquery produced more than one element'. The
    backend translates that into a clear ValidationError naming the
    actual cause."""
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "400 Scalar subquery produced more than one element; reason: "
        "invalidQuery, location: query"
    )
    backend = _backend_with_schema(
        mock_client,
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "label", "type": "string"},
            ],
            "primaryKey": ["id"],
        },
    )

    with pytest.raises(ValidationError) as exc:
        backend.upsert(
            "res-1",
            [{"id": 1, "label": "x"}, {"id": 1, "label": "y"}],  # dup PK
            method="upsert",
            include_total=False,
        )

    msg = str(exc.value)
    assert "duplicated" in msg.lower()
    assert "primary key" in msg.lower()


def test_update_translates_bigquery_scalar_subquery_error_to_duplicate_pk(
    mock_client: MagicMock,
) -> None:
    """Same translation on the DML UPDATE path."""
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "400 Scalar subquery produced more than one element"
    )
    backend = _backend_with_schema(
        mock_client,
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "label", "type": "string"},
            ],
            "primaryKey": ["id"],
        },
    )

    with pytest.raises(ValidationError, match="duplicate"):
        backend.upsert(
            "res-1",
            [{"id": 1, "label": "x"}, {"id": 1, "label": "y"}],
            method="update",
            include_total=False,
        )


def test_insert_translates_bigquery_bad_double_value_to_type_mismatch(
    mock_client: MagicMock,
) -> None:
    """BigQuery's `Bad double value: <v>` (raised when a record sends
    a non-numeric string for a `number` column) is translated to a
    clear ValidationError naming the bad value and the expected type.

    The create-flow runs CREATE TABLE → INSERT INTO — only the INSERT
    (2nd call) should fail. The first (CREATE TABLE) succeeds.
    """
    success_job = MagicMock()
    success_job.result.return_value = []
    fail_job = MagicMock()
    fail_job.result.side_effect = RuntimeError(
        "400 Bad double value: jk; reason: invalidQuery, location: query"
    )
    mock_client.query.side_effect = [success_job, fail_job]
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None

    with pytest.raises(ValidationError) as exc:
        backend.create(
            "res-1",
            schema={"fields": [{"name": "price", "type": "number"}]},
            records=[{"price": "jk"}],
            include_total=False,
        )
    msg = str(exc.value)
    assert "'jk'" in msg
    assert "number" in msg


def test_upsert_translates_bigquery_bad_int64_value_to_type_mismatch(
    mock_client: MagicMock,
) -> None:
    """`Bad int64 value: …` on the MERGE path becomes a ValidationError
    that says 'integer'."""
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "400 Bad int64 value: not-a-number; reason: invalidQuery"
    )
    backend = _backend_with_schema(
        mock_client,
        {"fields": [{"name": "id", "type": "integer"}], "primaryKey": ["id"]},
    )

    with pytest.raises(ValidationError) as exc:
        backend.upsert(
            "res-1", [{"id": "not-a-number"}], method="upsert",
            include_total=False,
        )
    assert "'not-a-number'" in str(exc.value)
    assert "integer" in str(exc.value)


def test_translate_invalid_timestamp_value(mock_client: MagicMock) -> None:
    """`Invalid timestamp: …` → ValidationError mentioning 'timestamp'."""
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "400 Invalid timestamp: 2025-99-99; reason: invalidQuery"
    )
    backend = _backend_with_schema(
        mock_client,
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "ts", "type": "datetime"},
            ],
            "primaryKey": ["id"],
        },
    )

    with pytest.raises(ValidationError) as exc:
        backend.upsert(
            "res-1", [{"id": 1, "ts": "2025-99-99"}], method="upsert",
            include_total=False,
        )
    assert "'2025-99-99'" in str(exc.value)
    assert "timestamp" in str(exc.value)


def test_translate_could_not_cast_literal_error(mock_client: MagicMock) -> None:
    """`Could not cast literal '...' to type <BQ_TYPE>` — alternative
    BigQuery phrasing for the same coercion failure as `Bad <type>
    value`. Should produce the same friendly message."""
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "400 Could not cast literal 'jk' to type INT64; reason: invalidQuery"
    )
    backend = _backend_with_schema(
        mock_client,
        {"fields": [{"name": "id", "type": "integer"}], "primaryKey": ["id"]},
    )
    with pytest.raises(ValidationError) as exc:
        backend.upsert(
            "res-1", [{"id": "jk"}], method="upsert", include_total=False,
        )
    msg = str(exc.value)
    assert "'jk'" in msg
    assert "integer" in msg


def test_translate_could_not_parse_as_type_error(
    mock_client: MagicMock,
) -> None:
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "400 Could not parse 'abc' as FLOAT64; reason: invalidQuery"
    )
    backend = _backend_with_schema(
        mock_client,
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "price", "type": "number"},
            ],
            "primaryKey": ["id"],
        },
    )
    with pytest.raises(ValidationError) as exc:
        backend.upsert(
            "res-1", [{"id": 1, "price": "abc"}],
            method="upsert", include_total=False,
        )
    msg = str(exc.value)
    assert "'abc'" in msg
    assert "number" in msg


def test_translate_value_out_of_range(mock_client: MagicMock) -> None:
    """Numeric value that parses but exceeds the column type's range
    → ValidationError mentioning out-of-range."""
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "400 Value out of range for INT64: 99999999999999999999; "
        "reason: invalidQuery"
    )
    backend = _backend_with_schema(
        mock_client,
        {"fields": [{"name": "id", "type": "integer"}], "primaryKey": ["id"]},
    )
    with pytest.raises(ValidationError) as exc:
        backend.upsert(
            # Use string to avoid orjson's 64-bit int limit — the test
            # checks the BigQuery-side error, not orjson encoding.
            "res-1", [{"id": "99999999999999999999"}],
            method="upsert", include_total=False,
        )
    msg = str(exc.value)
    assert "out of range" in msg
    assert "integer" in msg


def test_translate_bad_numeric_value(mock_client: MagicMock) -> None:
    """NUMERIC / BIGNUMERIC (e.g., after widening INT64 → NUMERIC) get
    the same `number` friendly name."""
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "400 Bad NUMERIC value: not-a-num; reason: invalidQuery"
    )
    backend = _backend_with_schema(
        mock_client,
        {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "amount", "type": "number"},
            ],
            "primaryKey": ["id"],
        },
    )
    with pytest.raises(ValidationError) as exc:
        backend.upsert(
            "res-1", [{"id": 1, "amount": "not-a-num"}],
            method="upsert", include_total=False,
        )
    assert "'not-a-num'" in str(exc.value)
    assert "number" in str(exc.value)


# --- _updated_at toggle ---------------------------------------------------


def test_sql_helpers_omit_updated_at_when_flag_disabled() -> None:
    """`Config.INCLUDE_UPDATED_AT=False` drops `_updated_at` from every
    write path: CREATE TABLE, INSERT, MERGE (both branches), UPDATE."""
    schema = {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "label", "type": "string"},
        ],
        "primaryKey": ["id"],
    }
    from datastore.infrastructure.engines.bigquery.lib import (
        column_defs,
        insert_sql,
        merge_sql,
        update_sql,
    )

    # CREATE TABLE
    cols = column_defs(schema, include_updated_at=False)
    assert "`_id` INT64" in cols
    assert not any("_updated_at" in c for c in cols)

    # INSERT
    ins = insert_sql("`p.d.r`", schema, include_updated_at=False)
    assert "`_id`, `id`, `label`" in ins  # no _updated_at in col list
    assert "CURRENT_TIMESTAMP()" not in ins
    assert "_updated_at" not in ins

    # MERGE — neither MATCHED nor NOT MATCHED touches `_updated_at`.
    mer = merge_sql("`p.d.r`", schema, include_updated_at=False)
    assert "_updated_at" not in mer
    assert "CURRENT_TIMESTAMP()" not in mer
    # The MATCHED branch still fires on real diffs, just without the
    # timestamp bump.
    assert "WHEN MATCHED AND (T.`label` IS DISTINCT FROM S.`label`)" in mer

    # UPDATE — SET only carries the user column edit.
    upd = update_sql("`p.d.r`", schema, include_updated_at=False)
    assert "_updated_at" not in upd
    assert "SET T.`label` = S.`label`" in upd


def test_update_sql_rejects_all_pk_schema_when_timestamp_disabled() -> None:
    """With `_updated_at` disabled, an all-PK schema has nothing to
    SET — raise so the backend can surface a clear ValidationError."""
    from datastore.infrastructure.engines.bigquery.lib import update_sql

    schema = {
        "fields": [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "string"},
        ],
        "primaryKey": ["a", "b"],
    }
    with pytest.raises(ValueError, match="nothing to SET"):
        update_sql("`p.d.r`", schema, include_updated_at=False)


def test_backend_propagates_config_flag_into_ddl(
    mock_client: MagicMock,
) -> None:
    """`BigQueryBackend._include_updated_at` reads `INCLUDE_UPDATED_AT`
    off the attached config; `_create_data_table` honours it."""
    backend = _backend(mock_client)
    backend.config.INCLUDE_UPDATED_AT = False

    backend._create_data_table(
        "res-1",
        {"fields": [{"name": "id", "type": "integer"}]},
    )

    sql = mock_client.query.call_args[0][0]
    assert "`_id` INT64" in sql
    assert "_updated_at" not in sql


# --- info() ---------------------------------------------------------------


def test_info_returns_stored_schema_total_and_primary_key(
    mock_client: MagicMock,
) -> None:
    """`info()` reads the Frictionless schema from `_table_metadata`
    and counts rows via `COUNT(*)` on the data table. `meta` exposes
    the schema's primaryKey under `primary_key`."""
    schema = {
        "fields": [
            {"name": "auction_id", "type": "integer"},
            {"name": "product_code", "type": "string"},
        ],
        "primaryKey": ["auction_id", "product_code"],
    }
    backend = _backend_with_schema(mock_client, schema)
    count_row = MagicMock()
    count_row.__getitem__.side_effect = lambda k: 18420 if k == "n" else None
    mock_client.query.return_value.result.return_value = [count_row]

    result = backend.info("balancing_auction_results_2025")

    sql = mock_client.query.call_args[0][0]
    assert sql == (
        "SELECT COUNT(*) AS n FROM "
        "`proj-1.ds-1.balancing_auction_results_2025`"
    )
    assert result.schema == schema
    assert result.meta["resource_id"] == "balancing_auction_results_2025"
    assert result.meta["total"] == 18420
    assert result.meta["primary_key"] == ["auction_id", "product_code"]


def test_info_raises_not_found_for_undeclared_resource(
    mock_client: MagicMock,
) -> None:
    """No metadata row → NotFoundError. The data table may exist
    out-of-band but the engine treats `_table_metadata` as the
    declaration source of truth."""
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None

    with pytest.raises(NotFoundError, match="not declared"):
        backend.info("ghost")
    # No COUNT runs when the resource isn't declared.
    mock_client.query.assert_not_called()


def test_info_returns_total_zero_when_count_fails(
    mock_client: MagicMock,
) -> None:
    """If the data table is missing while metadata exists (inconsistent
    state from manual cleanup), `info` reports total=0 rather than
    500-ing the call — the schema is still informative on its own."""
    schema = {
        "fields": [{"name": "id", "type": "integer"}],
        "primaryKey": ["id"],
    }
    backend = _backend_with_schema(mock_client, schema)
    mock_client.query.return_value.result.side_effect = RuntimeError(
        "404 Not found: Table proj-1:ds-1.res-1"
    )

    result = backend.info("res-1")

    assert result.meta["total"] == 0
    assert result.schema == schema


def test_info_placeholder_mode_returns_stub(mock_client: MagicMock) -> None:
    """No metadata store → return an empty stub so the unit suite can
    exercise the call path without GCP creds."""
    backend = _backend(mock_client)
    backend.metadata = None

    result = backend.info("res-1")

    assert result.schema == {"fields": []}
    assert result.meta == {"resource_id": "res-1", "total": 0}
    mock_client.query.assert_not_called()


# --- search() SQL builders -------------------------------------------------


def test_parse_sort_validates_and_defaults_direction() -> None:
    from datastore.infrastructure.engines.bigquery.search import parse_sort

    pairs = parse_sort("a, b desc, c asc", {"a", "b", "c"})
    assert pairs == [("a", "ASC"), ("b", "DESC"), ("c", "ASC")]

    with pytest.raises(ValueError, match="unknown column 'ghost'"):
        parse_sort("ghost desc", {"a"})
    with pytest.raises(ValueError, match="direction"):
        parse_sort("a sideways", {"a"})


def test_build_search_renders_full_param_set() -> None:
    """Every CKAN datastore_search param lands in the rendered SQL —
    filters bind as parameters (no inlining), `q` becomes a row-wide
    `SEARCH`, sort + projection are validated identifiers, and limit /
    offset close the statement."""
    from datastore.infrastructure.engines.bigquery.search import build_search

    schema = {
        "fields": [
            {"name": "auction_id", "type": "integer"},
            {"name": "product_code", "type": "string"},
            {"name": "accepted", "type": "boolean"},
        ],
        "primaryKey": ["auction_id"],
    }

    sql, params, projected = build_search(
        table_ref="`p.d.r`",
        schema=schema,
        include_updated_at=True,
        fields=["auction_id", "product_code"],
        filters={"product_code": "DCL", "accepted": True},
        q="apple",
        distinct=False,
        sort="auction_id desc",
        limit=100,
        offset=25,
    )

    assert sql.startswith("SELECT `auction_id`, `product_code` FROM `p.d.r` AS t")
    assert "WHERE `product_code` = @f0 AND `accepted` = @f1" in sql
    assert "SEARCH(t, @f2)" in sql
    assert "ORDER BY `auction_id` DESC" in sql
    assert sql.rstrip().endswith("LIMIT 100 OFFSET 25")
    # Parameter types track the schema (STRING / BOOL / STRING for q).
    by_name = {p.name: p for p in params}
    assert by_name["f0"].type_ == "STRING"
    assert by_name["f0"].value == "DCL"
    assert by_name["f1"].type_ == "BOOL"
    assert by_name["f1"].value is True
    assert by_name["f2"].type_ == "STRING"
    assert by_name["f2"].value == "apple"
    # Result schema reflects the projection, in user-specified order.
    assert [f["name"] for f in projected["fields"]] == [
        "auction_id", "product_code",
    ]


def test_build_search_in_clause_for_list_filter() -> None:
    """Filter value as a list → `col IN UNNEST(@p)` with an ARRAY param."""
    from datastore.infrastructure.engines.bigquery.search import build_search

    schema = {"fields": [{"name": "id", "type": "integer"}]}
    sql, params, _ = build_search(
        table_ref="`p.d.r`",
        schema=schema,
        include_updated_at=False,
        fields=None,
        filters={"id": [1, 2, 3]},
        q=None,
        distinct=False,
        sort=None,
        limit=10,
        offset=0,
    )
    assert "`id` IN UNNEST(@f0)" in sql
    assert params[0].array_type == "INT64"
    assert params[0].values == [1, 2, 3]


def test_build_search_default_sort_is_id_asc() -> None:
    """No `sort` → `_id ASC`. `_id` is always projected by default so
    this is well-defined."""
    from datastore.infrastructure.engines.bigquery.search import build_search

    schema = {"fields": [{"name": "x", "type": "integer"}]}
    sql, _, _ = build_search(
        table_ref="`p.d.r`",
        schema=schema,
        include_updated_at=False,
        fields=None,
        filters=None,
        q=None,
        distinct=False,
        sort=None,
        limit=10,
        offset=0,
    )
    assert "ORDER BY `_id` ASC" in sql


def test_build_search_rejects_unknown_columns() -> None:
    """`fields`, `sort`, `filters`, and dict-`q` all validate column
    names against the schema — closing the SQL-injection vector on the
    identifier-inlined slots."""
    from datastore.infrastructure.engines.bigquery.search import build_search

    schema = {"fields": [{"name": "a", "type": "integer"}]}
    kwargs = dict(
        table_ref="`p.d.r`",
        schema=schema,
        include_updated_at=False,
        filters=None, q=None, distinct=False, sort=None,
        limit=10, offset=0,
    )
    with pytest.raises(ValueError, match="fields references unknown"):
        build_search(fields=["ghost"], **kwargs)
    with pytest.raises(ValueError, match="sort references unknown"):
        build_search(fields=None, **{**kwargs, "sort": "ghost asc"})
    with pytest.raises(ValueError, match="filters references unknown"):
        build_search(fields=None, **{**kwargs, "filters": {"ghost": 1}})
    with pytest.raises(ValueError, match="q references unknown"):
        build_search(fields=None, **{**kwargs, "q": {"ghost": "x"}})


def test_build_search_rejects_filters_on_json_columns() -> None:
    """JSON/array/geojson columns have no clean equality in BQ — reject
    early so the caller gets a 400 rather than a 500 from BigQuery."""
    from datastore.infrastructure.engines.bigquery.search import build_search

    schema = {"fields": [{"name": "blob", "type": "object"}]}
    with pytest.raises(ValueError, match="JSON/array/geojson"):
        build_search(
            table_ref="`p.d.r`",
            schema=schema,
            include_updated_at=False,
            fields=None,
            filters={"blob": {"k": "v"}},
            q=None, distinct=False, sort=None,
            limit=10, offset=0,
        )


def test_needs_count_query_only_when_filtering_or_distinct() -> None:
    """Unfiltered + non-distinct search → backend takes the cheap
    `__TABLES__`/`_count_rows` path; otherwise must run a real COUNT."""
    from datastore.infrastructure.engines.bigquery.search import (
        needs_count_query,
    )

    assert needs_count_query(filters=None, q=None, distinct=False) is False
    assert needs_count_query(filters={"a": 1}, q=None, distinct=False) is True
    assert needs_count_query(filters=None, q="x", distinct=False) is True
    assert needs_count_query(filters=None, q=None, distinct=True) is True


# --- backend.search() orchestration ---------------------------------------


def test_search_returns_projection_schema_and_lazy_rows(
    mock_client: MagicMock,
) -> None:
    """End-to-end through `BigQueryBackend.search`: builds the SELECT,
    submits the search + count jobs (filtered → count is a real query,
    not `_count_rows`), yields tuples in projection order, and pipes
    the projected schema back to the streaming writer."""
    schema = {
        "fields": [
            {"name": "auction_id", "type": "integer"},
            {"name": "product_code", "type": "string"},
        ],
        "primaryKey": ["auction_id"],
    }
    backend = _backend_with_schema(mock_client, schema)

    # Distinct jobs for search + count; each yields its own .result().
    search_job = MagicMock()
    search_row = MagicMock()
    search_row.values.return_value = (1, "DCL")
    search_job.result.return_value = iter([search_row])

    count_job = MagicMock()
    count_row = MagicMock()
    count_row.__getitem__.side_effect = lambda k: 1 if k == "n" else None
    count_job.result.return_value = [count_row]

    mock_client.query.side_effect = [count_job, search_job]

    result = backend.search(
        resource_id="res-1",
        filters={"product_code": "DCL"},
        q=None,
        distinct=False, plain=True, language="english",
        limit=100, offset=0,
        fields=["auction_id", "product_code"],
        sort=None,
        include_total=True,
    )

    # Count query fires first (queued before search so both run in
    # parallel; this caller awaits search before count).
    assert mock_client.query.call_count == 2
    count_sql = mock_client.query.call_args_list[0][0][0]
    search_sql = mock_client.query.call_args_list[1][0][0]
    assert count_sql.startswith("SELECT COUNT(*) AS n FROM (")
    assert search_sql.startswith(
        "SELECT `auction_id`, `product_code` FROM `proj-1.ds-1.res-1` AS t"
    )
    assert result.total == 1
    # `records` is a generator — assert lazy by exhausting it once.
    rows = list(result.records)
    assert rows == [(1, "DCL")]
    # Projected schema is what the writer needs to label columns.
    assert [f["name"] for f in result.schema["fields"]] == [
        "auction_id", "product_code",
    ]


def test_search_unfiltered_uses_cheap_row_count(
    mock_client: MagicMock,
) -> None:
    """No filters, no q, no distinct → backend skips the filtered
    COUNT subquery and falls back to the cheap row-count helper. Two
    `client.query` calls land in this order: (1) the search SELECT,
    (2) `_count_rows`'s `SELECT COUNT(*) FROM target`. Neither wraps
    the data table in a subquery."""
    schema = {"fields": [{"name": "a", "type": "integer"}]}
    backend = _backend_with_schema(mock_client, schema)

    search_job = MagicMock()
    search_job.result.return_value = iter([])
    count_job = MagicMock()
    cnt = MagicMock()
    cnt.__getitem__.side_effect = lambda k: 42 if k == "n" else None
    count_job.result.return_value = [cnt]

    mock_client.query.side_effect = [search_job, count_job]

    result = backend.search(
        resource_id="res-1",
        filters=None, q=None, distinct=False, plain=True,
        language="english", limit=10, offset=0,
        fields=None, sort=None, include_total=True,
    )

    assert mock_client.query.call_count == 2
    sqls = [call.args[0] for call in mock_client.query.call_args_list]
    # `_updated_at` rides along in default projection because the
    # MagicMock config returns truthy for `INCLUDE_UPDATED_AT`.
    assert sqls[0].startswith(
        "SELECT `_id`, `a`, `_updated_at` FROM `proj-1.ds-1.res-1` AS t"
    )
    assert sqls[1] == (
        "SELECT COUNT(*) AS n FROM `proj-1.ds-1.res-1`"
    )
    # No filtered count subquery anywhere.
    assert not any("FROM (SELECT" in s for s in sqls)
    assert result.total == 42


def test_search_raises_not_found_for_undeclared_resource(
    mock_client: MagicMock,
) -> None:
    backend = _backend(mock_client)
    backend.metadata = MagicMock()
    backend.metadata.get.return_value = None

    with pytest.raises(NotFoundError, match="not declared"):
        backend.search(
            resource_id="ghost",
            filters=None, q=None, distinct=False, plain=True,
            language="english", limit=10, offset=0,
            fields=None, sort=None, include_total=False,
        )
    mock_client.query.assert_not_called()


def test_search_translates_builder_error_to_validation_error(
    mock_client: MagicMock,
) -> None:
    """Builder `ValueError` (unknown column, etc.) becomes a clean
    `ValidationError` — caller gets 400, never reaches BigQuery."""
    schema = {"fields": [{"name": "a", "type": "integer"}]}
    backend = _backend_with_schema(mock_client, schema)

    with pytest.raises(ValidationError, match="unknown column"):
        backend.search(
            resource_id="res-1",
            filters=None, q=None, distinct=False, plain=True,
            language="english", limit=10, offset=0,
            fields=["ghost"], sort=None, include_total=False,
        )
    mock_client.query.assert_not_called()
