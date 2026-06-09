"""Regression tests for the streaming row writers in `services.streaming`.

Targets the BigQuery scalar types orjson refuses by default:

  - `NUMERIC` / `BIGNUMERIC` → `decimal.Decimal`
  - `BYTES`                  → `bytes`

The fix lives in `_json_default` (passed via `orjson.dumps(default=...)`).
Without it, the stream crashes mid-row with
`TypeError: Type is not JSON serializable: decimal.Decimal`.
"""

from __future__ import annotations

import json
from decimal import Decimal

from datastore.services.streaming import (
    _records_array_array,
    _records_object_array,
)


def _join(parts: list[bytes]) -> str:
    """Stitch the yielded chunks together as a UTF-8 string."""
    return b"".join(parts).decode("utf-8")


def test_records_object_array_serialises_decimal_and_bytes() -> None:
    """Rows with NUMERIC (Decimal) + BYTES values must stream without
    blowing up; Decimal lands as a JSON number; bytes is base64-encoded."""
    rows = iter(
        [
            ("DCL", Decimal("47.82"), b"\x00\xff"),
            ("DCH", Decimal("0.00000000000000000000000000000000000001"), b"abc"),
        ]
    )
    columns = ["product_code", "clearing_price_gbp_per_mwh", "signature"]

    body = _join(list(_records_object_array(columns, rows)))
    records = json.loads(body)

    assert records == [
        {
            "product_code": "DCL",
            "clearing_price_gbp_per_mwh": 47.82,
            "signature": "AP8=",                 # b64("\x00\xff")
        },
        {
            "product_code": "DCH",
            "clearing_price_gbp_per_mwh": 1e-38,
            "signature": "YWJj",                 # b64(b"abc")
        },
    ]
    # Confirm the type, not just the value — `47.82 == "47.82"` would be
    # False but the eq above could pass with both as strings if the field
    # ever flipped back. Pin the JSON number contract explicitly.
    assert isinstance(records[0]["clearing_price_gbp_per_mwh"], float)


def test_records_array_array_serialises_decimal_and_bytes() -> None:
    """Same coverage for `records_format=lists`."""
    rows = iter([("DCL", Decimal("47.82"), b"\x00\xff")])

    body = _join(list(_records_array_array(rows)))
    records = json.loads(body)

    assert records == [["DCL", 47.82, "AP8="]]
    assert isinstance(records[0][1], float)


def test_unsupported_type_still_raises() -> None:
    """We don't want the default to silently swallow new unknown types —
    bail loudly so the bug surfaces in tests instead of in production."""

    class Mystery:
        pass

    rows = iter([(Mystery(),)])
    try:
        list(_records_array_array(rows))
    except TypeError as e:
        assert "Mystery" in str(e)
    else:
        raise AssertionError("expected TypeError for unsupported type")
