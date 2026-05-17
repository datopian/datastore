"""Reusable Pydantic validators and schema parts.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict

from datastore.core.constants import POSTGRES_TYPES

# --- validator functions -----------------------------------------------------


def to_list(value: Any) -> list[str] | None:
    """Coerce `None | str | list[str]` to `list[str] | None`.
    Equivalent to CKAN's `list_of_strings_or_string`.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return list(value)
    raise ValueError("must be a string or list of strings")


def check_postgres_type(value: Any) -> str | None:
    """Pass through `None`; otherwise resolve to a canonical PostgreSQL type.

    Raises ValueError when `value` isn't a string or isn't a recognised
    Postgres type / alias in `POSTGRES_TYPES`.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("type must be a string")
    key = " ".join(value.strip().lower().split())
    canonical = POSTGRES_TYPES.get(key)
    if canonical is None:
        canonicals = sorted(set(POSTGRES_TYPES.values()))
        raise ValueError(
            f"unknown field type '{value}'; "
            f"expected one of {canonicals} or a PostgreSQL alias"
        )
    return canonical


def to_json_object(value: Any) -> dict[str, Any] | None:
    """Decode a query-string JSON object. Pass-through dicts; reject the rest."""
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise ValueError("must be a JSON object")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("must be a JSON object")
    return parsed


def to_str_or_json_object(value: Any) -> str | dict[str, Any] | None:
    """`q`-style param: plain string, or JSON object when the value starts with `{`."""
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        if value.lstrip().startswith("{"):
            return to_json_object(value)
        return value
    raise ValueError("must be a string or JSON object")


def to_csv_list(value: Any) -> list[str] | None:
    """Coerce a comma-separated string to a list; pass-through lists."""
    if value is None or value == "":
        return None
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        parts = [token.strip() for token in value.split(",")]
        return [p for p in parts if p]
    raise ValueError("must be a comma-separated string or list of strings")


# --- reusable Annotated types ------------------------------------------------
# The parser functions above (`to_json_object`, `to_str_or_json_object`,
# `to_csv_list`) are invoked directly at the service boundary; they don't
# need an Annotated wrapper because FastAPI's `Annotated[Model, Query()]`
# only accepts scalar fields on the model — see DatastoreSearchRequest.
StringOrList = Annotated[list[str] | None, BeforeValidator(to_list)]
PostgresType = Annotated[str | None, BeforeValidator(check_postgres_type)]


# --- reusable schema parts ---------------------------------------------------
class FieldSpec(BaseModel):
    """A single column definition.

    - `id`   — SQL-safe identifier (required).
    - `type` — optional PostgreSQL type; aliases (`integer`, `bigint`, `varchar`,
      …) are normalised to their canonical Postgres form.
    - `info` — optional free-form data dictionary (title, description, unit, …).
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    type: PostgresType = None
    info: dict[str, Any] | None = None
