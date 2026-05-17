"""Reusable Pydantic validators and schema parts.
"""

from __future__ import annotations

from typing import Annotated, Any

from datastore.core.constants import POSTGRES_TYPES
from pydantic import BaseModel, BeforeValidator, ConfigDict

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


# --- reusable Annotated types ------------------------------------------------
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
