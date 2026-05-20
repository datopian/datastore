from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENGINES_DIR = (
    Path(__file__).resolve().parent.parent / "infrastructure" / "engines"
)


def _available_engines() -> set[str]:
    """Engine names = `infrastructure/engines/<name>/` directories on disk.

    Lets `DATASTORE_ENGINE` auto-accept any locally-present engine package
    without listing them statically. Backends gitignored for local dev
    (e.g. a test engine) work locally and stay invisible upstream.
    """
    if not _ENGINES_DIR.is_dir():
        return set()
    return {
        p.name for p in _ENGINES_DIR.iterdir()
        if p.is_dir()
        and not p.name.startswith(("_", "."))
        and p.name != "__pycache__"
    }


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Application metadata
    APP_MESSAGE: str = Field(
        default="Datastore API",
        description="Welcome message shown on the root endpoint",
    )

    # Request limits
    MAX_REQUEST_BODY_MB: int = Field(
        default=50,
        ge=1,
        le=1000,
        description="Maximum request body size in MB",
    )

    # Datastore backend. Typed as `str` (not `Literal`) so engines added
    # as local-only sub-packages (gitignored) are auto-accepted without
    # editing this file — see `_available_engines`. The committed list
    # of supported engines is the set of `infrastructure/engines/<name>/`
    # directories on disk at process start.
    DATASTORE_ENGINE: str = Field(
        default="bigquery",
        description=(
            "Backend engine name — must match an "
            "`infrastructure/engines/<name>/` package."
        ),
    )

    @field_validator("DATASTORE_ENGINE")
    @classmethod
    def _check_engine_available(cls, v: str) -> str:
        available = _available_engines()
        if v not in available:
            raise ValueError(
                f"DATASTORE_ENGINE={v!r} has no engine package; "
                f"available: {sorted(available)}"
            )
        return v

    # `datastore_search_sql` function allow-list — override the file the
    # engine loads. Unset (default) loads
    # Default `infrastructure/engines/<DATASTORE_ENGINE>/allowed_functions.txt`.
    SQL_FUNCTIONS_ALLOW_FILE: str | None = Field(
        default=None,
        description=(
            "Absolute path to a text file listing functions allowed in "
            "datastore_search_sql. One name per line, `#` comments. When "
            "unset, the engine's bundled allow-list is used."
        ),
    )

    # BigQuery settings
    BIGQUERY_PROJECT: str = Field(
        default="",
        description="Google Cloud project ID for BigQuery",
    )
    BIGQUERY_DATASET: str = Field(
        default="",
        description=(
            "BigQuery dataset that holds the datastore tables. Both the "
            "per-resource data tables and the internal `_table_metadata` "
            "table live here. Required when DATASTORE_ENGINE=bigquery."
        ),
    )
    BIGQUERY_CREDENTIALS: str = Field(
        default="",
        description=(
            "Service-account credentials for the read-write engine. "
            "Either JSON blob or path to a service-account JSON file."
        ),
    )
    BIGQUERY_CREDENTIALS_RO: str = Field(
        default="",
        description=(
            "Service-account credentials for the read-only engine. "
            "Either JSON blob or path to a service-account JSON file."
        ),
    )

    # Per-row system columns
    INCLUDE_UPDATED_AT: bool = Field(
        default=True,
        description=(
            "Add a `_updated_at` TIMESTAMP system column on each resource tables. "
        ),
    )

    # Redis settings
    REDIS_URL: str = Field(
        default="",
        description="Redis connection URL for caching",
    )

    # CKAN integration
    CKAN_URL: str = Field(
        default="",
        description="Base URL for CKAN instance",
    )
    HTTP_TIMEOUT_SECONDS: float = Field(
        default=10.0,
        gt=0,
        le=300,
        description="Timeout for CKAN API requests in seconds",
    )

    # Authentication
    AUTH_ENABLED: bool = Field(
        default=True,
        description="Enable CKAN-based authentication",
    )
    AUTH_CACHE_TTL: int = Field(
        default=300,
        description="TTL for auth cache entries in seconds",
    )

    # Logging
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level",
    )



@lru_cache
def get_config() -> Config:
    return Config()

