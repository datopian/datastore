from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENGINES_DIR = (
    Path(__file__).resolve().parent.parent / "infrastructure" / "engines"
)
_AUTH_DIR = Path(__file__).resolve().parent.parent / "auth"


def _subdirs(root: Path) -> set[str]:
    if not root.is_dir():
        return set()
    return {
        p.name for p in root.iterdir()
        if p.is_dir()
        and not p.name.startswith(("_", "."))
        and p.name != "__pycache__"
    }


def _available_engines() -> set[str]:
    """Engine names = `infrastructure/engines/<name>/` directories on disk.

    Lets `DATASTORE_ENGINE` auto-accept any locally-present engine package
    without listing them statically. Backends gitignored for local dev
    (e.g. a test engine) work locally and stay invisible upstream.
    """
    return _subdirs(_ENGINES_DIR)


def _available_auth_types() -> set[str]:
    """Auth provider names = `datastore/auth/<name>/` directories on disk."""
    return _subdirs(_AUTH_DIR)


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
    BIGQUERY_USE_QUERY_CACHE: bool = Field(
        default=True,
        description=(
            "Use BigQuery's built-in 24h query-results cache on read paths "
            "(datastore_search / datastore_search_sql / datastore_info). "
            "Identical, deterministic SELECTs return free + fast on cache "
            "hits. Set False for freshness-sensitive deployments or to "
            "force a fresh scan in tests."
        ),
    )
    BIGQUERY_EXPORT_BUCKET: str = Field(
        default="",
        description=(
            "GCS bucket name (no `gs://` prefix) that `/datastore/dump/<rid>` "
        ),
    )
    BIGQUERY_EXPORT_URL_EXPIRY_HOURS: int = Field(
        default=1,
        ge=1,
        le=168,
        description=(
            "Signed-URL TTL for dump manifest entries (hours). Defaults to 1h."
        ),
    )

    # Per-row system columns
    INCLUDE_UPDATED_AT: bool = Field(
        default=True,
        description=(
            "Add a `_updated_at` TIMESTAMP system column on each resource tables. "
        ),
    )

    # Search
    SEARCH_RESULT_ROWS_MAX: int = Field(
        default=32000,
        ge=1,
        description=(
            "Hard cap on `datastore_search` / `datastore_search_sql` `limit`. "
            "Requests above this return 400."
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

    # Authentication. `AUTH_TYPE` selects the provider package under
    # `datastore/auth/<name>/`. Drop a sibling folder to add one.
    AUTH_TYPE: str = Field(
        default="ckan",
        description=(
            "Auth provider — must match a `datastore/auth/<name>/` package. "
            "Built-in: `ckan`, `jwt`, `anonymous` (no auth)."
        ),
    )
    AUTH_CACHE_TTL: int = Field(
        default=300,
        description="TTL for auth cache entries in seconds",
    )

    @field_validator("AUTH_TYPE")
    @classmethod
    def _check_auth_type(cls, v: str) -> str:
        available = _available_auth_types()
        if v not in available:
            raise ValueError(
                f"AUTH_TYPE={v!r} has no provider package; "
                f"available: {sorted(available)}"
            )
        return v

    # JWT settings (consumed by `datastore/auth/jwt` only).
    JWT_ALGORITHM: Literal[
        "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "ES256", "ES384"
    ] = Field(
        default="HS256",
        description=(
            "JWT signing algorithm. HS* uses JWT_SECRET; "
            "RS*/ES* uses JWT_PUBLIC_KEY (PEM)."
        ),
    )
    JWT_SECRET: str = Field(
        default="",
        description="HS* shared secret. Required when AUTH_TYPE=jwt and JWT_ALGORITHM=HS*.",
    )
    JWT_PUBLIC_KEY: str = Field(
        default="",
        description="RS*/ES* PEM-encoded public key. Required for RS*/ES*.",
    )
    JWT_AUDIENCE: str = Field(
        default="",
        description="Expected `aud` claim. Empty = skip audience check.",
    )
    JWT_ISSUER: str = Field(
        default="",
        description="Expected `iss` claim. Empty = skip issuer check.",
    )

    # Logging
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level",
    )

    @model_validator(mode="after")
    def _check_ckan_url_required_for_ckan_auth(self) -> Config:
        if self.AUTH_TYPE == "ckan" and not self.CKAN_URL:
            raise ValueError(
                "CKAN_URL must be set when AUTH_TYPE=ckan "
                "(use AUTH_TYPE=anonymous or jwt to run standalone)"
            )
        return self



@lru_cache
def get_config() -> Config:
    return Config()

