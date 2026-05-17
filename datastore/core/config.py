from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # Datastore backend
    DATASTORE_ENGINE: Literal["bigquery", "ducklake"] = Field(
        default="bigquery",
        description="Backend engine: 'bigquery' or 'ducklake'",
    )

    # BigQuery settings
    BQ_PROJECT: str = Field(
        default="",
        description="Google Cloud project ID for BigQuery",
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

