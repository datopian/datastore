from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_LIMIT = 1000
MAX_LIMIT = 10000
BATCH_SIZE = 500


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_MESSAGE: str = "Datastore API"
    MAX_REQUEST_BODY_MB: int = 50
    DATASTORE_ENGINE: str = "bigquery"
    BQ_PROJECT: str = ""
    REDIS_URL: str = ""
    CKAN_URL: str = ""
    LOG_LEVEL: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
