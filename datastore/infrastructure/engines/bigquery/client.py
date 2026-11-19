"""BigQuery `Client` construction."""

from __future__ import annotations

import gzip
import json
from typing import TYPE_CHECKING, Any, Literal

from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from google.cloud import bigquery
    from requests import PreparedRequest, Response  # type: ignore[import-untyped]

    from datastore.core.config import Config

Mode = Literal["rw", "ro"]

GZIP_MIN_BYTES = 256 * 1024


class GzipRequestAdapter(HTTPAdapter):  # type: ignore[misc]
    """Gzips large JSON request bodies.

    BigQuery accepts a multi-MB query parameter several times faster
    gzipped than plain, and the client library cannot compress requests.
    """

    def send(self, request: PreparedRequest, *args: Any, **kwargs: Any) -> Response:
        body = request.body.encode("utf-8") if isinstance(request.body, str) else request.body
        if (
            isinstance(body, bytes)
            and len(body) >= GZIP_MIN_BYTES
            and "Content-Encoding" not in request.headers
            and request.headers.get("Content-Type", "").startswith("application/json")
        ):
            request.body = gzip.compress(body, compresslevel=6)
            request.headers["Content-Encoding"] = "gzip"
            request.headers["Content-Length"] = str(len(request.body))
        return super().send(request, *args, **kwargs)


def build_client(config: Config, mode: Mode) -> bigquery.Client:
    from google.cloud import bigquery

    project = config.BIGQUERY_PROJECT.strip()
    if not project:
        raise RuntimeError("BIGQUERY_PROJECT is required when DATASTORE_ENGINE=bigquery")

    creds = load_credentials(config, mode)
    if creds is None:
        client = bigquery.Client(project=project)
    else:
        client = bigquery.Client(project=project, credentials=creds)
    client._http.mount("https://", GzipRequestAdapter())
    return client


def load_credentials(config: Config, mode: Mode = "ro"):
    """Resolve service-account credentials for this engine mode.

    Defaults to `ro` — the safer choice. Read-only paths (dump, search)
    use this on their own; `build_client` passes `mode` explicitly when
    constructing the rw client. An empty RO credential falls through
    to ADC; it **never** falls back to the RW key, which would silently
    give read paths write privileges and defeat the credential split.
    """
    creds_raw = (
        config.BIGQUERY_CREDENTIALS_RO if mode == "ro" else config.BIGQUERY_CREDENTIALS
    ).strip()
    if not creds_raw:
        return None
    return _credentials_from_raw(creds_raw)


def _credentials_from_raw(raw: str):
    from google.oauth2 import service_account

    if raw.startswith("{"):
        return service_account.Credentials.from_service_account_info(json.loads(raw))
    return service_account.Credentials.from_service_account_file(raw)
