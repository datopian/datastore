from __future__ import annotations

from fastapi import APIRouter

from datastore.api.endpoints import datastore, dump, health
from datastore.core.constants import API_BASE_PREFIX, API_PREFIX, DUMP_PREFIX

api_router = APIRouter()
# Unversioned: a probe URL is stable regardless of the action contract.
api_router.include_router(health.probe_router, prefix=API_BASE_PREFIX)
# Downloads sit outside `/api` — a dump URL is opened in a browser or fed to
# `curl`, not driven by an API client.
api_router.include_router(dump.router, prefix=DUMP_PREFIX)
# Versioned: the datastore actions are the compatibility contract.
api_router.include_router(datastore.router, prefix=API_PREFIX)
