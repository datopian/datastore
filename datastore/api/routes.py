from __future__ import annotations

from fastapi import APIRouter

from datastore.api.endpoints import datastore, dump, health
from datastore.core.constants import API_BASE_PREFIX, API_PREFIX

api_router = APIRouter()
# Unversioned: probes and downloads are stable regardless of the action
# contract's version.
api_router.include_router(health.probe_router, prefix=API_BASE_PREFIX)
api_router.include_router(dump.router, prefix=API_BASE_PREFIX)
# Versioned: the datastore actions are the compatibility contract.
api_router.include_router(datastore.router, prefix=API_PREFIX)
