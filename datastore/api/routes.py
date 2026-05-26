from __future__ import annotations

from fastapi import APIRouter

from datastore.api.endpoints import datastore, dump, health

api_router = APIRouter()
api_router.include_router(health.welcome_router)
api_router.include_router(health.probe_router)
api_router.include_router(health.probe_router, prefix="/api/3/action")
api_router.include_router(datastore.router, prefix="/api/3/action")
api_router.include_router(dump.router)