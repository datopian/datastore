from __future__ import annotations

from fastapi import APIRouter

from datastore.api.endpoints import datastore, health

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(datastore.router, prefix="/api/3/action")
