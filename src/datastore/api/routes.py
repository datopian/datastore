"""HTTP routes for the datastore API."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from datastore.config import Settings, get_settings

router = APIRouter()


@router.get("/")
def welcome(settings: Settings = Depends(get_settings)) -> dict[str, str]:
    return {"message": settings.APP_MESSAGE}


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
def ready() -> dict[str, str]:
    return {"status": "ready"}
