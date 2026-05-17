from __future__ import annotations

from fastapi import APIRouter
from starlette.requests import Request

from datastore.api.responses import ckan_success
from datastore.core.config import get_config
from datastore.schemas.responses import StatusResponse, WelcomeResponse

router = APIRouter(tags=["health"])


@router.get("/", response_model=WelcomeResponse)
def welcome(request: Request):
    return ckan_success(
        request,
        WelcomeResponse.Result(message=get_config().APP_MESSAGE),
    )

@router.get("/health", response_model=StatusResponse)
def health(request: Request):
    return ckan_success(request, StatusResponse.Result(status="ok"))


@router.get("/ready", response_model=StatusResponse)
def ready(request: Request):
    return ckan_success(request, StatusResponse.Result(status="ready"))
