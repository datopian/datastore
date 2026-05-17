from __future__ import annotations

from fastapi import APIRouter
from starlette.requests import Request

from datastore.api.responses import _success_response
from datastore.core.config import get_config
from datastore.schemas.responses import StatusResponse, WelcomeResponse

router = APIRouter(tags=["health"])


@router.get("/", response_model=WelcomeResponse)
def welcome(request: Request):
    return _success_response(
        request,
        WelcomeResponse.Result(message=get_config().APP_MESSAGE),
    )

@router.get("/health", response_model=StatusResponse)
def health(request: Request):
    return _success_response(request, StatusResponse.Result(status="ok"))


@router.get("/ready", response_model=StatusResponse)
def ready(request: Request):
    return _success_response(request, StatusResponse.Result(status="ready"))
