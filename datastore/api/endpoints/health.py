from __future__ import annotations

from types import SimpleNamespace

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import JSONResponse

from datastore.api.responses import _success_response
from datastore.core.config import get_config
from datastore.infrastructure.engines.registry import get_datastore_engine
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
    """Liveness — always 200 while the process is up."""
    return _success_response(request, StatusResponse.Result(status="ok"))


@router.get("/ready", response_model=StatusResponse)
def ready(request: Request):
    """Readiness — 200 when both rw and ro engines pass `healthcheck()`,
    503 otherwise. Probes both modes because the credential split means
    one can fail while the other works."""
    ctx = SimpleNamespace(config=get_config())

    failing: list[str] = []
    for mode in ("rw", "ro"):
        try:
            engine = get_datastore_engine(ctx, mode=mode)  # type: ignore[arg-type]
            if not engine.healthcheck():
                failing.append(mode)
        except Exception:
            failing.append(mode)

    if failing:
        return JSONResponse(
            status_code=503,
            content={
                "help": str(request.url),
                "success": False,
                "error": {
                    "__type": "Service Unavailable",
                    "message": (
                        f"engine healthcheck failed for mode(s): "
                        f"{', '.join(failing)}"
                    ),
                },
            },
        )
    return _success_response(request, StatusResponse.Result(status="ready"))
