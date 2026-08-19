from __future__ import annotations

from types import SimpleNamespace

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import JSONResponse

from datastore.api.responses import _help, _success_response
from datastore.core.config import get_config
from datastore.infrastructure.engines.registry import get_datastore_engine
from datastore.schemas.responses import StatusResponse

probe_router = APIRouter(tags=["Health"])


@probe_router.get(
    "/health", response_model=StatusResponse, summary="Liveness probe"
)
def health(request: Request):
    """Liveness — always 200 while the process is up."""
    return _success_response(request, StatusResponse.Result(status="ok"))


@probe_router.get(
    "/ready",
    response_model=StatusResponse,
    summary="Readiness probe",
    responses={503: {"model": StatusResponse, "description": "One or more engines unavailable"}},
)
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
                "help": _help(request),
                "success": False,
                "result": {"status": "not_ready"},
            },
        )
    return _success_response(request, StatusResponse.Result(status="ready"))
