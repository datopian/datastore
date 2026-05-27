from __future__ import annotations

from typing import Any

import httpx

from datastore.core.exceptions import (
    APIError,
    AuthorizationError,
    ConflictError,
    NotFoundError,
    ServerError,
    ValidationError,
)

_CKAN_TYPE_TO_ERROR: dict[str, type[APIError]] = {
    "validation error": ValidationError,
    "search query error": ValidationError,
    "authorization error": AuthorizationError,
    "access denied": AuthorizationError,
    "not found error": NotFoundError,
    "not found": NotFoundError,
    "conflict error": ConflictError,
    "integrity error": ConflictError,
    "internal server error": ServerError,
    "server error": ServerError,
}


class CKANClient:
    """Thin async wrapper around the CKAN action API.

    The shared `httpx.AsyncClient` is owned by the FastAPI lifespan so the
    connection pool is reused across requests. `bind(api_key)` returns a
    per-request copy that shares the pool and carries the caller's token.
    """

    def __init__(
        self, base_url: str, http: httpx.AsyncClient, api_key: str | None = None
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http
        self._api_key = api_key

    def bind(self, api_key: str | None) -> "CKANClient":
        """Return a per-request copy with `api_key` bound; shares the http pool."""
        return CKANClient(base_url=self._base_url, http=self._http, api_key=api_key)

    async def datastore_authorize(
        self,
        *,
        resource_id: str | None,
        package_id: str | None,
        permission: str | None = None,
    ) -> dict[str, Any]:
        """`/api/3/action/datastore_authorize`.
        Authorize resource and package.
        """
        if (resource_id is None) == (package_id is None):
            raise ValidationError(
                "datastore_authorize requires exactly one of resource_id or package_id"
            )
        body: dict[str, Any] = (
            {"resource_id": resource_id}
            if resource_id is not None
            else {"package_id": package_id}
        )
        if permission is not None:
            body["permission"] = permission
        return await self._post_action("datastore_authorize", body)

    async def resource_create(self, *, resource: dict[str, Any]) -> dict[str, Any]:
        """`/api/3/action/resource_create`. `resource` must include `package_id`."""
        if not resource.get("package_id"):
            raise ValidationError(
                "resource_create requires 'package_id' in the resource dict"
            )
        return await self._post_action("resource_create", dict(resource))

    async def resource_patch(
        self, *, resource_id: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        """`/api/3/action/resource_patch`. Merges `patch` onto the resource."""
        return await self._post_action("resource_patch", {"id": resource_id, **patch})

    # --- transport ---------------------------------------------------------
    async def _post_action(
        self, action: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        if not self._base_url:
            raise ServerError("CKAN_URL is not configured")

        url = f"{self._base_url}/api/3/action/{action}"
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = self._api_key

        try:
            response = await self._http.post(url, json=body, headers=headers)
        except httpx.HTTPError as exc:
            raise ServerError(f"CKAN request failed: {exc}") from exc

        self._raise_for_status(action, response)
        return self._unwrap(action, response)

    @staticmethod
    def _raise_for_status(action: str, response: httpx.Response) -> None:
        """Map HTTP-level CKAN failures to our APIError taxonomy.

        Status codes whose body carries useful detail (400 validation,
        409 conflict, 422 unprocessable) are NOT raised here — they fall
        through to `_unwrap`, which reads `__type` + `message` from the
        CKAN envelope and dispatches via `_CKAN_TYPE_TO_ERROR`. That way
        the consumer sees "field X is required", not a generic 500.
        """
        status = response.status_code
        if status in (401, 403):
            raise AuthorizationError(
                f"Access denied: Action {action} requires an authenticated user"
            )
        if status == 404:
            raise NotFoundError("Not found")
        if status >= 500:
            raise ServerError(f"CKAN returned {status} for {action}")

    @staticmethod
    def _unwrap(action: str, response: httpx.Response) -> dict[str, Any]:
        """Parse the CKAN envelope; raise on `success=false` or bad shape."""
        print(response.status_code)
        try:
            payload = response.json()
        except ValueError as exc:
            raise ServerError(f"CKAN {action} returned a non-JSON body") from exc

        if not isinstance(payload, dict):
            raise ServerError(f"CKAN {action} returned a non-object body")

        if not payload.get("success"):
            error = payload.get("error") or {}
            error_type = str(error.get("__type", "")).strip().lower()
            # CKAN puts field-level validation errors as arbitrary keys on
            # `error` alongside `__type` / `message`, e.g.
            #     {"__type": "Validation Error",
            #      "ingestion_method": ["must be one of ..."]}.
            # Pull them out into `APIError.fields` so the response carries
            # the structured detail, and build a human-readable message
            # from the first field error when CKAN didn't send `message`.
            field_errors: dict[str, list[str]] = {}
            for key, value in error.items():
                if key in ("__type", "message"):
                    continue
                if isinstance(value, list):
                    field_errors[key] = [str(v) for v in value]
                elif value:
                    field_errors[key] = [str(value)]

            if error.get("message"):
                message = str(error["message"])
            elif field_errors:
                first_field, first_msgs = next(iter(field_errors.items()))
                message = f"{first_field}: {first_msgs[0]}" if first_msgs else first_field
            else:
                message = f"CKAN denied {action}"

            exc_cls = _CKAN_TYPE_TO_ERROR.get(error_type, ServerError)
            raise exc_cls(message, fields=field_errors or None)

        result = payload.get("result")
        if not isinstance(result, dict):
            raise ServerError(f"CKAN {action} returned an unexpected shape")
        return result
