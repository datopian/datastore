from __future__ import annotations

from typing import Any

import httpx

from datastore.core.exceptions import AuthorizationError, NotFoundError, ServerError


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
            raise ValueError(
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
            raise ValueError("resource_create requires 'package_id' in the resource dict")
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
        """Map HTTP-level CKAN failures to our APIError taxonomy."""
        status = response.status_code
        if status in (401, 403, 409):
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
        try:
            payload = response.json()
        except ValueError as exc:
            raise ServerError(f"CKAN {action} returned a non-JSON body") from exc

        if not payload.get("success"):
            error = payload.get("error") or {}
            error_type = str(error.get("__type", "")).lower()
            message = error.get("message") or f"CKAN denied {action}"
            if "not found" in error_type:
                raise NotFoundError(message)
            raise AuthorizationError(message)

        result = payload.get("result")
        if not isinstance(result, dict):
            raise ServerError(f"CKAN {action} returned an unexpected shape")
        return result
