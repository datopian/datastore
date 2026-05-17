from __future__ import annotations


class APIError(Exception):
    """Base class for application errors that map to a CKAN error envelope."""

    status_code: int = 500
    type_label: str = "Internal Error"

    def __init__(
        self,
        message: str,
        *,
        fields: dict[str, list[str]] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.fields = fields


class ValidationError(APIError):
    status_code = 400
    type_label = "Validation Error"


class AuthorizationError(APIError):
    status_code = 403
    type_label = "Authorization Error"


class NotFoundError(APIError):
    status_code = 404
    type_label = "Not Found Error"


class ConflictError(APIError):
    status_code = 409
    type_label = "Conflict Error"


class ServerError(APIError):
    status_code = 500
    type_label = "Internal Error"


HTTP_STATUS_TO_TYPE_LABEL: dict[int, str] = {
    400: "Validation Error",
    401: "Authorization Error",
    403: "Authorization Error",
    404: "Not Found Error",
    405: "Not Found Error",
    409: "Conflict Error",
    413: "Validation Error",
    422: "Validation Error",
    501: "Not Implemented",
}
