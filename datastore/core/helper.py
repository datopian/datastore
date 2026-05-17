from __future__ import annotations


def parse_authorization_header(authorization: str | None) -> str | None:
    """Return the bare token from an `Authorization` header value, or None.

    Accepts the value verbatim — CKAN api_keys are passed as the bare token,
    no `Bearer ` prefix. Empty / whitespace-only values become `None`.
    """
    if authorization is None:
        return None
    token = authorization.strip()
    return token or None
