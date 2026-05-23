"""JWT provider — verifies signature + optional `aud` / `iss` claims.

Verifies against the configured key (HS* secret or RS*/ES* PEM public
key). Decoded claims become `Decision.claims`; `sub` becomes `subject`.

Does NOT contact any external service. Authorization is implicit: a
valid JWT = allowed. Endpoints that need finer-grained policy can
inspect `Decision.claims` themselves.
"""

from __future__ import annotations

import jwt
from jwt import InvalidTokenError, PyJWTError

from datastore.auth.base import Decision, default_key_id
from datastore.core.config import Config
from datastore.core.exceptions import AuthorizationError


class JWTAuthProvider:
    name = "jwt"

    def __init__(self, *, config: Config, **_: object) -> None:
        algo = config.JWT_ALGORITHM
        self._algorithm = algo
        self._audience = config.JWT_AUDIENCE or None
        self._issuer = config.JWT_ISSUER or None
        if algo.startswith("HS"):
            if not config.JWT_SECRET:
                raise ValueError(
                    f"JWT_SECRET required when JWT_ALGORITHM={algo}"
                )
            self._key: str = config.JWT_SECRET
        else:
            if not config.JWT_PUBLIC_KEY:
                raise ValueError(
                    f"JWT_PUBLIC_KEY required when JWT_ALGORITHM={algo}"
                )
            self._key = config.JWT_PUBLIC_KEY

    async def authorize(
        self,
        *,
        credential: str | None,
        resource_id: str | None,
        package_id: str | None,
        permission: str | None,
    ) -> Decision:
        if not credential:
            raise AuthorizationError("Access denied: JWT token required")
        try:
            claims = jwt.decode(
                credential,
                self._key,
                algorithms=[self._algorithm],
                audience=self._audience,
                issuer=self._issuer,
            )
        except InvalidTokenError as exc:
            raise AuthorizationError(f"invalid JWT: {exc}") from exc
        except PyJWTError as exc:
            raise AuthorizationError("JWT verification failed") from exc
        sub = claims.get("sub")
        subject = sub if isinstance(sub, str) else None
        return Decision(subject=subject, claims=claims)

    def key_id(self, credential: str) -> str:
        return default_key_id(credential)
