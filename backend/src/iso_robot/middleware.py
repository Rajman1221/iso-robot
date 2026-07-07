from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from iso_robot.helpers.auth import create_token, decode_token

# Paths reachable WITHOUT a valid session.
PUBLIC_EXACT = {
    "/",
    "/health",
    "/api/v1/health",
    "/auth/login",
    "/auth/register",
    "/api/v1/auth/login",
    "/api/v1/auth/register",
}
PUBLIC_PREFIXES = ("/docs", "/redoc", "/openapi.json")

# Paths authenticated against the EXTERNAL backend (not ISO Robot's own JWT).
# They carry the existing-backend token and are protected by the
# `verify_external_user` dependency, so the JWT session checkpoint must skip them.
EXTERNAL_AUTH_PREFIXES = ("/api/v1/ingest", "/api/v1/pipeline/status", "/api/v1/pipeline/cancel")


def _is_public(path: str) -> bool:
    return (
        path in PUBLIC_EXACT
        or any(path.startswith(p) for p in PUBLIC_PREFIXES)
        or any(path.startswith(p) for p in EXTERNAL_AUTH_PREFIXES)
    )


class SessionValidationMiddleware(BaseHTTPMiddleware):
    """Single checkpoint: every protected request must carry a valid session (JWT).
    Validates it, exposes the claims to handlers, and slides the window by
    re-issuing a fresh token on the way out."""

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method == "OPTIONS" or _is_public(request.url.path):
            return await call_next(request)

        auth = request.headers.get("authorization")
        if not auth or not auth.lower().startswith("bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header", "code": "UNAUTHORIZED"},
            )

        claims = decode_token(auth.split(" ", 1)[1].strip())
        if not claims or "sub" not in claims:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired session", "code": "SESSION_INVALID"},
            )

        request.state.user_claims = claims          # hand the validated claims to handlers
        response = await call_next(request)
        response.headers["X-Refresh-Token"] = create_token(  # sliding window
            claims["sub"], claims.get("org", ""), claims.get("role", "")
        )
        return response