"""External backend user-verification client.

ISO Robot does not own the user store for the ingest / pipeline-status APIs.
Instead it forwards the caller's existing-backend token (plus the claimed
identity) to that backend's verify endpoint and trusts the result.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict

import httpx

from iso_robot.config import Settings
from iso_robot.errors import APIError

logger = logging.getLogger(__name__)


@dataclass
class VerifyResult:
    """Normalized outcome of an external verification call."""

    valid: bool
    raw: Dict[str, Any] = field(default_factory=dict)


def _is_valid_response(settings: Settings, payload: Dict[str, Any]) -> bool:
    """Apply the optional configured validity assertion to a 2xx body."""
    field_name = settings.verify_api_valid_field
    if not field_name:
        return True
    if field_name not in payload:
        return False
    actual = payload[field_name]
    expected = settings.verify_api_valid_value
    if expected:
        return str(actual) == str(expected)
    return bool(actual)


async def verify_user(
    settings: Settings,
    *,
    token: str,
    user_id: str,
    email: str,
    organisation_id: str,
) -> VerifyResult:
    """Verify a user against the external backend.

    Returns a VerifyResult on a definitive answer (valid or invalid).
    Raises APIError(EXTERNAL_AUTH_UNAVAILABLE) when the backend cannot be reached
    or returns a server error (fail closed).
    """
    url = settings.require_verify_api_url()

    headers = {"Authorization": f"Bearer {token}"}
    if settings.verify_api_key:
        headers["X-Api-Key"] = settings.verify_api_key

    body = {
        "token": token,
        "userId": user_id,
        "email": email,
        "organisationId": organisation_id,
    }

    try:
        async with httpx.AsyncClient(timeout=settings.verify_api_timeout_seconds) as client:
            response = await client.post(url, json=body, headers=headers)
    except httpx.HTTPError as exc:
        logger.warning("Verify backend unreachable: %s", type(exc).__name__)
        raise APIError(
            "User verification backend is unavailable",
            code="EXTERNAL_AUTH_UNAVAILABLE",
            status_code=503,
        ) from exc

    if response.status_code in (401, 403):
        return VerifyResult(valid=False)

    if response.status_code >= 500:
        logger.warning("Verify backend returned %s", response.status_code)
        raise APIError(
            "User verification backend is unavailable",
            code="EXTERNAL_AUTH_UNAVAILABLE",
            status_code=503,
        )

    if not (200 <= response.status_code < 300):
        return VerifyResult(valid=False)

    try:
        payload = response.json()
        if not isinstance(payload, dict):
            payload = {"value": payload}
    except ValueError:
        payload = {}

    return VerifyResult(valid=_is_valid_response(settings, payload), raw=payload)
