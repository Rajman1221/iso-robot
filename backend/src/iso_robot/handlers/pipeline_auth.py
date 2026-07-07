"""Auth dependency for the ingest / pipeline-status APIs.

These endpoints are authenticated against the EXISTING backend, not ISO Robot's
own JWT. The caller sends:

    Authorization: Bearer <external token>
    X-User-Id:    <userId>
    X-User-Email: <email>
    X-Org-Id:     <organisationId>

The dependency enforces that X-Org-Id matches the {client_org_id} in the path,
reuses a recent successful verification (30-min sliding cache), otherwise calls
the external verify backend, and fails closed if that backend is unavailable.

Set `VERIFY_MOCK=true` to skip the HTTP call entirely (dev/test only) — headers
and the org-id match check still apply.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Depends, Request

from iso_robot.config import Settings
from iso_robot.deps import get_app_settings, get_audit_repo
from iso_robot.errors import APIError
from iso_robot.helpers.verify_cache import (
    VerifiedContext,
    get_verification_cache,
)
from iso_robot.integrations.verify_client import verify_user
from iso_robot.repositories.org_repository import AuditLogRepository

logger = logging.getLogger(__name__)


def _bearer_token(request: Request) -> str:
    header = request.headers.get("authorization")
    if not header or not header.lower().startswith("bearer "):
        raise APIError(
            "Missing or invalid Authorization header",
            code="UNAUTHORIZED",
            status_code=401,
        )
    token = header.split(" ", 1)[1].strip()
    if not token:
        raise APIError(
            "Missing or invalid Authorization header",
            code="UNAUTHORIZED",
            status_code=401,
        )
    return token


def _required_header(request: Request, name: str) -> str:
    value = request.headers.get(name)
    if not value or not value.strip():
        raise APIError(
            f"Missing required header: {name}",
            code="UNAUTHORIZED",
            status_code=401,
        )
    return value.strip()


async def verify_external_user(
    client_org_id: str,
    request: Request,
    settings: Annotated[Settings, Depends(get_app_settings)],
    audit_repo: Annotated[AuditLogRepository, Depends(get_audit_repo)],
) -> VerifiedContext:
    """Authenticate a request against the external backend, with a sliding cache."""
    token = _bearer_token(request)
    user_id = _required_header(request, "X-User-Id")
    email = _required_header(request, "X-User-Email")
    org_id = _required_header(request, "X-Org-Id")

    if org_id != client_org_id:
        await audit_repo.log(
            api_name="external_verify",
            client_org_id=client_org_id,
            requested_by=user_id,
            status="denied",
            error_details="org_mismatch",
        )
        raise APIError(
            "You do not have access to this organisation",
            code="FORBIDDEN",
            status_code=403,
        )

    if settings.verify_mock:
        logger.debug("VERIFY_MOCK: skipping external verify for org %s", client_org_id)
        await audit_repo.log(
            api_name="external_verify",
            client_org_id=client_org_id,
            requested_by=user_id,
            status="success",
            input_metadata={"verify_mock": True},
        )
        return VerifiedContext(user_id=user_id, email=email, org_id=org_id)

    cache = get_verification_cache(settings.external_auth_cache_minutes * 60)

    cached = await cache.get(token)
    if cached is not None:
        return cached

    result = await verify_user(
        settings,
        token=token,
        user_id=user_id,
        email=email,
        organisation_id=org_id,
    )

    if not result.valid:
        await audit_repo.log(
            api_name="external_verify",
            client_org_id=client_org_id,
            requested_by=user_id,
            status="denied",
            error_details="verification_failed",
        )
        raise APIError(
            "Invalid or expired session",
            code="SESSION_INVALID",
            status_code=401,
        )

    # Best-effort cross-check: if the external backend echoed identity fields,
    # they must agree with the claimed identity.
    _cross_check(result.raw, email=email, org_id=org_id, client_org_id=client_org_id)

    context = VerifiedContext(user_id=user_id, email=email, org_id=org_id)
    await cache.set(token, context)

    await audit_repo.log(
        api_name="external_verify",
        client_org_id=client_org_id,
        requested_by=user_id,
        status="success",
    )
    return context


def _cross_check(raw: dict, *, email: str, org_id: str, client_org_id: str) -> None:
    """Reject if the verify response contains identity fields that disagree."""
    resp_email = raw.get("email")
    if isinstance(resp_email, str) and resp_email and resp_email != email:
        raise APIError(
            "Verified identity does not match the supplied credentials",
            code="FORBIDDEN",
            status_code=403,
        )
    for key in ("organisationId", "organizationId", "orgId", "organisation_id"):
        resp_org = raw.get(key)
        if isinstance(resp_org, str) and resp_org and resp_org != org_id:
            raise APIError(
                "Verified organisation does not match the supplied credentials",
                code="FORBIDDEN",
                status_code=403,
            )
