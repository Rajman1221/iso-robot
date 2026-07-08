"""Token introspection: `POST /auth/verify`.

This is how a downstream backend checks a token we issued — no JWT library or
shared secret of its own, just one HTTP call. It always returns HTTP 200 with
`data.valid`; a wrong/missing `X-Api-Key` (when keys are configured) is the only
case that raises 401.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from conftest import unique_id
from iso_robot.config import get_settings
from iso_robot.helpers.auth import create_token, hash_password
from iso_robot.main import app


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


async def _org_and_user(db_session, role: str = "analyst") -> tuple[dict, dict]:
    from iso_robot.repositories.org_repository import OrgRepository, UserRepository

    slug = unique_id("verify-org")
    org = await OrgRepository(db_session).create(name=f"Verify Org {slug}", slug=slug)
    user = await UserRepository(db_session).create(
        email=f"{slug}@example.com",
        hashed_password=hash_password("pw"),
        full_name="Verify User",
        client_org_id=org["id"],
        role=role,
    )
    return org, user


@pytest.mark.asyncio
async def test_verify_valid_token_returns_identity(client: TestClient, db_session) -> None:
    org, user = await _org_and_user(db_session)
    token = create_token(user["id"], org["id"], user["role"], email=user["email"])

    resp = client.post("/api/v1/auth/verify", json={"token": token})
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["valid"] is True
    assert data["user_id"] == user["id"]
    assert data["email"] == user["email"]
    assert data["client_org_id"] == org["id"]
    assert data["role"] == user["role"]
    assert isinstance(data["expires_at"], int)


@pytest.mark.asyncio
async def test_verify_garbage_token_is_invalid(client: TestClient) -> None:
    resp = client.post("/api/v1/auth/verify", json={"token": "not-a-real-jwt"})
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["valid"] is False
    assert data["user_id"] is None


@pytest.mark.asyncio
async def test_verify_disabled_user_is_invalid(client: TestClient, db_session) -> None:
    from iso_robot.models.org import User

    org, user = await _org_and_user(db_session)
    token = create_token(user["id"], org["id"], user["role"], email=user["email"])

    # Deactivate the user — a still-signature-valid token must now report invalid.
    obj = await db_session.get(User, user["id"])
    obj.is_active = False
    await db_session.commit()

    resp = client.post("/api/v1/auth/verify", json={"token": token})
    assert resp.status_code == 200
    assert resp.json()["data"]["valid"] is False


@pytest.mark.asyncio
async def test_verify_requires_api_key_when_configured(
    client: TestClient, db_session, monkeypatch
) -> None:
    monkeypatch.setattr(get_settings(), "auth_introspection_keys", "s3cret, other-key")

    org, user = await _org_and_user(db_session)
    token = create_token(user["id"], org["id"], user["role"], email=user["email"])

    # No key → 401.
    missing = client.post("/api/v1/auth/verify", json={"token": token})
    assert missing.status_code == 401

    # Wrong key → 401.
    wrong = client.post(
        "/api/v1/auth/verify", json={"token": token}, headers={"X-Api-Key": "nope"}
    )
    assert wrong.status_code == 401

    # Correct key → 200 valid.
    ok = client.post(
        "/api/v1/auth/verify", json={"token": token}, headers={"X-Api-Key": "s3cret"}
    )
    assert ok.status_code == 200
    assert ok.json()["data"]["valid"] is True
