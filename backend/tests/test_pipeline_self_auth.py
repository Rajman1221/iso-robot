"""Self-mode auth for the pipeline APIs (the shipped default: auth_mode="self").

Here ISO Robot IS the identity provider: the caller sends the JWT we issued at
`POST /auth/login`, and `authenticate_pipeline_request` validates our own token
and enforces that the token's org matches the `{client_org_id}` in the path —
no external verify backend, no `X-User-*` headers required.

The conftest pins the process to `AUTH_MODE=external`, so every test here flips
`settings.auth_mode` back to `"self"` for the duration via monkeypatch.
"""

from __future__ import annotations

import io
from typing import Any

import pytest
from fastapi.testclient import TestClient

from conftest import unique_id
from iso_robot.config import get_settings
from iso_robot.helpers.auth import create_token, hash_password
from iso_robot.main import app


def _pdf_bytes(tag: str) -> bytes:
    return f"%PDF-1.4 fake content {tag}".encode("utf-8")


@pytest.fixture
def self_client(monkeypatch: pytest.MonkeyPatch):
    """A TestClient with enqueue stubbed and auth_mode forced to 'self'."""
    monkeypatch.setattr(get_settings(), "auth_mode", "self")

    calls: list[dict[str, Any]] = []

    def _fake_enqueue(run_id: str, document_ids: list[str]) -> str:
        calls.append({"run_id": run_id, "document_ids": document_ids})
        return f"fake-task-{run_id}"

    monkeypatch.setattr("iso_robot.handlers.pipeline.enqueue_pipeline", _fake_enqueue)

    with TestClient(app) as test_client:
        test_client.enqueue_calls = calls  # type: ignore[attr-defined]
        yield test_client


async def _org_and_token(db_session, role: str = "analyst") -> tuple[dict, str]:
    """Create an org + an active user in it and mint one of our JWTs for them."""
    from iso_robot.repositories.org_repository import OrgRepository, UserRepository

    slug = unique_id("self-org")
    org = await OrgRepository(db_session).create(name=f"Self Org {slug}", slug=slug)
    user = await UserRepository(db_session).create(
        email=f"{slug}@example.com",
        hashed_password=hash_password("pw"),
        full_name="Self Test User",
        client_org_id=org["id"],
        role=role,
    )
    token = create_token(user["id"], org["id"], role, email=user["email"], name="Self Test User")
    return org, token


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_self_token_ingest_succeeds(self_client: TestClient, db_session) -> None:
    org, token = await _org_and_token(db_session)
    resp = self_client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_auth(token),
        files={"file": ("policy.pdf", io.BytesIO(_pdf_bytes("self-1")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert resp.status_code == 202
    data = resp.json()["data"]
    assert data["client_org_id"] == org["id"]
    assert data["new_documents"] == 1


@pytest.mark.asyncio
async def test_self_token_status_succeeds(self_client: TestClient, db_session) -> None:
    org, token = await _org_and_token(db_session)
    self_client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_auth(token),
        files={"file": ("s.pdf", io.BytesIO(_pdf_bytes("self-2")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    resp = self_client.get(f"/api/v1/pipeline/status/{org['id']}", headers=_auth(token))
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "queued"


@pytest.mark.asyncio
async def test_self_token_for_other_org_is_forbidden(self_client: TestClient, db_session) -> None:
    _, token_a = await _org_and_token(db_session)
    org_b, _ = await _org_and_token(db_session)
    # Token belongs to org A; try to act on org B.
    resp = self_client.post(
        f"/api/v1/ingest/{org_b['id']}",
        headers=_auth(token_a),
        files={"file": ("x.pdf", io.BytesIO(_pdf_bytes("self-3")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert resp.status_code == 403
    assert resp.json()["code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_self_mode_missing_bearer_is_unauthorized(self_client: TestClient, db_session) -> None:
    org, _ = await _org_and_token(db_session)
    resp = self_client.post(
        f"/api/v1/ingest/{org['id']}",
        files={"file": ("x.pdf", io.BytesIO(_pdf_bytes("self-4")), "application/pdf")},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_self_mode_garbage_token_is_unauthorized(self_client: TestClient, db_session) -> None:
    org, _ = await _org_and_token(db_session)
    resp = self_client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_auth("not-a-real-jwt"),
        files={"file": ("x.pdf", io.BytesIO(_pdf_bytes("self-5")), "application/pdf")},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_self_mode_mismatched_header_is_forbidden(self_client: TestClient, db_session) -> None:
    org, token = await _org_and_token(db_session)
    headers = _auth(token)
    headers["X-Org-Id"] = "some-other-org"  # optional header, must match the token
    resp = self_client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=headers,
        files={"file": ("x.pdf", io.BytesIO(_pdf_bytes("self-6")), "application/pdf")},
    )
    assert resp.status_code == 403
