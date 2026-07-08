"""The unified organisation-profile endpoint: `POST /organisation-profile`.

One call that CREATES an organisation together with its demography (no
`client_org_id`), or UPSERTS an existing org's fields + demography (with a
`client_org_id`). Admin-gated, like plain org creation.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from conftest import unique_id
from iso_robot.helpers.auth import create_token, hash_password
from iso_robot.main import app

URL = "/api/v1/organisation-profile"


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


async def _user_token(db_session, role: str = "admin") -> str:
    """Bootstrap an org + a user with `role` and return one of our JWTs for them."""
    from iso_robot.repositories.org_repository import OrgRepository, UserRepository

    slug = unique_id("auth-org")
    org = await OrgRepository(db_session).create(name=f"Auth Org {slug}", slug=slug)
    user = await UserRepository(db_session).create(
        email=f"{slug}@example.com",
        hashed_password=hash_password("pw"),
        full_name="Auth User",
        client_org_id=org["id"],
        role=role,
    )
    return create_token(user["id"], org["id"], role, email=user["email"])


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_create_org_with_demography(client: TestClient, db_session) -> None:
    token = await _user_token(db_session)
    slug = unique_id("acme")
    resp = client.post(
        URL,
        headers=_auth(token),
        json={
            "name": "Acme Corp",
            "slug": slug,
            "industry": "Manufacturing",
            "region": "EU",
            "business_demography": {
                "sub_industry": "Automotive",
                "employee_count": "5000",
                "annual_revenue": "1B",
            },
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()["data"]
    assert data["created"] is True
    assert data["organisation"]["name"] == "Acme Corp"
    assert data["organisation"]["slug"] == slug
    assert data["organisation"]["industry"] == "Manufacturing"
    assert data["demography"]["sub_industry"] == "Automotive"
    assert data["demography"]["employee_count"] == "5000"


@pytest.mark.asyncio
async def test_upsert_demography_via_same_endpoint_merges(client: TestClient, db_session) -> None:
    token = await _user_token(db_session)
    slug = unique_id("beta")

    created = client.post(
        URL,
        headers=_auth(token),
        json={
            "name": "Beta Ltd",
            "slug": slug,
            "business_demography": {"industry": "Finance", "employee_count": "100"},
        },
    )
    org_id = created.json()["data"]["organisation"]["id"]

    # Update only sub_industry — industry + employee_count must be preserved.
    updated = client.post(
        URL,
        headers=_auth(token),
        json={
            "client_org_id": org_id,
            "business_demography": {"sub_industry": "Insurance"},
        },
    )
    assert updated.status_code == 200, updated.text
    data = updated.json()["data"]
    assert data["created"] is False
    demo = data["demography"]
    assert demo["sub_industry"] == "Insurance"   # newly set
    assert demo["industry"] == "Finance"          # preserved (upsert, not replace)
    assert demo["employee_count"] == "100"        # preserved


@pytest.mark.asyncio
async def test_upsert_updates_org_fields(client: TestClient, db_session) -> None:
    token = await _user_token(db_session)
    slug = unique_id("gamma")
    created = client.post(
        URL, headers=_auth(token), json={"name": "Gamma", "slug": slug}
    )
    org_id = created.json()["data"]["organisation"]["id"]

    updated = client.post(
        URL,
        headers=_auth(token),
        json={"client_org_id": org_id, "name": "Gamma Renamed", "industry": "Retail"},
    )
    org = updated.json()["data"]["organisation"]
    assert org["name"] == "Gamma Renamed"
    assert org["industry"] == "Retail"
    assert org["slug"] == slug  # untouched


@pytest.mark.asyncio
async def test_create_requires_name_and_slug(client: TestClient, db_session) -> None:
    token = await _user_token(db_session)
    resp = client.post(
        URL, headers=_auth(token), json={"business_demography": {"industry": "X"}}
    )
    assert resp.status_code == 422
    assert resp.json()["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_update_unknown_org_404(client: TestClient, db_session) -> None:
    token = await _user_token(db_session)
    resp = client.post(
        URL,
        headers=_auth(token),
        json={"client_org_id": unique_id("ghost"), "business_demography": {"industry": "X"}},
    )
    assert resp.status_code == 404
    assert resp.json()["code"] == "CLIENT_ORG_NOT_FOUND"


@pytest.mark.asyncio
async def test_duplicate_slug_409(client: TestClient, db_session) -> None:
    token = await _user_token(db_session)
    slug = unique_id("dup")
    first = client.post(URL, headers=_auth(token), json={"name": "One", "slug": slug})
    assert first.status_code == 200
    second = client.post(URL, headers=_auth(token), json={"name": "Two", "slug": slug})
    assert second.status_code == 409
    assert second.json()["code"] == "DUPLICATE_RECORD"


@pytest.mark.asyncio
async def test_non_admin_forbidden(client: TestClient, db_session) -> None:
    token = await _user_token(db_session, role="analyst")
    resp = client.post(
        URL, headers=_auth(token), json={"name": "NoPerm", "slug": unique_id("np")}
    )
    assert resp.status_code == 403
    assert resp.json()["code"] == "FORBIDDEN"
