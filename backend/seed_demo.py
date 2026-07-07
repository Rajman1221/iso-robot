"""Seed demo orgs + users for local/Postman verification. Idempotent — safe to re-run.

Run from backend/:
  source ../.venv/bin/activate
  export PYTHONPATH=src
  python3 seed_demo.py
"""
from __future__ import annotations

import asyncio

from iso_robot.config import get_settings
from iso_robot.domain.repair_storage_paths import sync_org_folder_mapping
from iso_robot.helpers.auth import hash_password
from iso_robot.repositories.database import get_session_factory
from iso_robot.repositories.migrations import run_migrations
from iso_robot.repositories.org_repository import (
    FolderRepository,
    OrgRepository,
    TenantRepository,
    UserRepository,
)

ADMIN = {
    "email": "admin@catalytics.local",
    "password": "AdminPWD123!",
    "full_name": "Platform Admin",
    "role": "admin",
    "org_slug": "platform",
    "org_name": "Platform (internal)",
}

ANALYST_ORGS = [
    {
        "slug": "ORG001",
        "name": "QOC Demo Org",
        "industry": "Oil & Gas",
        "region": "GCC",
        "user": {
            "email": "qoc@demo.local",
            "password": "Passw0rd!",
            "full_name": "QOC Analyst",
        },
    },
    {
        "slug": "ORG002",
        "name": "Energy Demo Org",
        "industry": "Energy",
        "region": "APAC",
        "user": {
            "email": "energy@demo.local",
            "password": "Passw0rd!",
            "full_name": "Energy Analyst",
        },
    },
    {
        "slug": "ORG003",
        "name": "Transport Demo Org",
        "industry": "Transport",
        "region": "EMEA",
        "user": {
            "email": "transport@demo.local",
            "password": "Passw0rd!",
            "full_name": "Transport Analyst",
        },
    },
]


async def _ensure_org_folders(
    settings,
    org_repo: OrgRepository,
    folder_repo: FolderRepository,
    tenant_repo: TenantRepository,
    org: dict,
) -> None:
    org_id = org["id"]
    slug = org["slug"]
    tenant = await tenant_repo.get_by_org(org_id)
    if not tenant:
        await tenant_repo.create(client_org_id=org_id, tenant_id=slug)
        print(f"  tenant  {slug}")

    paths = await sync_org_folder_mapping(
        settings,
        folder_repo,
        client_org_id=org_id,
        org_slug=slug,
    )
    print(f"  folders {paths['control_documents']}")


async def _ensure_user(
    users: UserRepository,
    *,
    email: str,
    password: str,
    full_name: str,
    client_org_id: str,
    role: str,
) -> None:
    existing = await users.get_by_email(email)
    if existing:
        print(f"exists  {email}")
        return
    await users.create(
        email=email,
        hashed_password=hash_password(password),
        full_name=full_name,
        client_org_id=client_org_id,
        role=role,
    )
    print(f"created {email} ({role})")


async def main() -> None:
    settings = get_settings()

    await run_migrations()
    print("schema ok")

    session_factory = get_session_factory()
    async with session_factory() as session:
        orgs = OrgRepository(session)
        users = UserRepository(session)
        folders = FolderRepository(session)
        tenants = TenantRepository(session)

        platform = await orgs.get_by_slug(ADMIN["org_slug"])
        if not platform:
            platform = await orgs.create(
                name=ADMIN["org_name"],
                slug=ADMIN["org_slug"],
                industry="internal",
                region="internal",
            )
            print(f"org     {ADMIN['org_slug']} -> {platform['id']}")
        await _ensure_org_folders(settings, orgs, folders, tenants, platform)
        await _ensure_user(
            users,
            email=ADMIN["email"],
            password=ADMIN["password"],
            full_name=ADMIN["full_name"],
            client_org_id=platform["id"],
            role=ADMIN["role"],
        )

        for spec in ANALYST_ORGS:
            org = await orgs.get_by_slug(spec["slug"])
            if not org:
                org = await orgs.create(
                    name=spec["name"],
                    slug=spec["slug"],
                    industry=spec["industry"],
                    region=spec["region"],
                )
                print(f"org     {spec['slug']} -> {org['id']}")
            await _ensure_org_folders(settings, orgs, folders, tenants, org)
            u = spec["user"]
            await _ensure_user(
                users,
                email=u["email"],
                password=u["password"],
                full_name=u["full_name"],
                client_org_id=org["id"],
                role="analyst",
            )

        print("\nLogin credentials ready:")
        print(f"  Admin:   {ADMIN['email']} / {ADMIN['password']}")
        print(f"  Analyst: {ANALYST_ORGS[0]['user']['email']} / {ANALYST_ORGS[0]['user']['password']}")


if __name__ == "__main__":
    asyncio.run(main())
