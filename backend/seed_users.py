"""Seed the demo analyst users (one per org). Idempotent: safe to re-run.
Run from backend/ with PYTHONPATH=src. Dev tool — not used at runtime."""
import asyncio

from iso_robot.helpers.auth import hash_password
from iso_robot.repositories.database import get_session_factory
from iso_robot.repositories.migrations import run_migrations
from iso_robot.repositories.org_repository import OrgRepository, UserRepository

DEMO_USERS = [
    {"slug": "ORG001", "email": "qoc@demo.local",       "full_name": "QOC Analyst",       "password": "Passw0rd!"},
    {"slug": "ORG002", "email": "energy@demo.local",    "full_name": "Energy Analyst",    "password": "Passw0rd!"},
    {"slug": "ORG003", "email": "transport@demo.local", "full_name": "Transport Analyst", "password": "Passw0rd!"},
]


async def main() -> None:
    await run_migrations()
    session_factory = get_session_factory()
    async with session_factory() as session:
        orgs = OrgRepository(session)
        users = UserRepository(session)
        for u in DEMO_USERS:
            org = await orgs.get_by_slug(u["slug"])
            if not org:
                print(f"SKIP {u['email']}: org {u['slug']} not found"); continue
            if await users.get_by_email(u["email"]):
                print(f"exists  {u['email']}"); continue
            row = await users.create(
                email=u["email"], hashed_password=hash_password(u["password"]),
                full_name=u["full_name"], client_org_id=org["id"], role="analyst",
            )
            print(f"created {u['email']}  ({u['slug']} -> {row['id']})")


if __name__ == "__main__":
    asyncio.run(main())