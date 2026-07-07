"""One-off DEV tool: create the platform admin. Not part of app runtime.
Run from the `backend/` folder with PYTHONPATH=src. Password from ADMIN_PASSWORD env var.
Do NOT commit a real password."""
import asyncio
import os

from dotenv import load_dotenv
load_dotenv()

from iso_robot.helpers.auth import hash_password
from iso_robot.repositories.database import get_session_factory
from iso_robot.repositories.migrations import run_migrations
from iso_robot.repositories.org_repository import OrgRepository, UserRepository

ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")   # override via env for anything real
PLATFORM_SLUG = "platform"


async def main() -> None:
    await run_migrations()  # safe to re-run; migrations are idempotent
    session_factory = get_session_factory()
    async with session_factory() as session:
        orgs = OrgRepository(session)
        users = UserRepository(session)

        platform = await orgs.get_by_slug(PLATFORM_SLUG)
        if not platform:
            platform = await orgs.create(name="Platform (internal)", slug=PLATFORM_SLUG,
                                         industry="internal", region="internal")
            print("Created platform org:", platform["id"])
        else:
            print("Platform org exists:", platform["id"])

        if await users.get_by_email(ADMIN_EMAIL):
            print("Admin already exists:", ADMIN_EMAIL); return
        admin = await users.create(email=ADMIN_EMAIL, hashed_password=hash_password(ADMIN_PASSWORD),
                                   full_name="Platform Admin", client_org_id=platform["id"], role="admin")
        print("Created admin:", admin["id"], "| email:", ADMIN_EMAIL)


if __name__ == "__main__":
    asyncio.run(main())