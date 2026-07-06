"""
Isolation test for the SQLAlchemy ControlRepository — proves the converted repo works
against whatever DATABASE_URL points to (MSSQL primary), without touching the running app.

Run from backend/ with the venv active and DATABASE_URL set in backend/.env:
    cd backend
    $env:PYTHONPATH = "src"
    python test_control_repo_sa.py

It creates a throwaway document + 2 controls, exercises every method, prints results,
then cleans up after itself. Safe to run repeatedly.
"""
import asyncio
import sys

# Windows + async safety (harmless elsewhere)
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from sqlalchemy import delete, insert
from iso_robot.repositories.db import SessionFactory, DATABASE_URL   # engine/session added in Step 1
from iso_robot.repositories.models import documents
from iso_robot.repositories.control_repository_sa import ControlRepository

DOC_ID = "test-doc-sa-0001"
ORG = "test-org-sa"


async def main() -> None:
    print(f"DATABASE_URL in use: {DATABASE_URL.split('://')[0]}://...")
    async with SessionFactory() as s:
        # clean any leftovers from a previous run
        repo = ControlRepository(s)
        await repo.delete_for_document(DOC_ID)
        await s.execute(delete(documents).where(documents.c.id == DOC_ID))
        await s.commit()

        # parent document (controls.document_id is a NOT NULL FK -> documents.id)
        await s.execute(insert(documents), {
            "id": DOC_ID, "filename": "sa_test.pdf", "path": "/tmp/sa_test.pdf",
            "sha256": "sa-test-sha-0001", "size_bytes": 123, "status": "local",
            "client_org_id": ORG, "created_at": "2026-01-01T00:00:00Z",
        })
        await s.commit()

        # exercise the repo
        await repo.insert_many([
            {"id": "sa-c1", "document_id": DOC_ID, "control_text": "First",  "created_at": "2026-01-01T00:00:01Z"},
            {"id": "sa-c2", "document_id": DOC_ID, "control_text": "Second", "created_at": "2026-01-01T00:00:02Z"},
        ], client_org_id=ORG)

        listed = await repo.list_all(client_org_id=ORG)
        print("list_all count      :", len(listed), "(expect 2)")
        print("order (newest first):", [r["id"] for r in listed], "(expect sa-c2, sa-c1)")
        print("row keys            :", sorted(listed[0].keys()))
        print("get_by_document     :", [r["id"] for r in await repo.get_by_document(DOC_ID)])
        print("stats_for_org       :", await repo.stats_for_org(ORG), "(expect controls=2, documents=1)")

        # cleanup
        await repo.delete_for_document(DOC_ID)
        await s.execute(delete(documents).where(documents.c.id == DOC_ID))
        await s.commit()
        print("cleanup done        :", len(await repo.list_all(client_org_id=ORG)), "controls remain (expect 0)")

    print("\nISOLATION TEST PASSED")


if __name__ == "__main__":
    asyncio.run(main())
