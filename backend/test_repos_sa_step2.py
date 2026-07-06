"""
Isolation test for the Step-2 converted repositories (risk_source, document,
issue_control, issue, issue_classification). Runs against whatever DATABASE_URL
points to (MSSQL primary). Creates throwaway rows, exercises every method with
parity assertions, then cleans up. Safe to re-run.

    cd backend
    $env:PYTHONPATH = "src"
    python test_repos_sa_step2.py
"""
import asyncio
import sys

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from sqlalchemy import delete, insert
from iso_robot.repositories.db import SessionFactory, DATABASE_URL, engine
from iso_robot.repositories.models import documents, controls, risk_sources, issues, issue_classifications, issue_controls
from iso_robot.repositories.risk_source_repository_sa import RiskSourceRepository
from iso_robot.repositories.document_repository_sa import DocumentRepository
from iso_robot.repositories.issue_control_repository_sa import IssueControlRepository
from iso_robot.repositories.issue_repository_sa import IssueRepository, IssueClassificationRepository

ORG = "test-org-sa"
DOC = "test-doc-step2"


async def _cleanup(s):
    await s.execute(delete(issue_controls).where(issue_controls.c.issue_id.in_(["i1", "i2", "i3"])))
    await s.execute(delete(issue_classifications).where(issue_classifications.c.issue_id.in_(["i1", "i2", "i3"])))
    await s.execute(delete(issues).where(issues.c.client_org_id == ORG))
    await s.execute(delete(controls).where(controls.c.document_id == DOC))
    await s.execute(delete(documents).where(documents.c.id == DOC))
    await s.execute(delete(risk_sources).where(risk_sources.c.id == "rs1"))
    await s.commit()


async def main():
    print(f"DATABASE_URL: {DATABASE_URL.split('://')[0]}://...")
    async with SessionFactory() as s:
        await _cleanup(s)

        rs = RiskSourceRepository(s)
        await rs.upsert(source_id="rs1", name="Alpha", source_type="feed", url="http://a")
        await rs.upsert(source_id="rs1", name="Alpha2", source_type=None, url=None)
        row = [r for r in await rs.list_all() if r["id"] == "rs1"][0]
        assert row["name"] == "Alpha2" and row["source_type"] == "feed" and row["url"] == "http://a"
        print("RiskSource         : OK")

        dr = DocumentRepository(s)
        did, new = await dr.upsert_by_sha256(doc_id=DOC, filename="f", path="/f", sha256="H-step2",
                                             mime_type=None, size_bytes=1, framework="ISO", status="local", source_url=None)
        assert (did, new) == (DOC, True)
        did2, new2 = await dr.upsert_by_sha256(doc_id="dX", filename="f2", path="/f2", sha256="H-step2",
                                               mime_type=None, size_bytes=2, framework=None, status="ready", source_url=None)
        assert (did2, new2) == (DOC, False)
        got = await dr.get_by_id(DOC)
        assert got["status"] == "ready" and got["framework"] == "ISO"
        print("Document           : OK")

        await s.execute(insert(controls).values(id="ctrlA", document_id=DOC, control_text="TEXT-A", section_ref="1.1", client_org_id=ORG, created_at="2026-01-01T00:00:00Z"))
        await s.execute(insert(controls).values(id="ctrlB", document_id=DOC, control_text="TEXT-B", section_ref="1.0", client_org_id=ORG, created_at="2026-01-01T00:00:00Z"))
        await s.commit()

        ir = IssueRepository(s)
        for iid in ("i1", "i2"):
            await ir.insert(issue_id=iid, risk_source_id=None, title=iid.upper(), body="b", client_org_id=ORG,
                            raw_payload={"source_document_id": DOC, "origin": "from_controls"})
        listed = await ir.list_all(client_org_id=ORG)
        assert len(listed) == 2 and "raw_payload" in listed[0] and "raw_payload_json" not in listed[0]
        assert len(await ir.list_all(source_document_id=DOC)) == 2
        assert await ir.stats_for_org(ORG) == {"issues": 2, "documents": 1}
        await ir.upsert(issue_id="i1", risk_source_id=None, title="I1b", body="b2",
                        raw_payload={"source_document_id": DOC, "origin": "from_controls"})
        assert (await ir.get_by_id("i1"))["title"] == "I1b"
        assert await ir.delete_derived_from_document(DOC) == 2
        assert await ir.stats_for_org(ORG) == {"issues": 0, "documents": 0}
        print("Issue              : OK")

        await ir.insert(issue_id="i3", risk_source_id=None, title="I3", body="b", client_org_id=ORG,
                        raw_payload={"source_document_id": DOC, "origin": "from_controls"})
        icr = IssueControlRepository(s)
        assert await icr.assign("i3", ["ctrlA", "ctrlB", "ctrlA", " "]) == 3
        assert await icr.list_control_texts_for_issue("i3") == ["TEXT-B", "TEXT-A"]
        await icr.clear("i3")
        assert await icr.list_control_texts_for_issue("i3") == []
        print("IssueControl       : OK")

        cr = IssueClassificationRepository(s)
        assert "i3" in await ir.list_ids_missing_classification()
        await cr.insert(row_id="cl1", issue_id="i3", classification={"x": 1}, model_version="v1")
        assert (await cr.get_latest_for_issue("i3"))["classification"] == {"x": 1}
        assert "i3" not in await ir.list_ids_missing_classification()
        assert (await cr.map_for_issues(["i3"]))["i3"]["classification"] == {"x": 1}
        print("Classification     : OK")

        await _cleanup(s)

    await engine.dispose()
    print("\nALL FOUR REPOS: PASSED")


if __name__ == "__main__":
    asyncio.run(main())
