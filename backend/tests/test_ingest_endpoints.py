"""Endpoint tests for `POST /ingest/{client_org_id}` and
`GET /pipeline/status/{client_org_id}`.

These exercise the HTTP contract (auth, dedup, one-active-run-per-org,
response shape) in isolation from the Celery canvas: `enqueue_pipeline` is
monkeypatched to a no-op stub, because with `CELERY_TASK_ALWAYS_EAGER=true`
the real canvas would run fully synchronously *inside* the request handler
(and would need real Azure/OpenAI credentials). The canvas itself is covered
separately by `test_pipeline_canvas_eager.py`.
"""

from __future__ import annotations

import io
from typing import Any

import pytest
from fastapi.testclient import TestClient

from conftest import unique_id
from iso_robot.main import app


def _pdf_bytes(tag: str) -> bytes:
    return f"%PDF-1.4 fake content {tag}".encode("utf-8")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    calls: list[dict[str, Any]] = []

    def _fake_enqueue(run_id: str, documents: list[dict[str, Any]]) -> str:
        calls.append({"run_id": run_id, "documents": documents})
        return f"fake-task-{run_id}"

    monkeypatch.setattr("iso_robot.handlers.pipeline.enqueue_pipeline", _fake_enqueue)

    with TestClient(app) as test_client:
        test_client.enqueue_calls = calls  # type: ignore[attr-defined]
        yield test_client


def _headers(org_id: str) -> dict[str, str]:
    return {
        "Authorization": "Bearer external-token",
        "X-User-Id": "test-user",
        "X-User-Email": "test@example.com",
        "X-Org-Id": org_id,
    }


async def _create_org(db_session) -> dict:
    from iso_robot.repositories.org_repository import OrgRepository

    slug = unique_id("ingest-org")
    return await OrgRepository(db_session).create(name=f"Ingest Org {slug}", slug=slug)


@pytest.mark.asyncio
async def test_ingest_unknown_org_returns_404(client: TestClient) -> None:
    org_id = unique_id("missing-org")
    resp = client.post(
        f"/api/v1/ingest/{org_id}",
        headers=_headers(org_id),
        files={"file": ("doc.pdf", io.BytesIO(_pdf_bytes("a")), "application/pdf")},
    )
    assert resp.status_code == 404
    assert resp.json()["code"] == "CLIENT_ORG_NOT_FOUND"


@pytest.mark.asyncio
async def test_ingest_org_mismatch_header_forbidden(db_session) -> None:
    # Org-id match is enforced even when VERIFY_MOCK=true (see conftest.py).
    org = await _create_org(db_session)
    headers = _headers(org["id"])
    headers["X-Org-Id"] = "some-other-org"
    with TestClient(app) as raw_client:
        resp = raw_client.post(
            f"/api/v1/ingest/{org['id']}",
            headers=headers,
            files={"file": ("doc.pdf", io.BytesIO(_pdf_bytes("a")), "application/pdf")},
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_ingest_rejects_non_pdf(client: TestClient, db_session) -> None:
    org = await _create_org(db_session)
    resp = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("notes.txt", io.BytesIO(b"hello"), "text/plain")},
    )
    assert resp.status_code == 202
    data = resp.json()["data"]
    assert data["documents"][0]["status"] == "rejected"
    assert data["new_documents"] == 0
    assert data["total_documents"] == 1


@pytest.mark.asyncio
async def test_ingest_new_document_queues_pipeline(client: TestClient, db_session) -> None:
    org = await _create_org(db_session)
    resp = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("policy.pdf", io.BytesIO(_pdf_bytes("unique-1")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert resp.status_code == 202
    body = resp.json()
    data = body["data"]
    assert data["client_org_id"] == org["id"]
    assert data["status"] == "queued"
    assert data["save_to_storage"] is False
    assert data["total_documents"] == 1
    assert data["new_documents"] == 1
    assert data["skipped_duplicate_documents"] == 0
    doc = data["documents"][0]
    assert doc["status"] == "queued"
    assert doc["filename"] == "policy.pdf"
    assert len(doc["sha256"]) == 64
    assert data["status_url"].endswith(f"/api/v1/pipeline/status/{org['id']}?pipeline_run_id={data['pipeline_run_id']}")
    assert client.enqueue_calls == [  # type: ignore[attr-defined]
        {
            "run_id": data["pipeline_run_id"],
            "documents": [
                {
                    "document_id": doc["document_id"],
                    "filename": doc["filename"],
                    "document_registry_id": doc["document_registry_id"],
                }
            ],
        }
    ]


@pytest.mark.asyncio
async def test_ingest_duplicate_document_is_skipped(client: TestClient, db_session) -> None:
    org = await _create_org(db_session)
    content = _pdf_bytes("dup-1")

    first = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("dup.pdf", io.BytesIO(content), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert first.status_code == 202

    # A run only stays "active" (queued/running) until pipeline_complete runs it
    # to "completed" — since enqueue_pipeline is stubbed out, mark it completed
    # here to unblock a second ingest call, mirroring what the real canvas does.
    from iso_robot.repositories.pipeline_repository import PipelineRunRepository

    run_id = first.json()["data"]["pipeline_run_id"]
    await PipelineRunRepository(db_session).mark_completed(run_id)

    second = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("dup.pdf", io.BytesIO(content), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert second.status_code == 202
    data = second.json()["data"]
    assert data["new_documents"] == 0
    assert data["skipped_duplicate_documents"] == 1
    assert data["documents"][0]["status"] == "duplicate"
    # Note: the dedup fast-path in `_register_document` returns the registry
    # row's `times_seen` as-is without calling `registry_repo.upsert()` (that
    # only happens on the new/force_reprocess path), so a plain duplicate hit
    # does not bump the counter — this pins down current behavior.
    assert data["documents"][0]["times_seen"] == 1


@pytest.mark.asyncio
async def test_ingest_conflicts_while_run_active(client: TestClient, db_session) -> None:
    org = await _create_org(db_session)
    first = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("a.pdf", io.BytesIO(_pdf_bytes("active-1")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert first.status_code == 202

    second = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("b.pdf", io.BytesIO(_pdf_bytes("active-2")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert second.status_code == 409
    assert second.json()["code"] == "PIPELINE_RUN_IN_PROGRESS"


@pytest.mark.asyncio
async def test_ingest_missing_auth_headers_unauthorized() -> None:
    with TestClient(app) as raw_client:
        resp = raw_client.post(
            "/api/v1/ingest/some-org",
            files={"file": ("a.pdf", io.BytesIO(_pdf_bytes("x")), "application/pdf")},
        )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_pipeline_status_unknown_org_404(client: TestClient) -> None:
    org_id = unique_id("missing-status-org")
    resp = client.get(f"/api/v1/pipeline/status/{org_id}", headers=_headers(org_id))
    assert resp.status_code == 404
    assert resp.json()["code"] == "CLIENT_ORG_NOT_FOUND"


@pytest.mark.asyncio
async def test_pipeline_status_no_runs_yet_404(client: TestClient, db_session) -> None:
    org = await _create_org(db_session)
    resp = client.get(f"/api/v1/pipeline/status/{org['id']}", headers=_headers(org["id"]))
    assert resp.status_code == 404
    assert resp.json()["code"] == "PIPELINE_RUN_NOT_FOUND"


@pytest.mark.asyncio
async def test_pipeline_status_latest_run_reports_progress(client: TestClient, db_session) -> None:
    org = await _create_org(db_session)
    ingest_resp = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("c.pdf", io.BytesIO(_pdf_bytes("status-1")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    run_id = ingest_resp.json()["data"]["pipeline_run_id"]

    status_resp = client.get(f"/api/v1/pipeline/status/{org['id']}", headers=_headers(org["id"]))
    assert status_resp.status_code == 200
    data = status_resp.json()["data"]
    assert data["pipeline_run_id"] == run_id
    assert data["status"] == "queued"
    assert data["current_stage"] == "ingest_register"
    assert data["progress_percent"] == 0
    assert data["steps"] == []

    # Explicit ?pipeline_run_id= must resolve the same run.
    by_id_resp = client.get(
        f"/api/v1/pipeline/status/{org['id']}",
        headers=_headers(org["id"]),
        params={"pipeline_run_id": run_id},
    )
    assert by_id_resp.status_code == 200
    assert by_id_resp.json()["data"]["pipeline_run_id"] == run_id


@pytest.mark.asyncio
async def test_pipeline_status_completed_run_is_100_percent(client: TestClient, db_session) -> None:
    from iso_robot.repositories.pipeline_repository import PipelineRunRepository

    org = await _create_org(db_session)
    ingest_resp = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("d.pdf", io.BytesIO(_pdf_bytes("status-2")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    run_id = ingest_resp.json()["data"]["pipeline_run_id"]
    await PipelineRunRepository(db_session).mark_completed(run_id)

    status_resp = client.get(f"/api/v1/pipeline/status/{org['id']}", headers=_headers(org["id"]))
    data = status_resp.json()["data"]
    assert data["status"] == "completed"
    assert data["progress_percent"] == 100
    assert data["completed_at"] is not None


@pytest.mark.asyncio
async def test_pipeline_status_run_belonging_to_another_org_is_404(client: TestClient, db_session) -> None:
    org_a = await _create_org(db_session)
    org_b = await _create_org(db_session)

    ingest_resp = client.post(
        f"/api/v1/ingest/{org_a['id']}",
        headers=_headers(org_a["id"]),
        files={"file": ("e.pdf", io.BytesIO(_pdf_bytes("cross-org")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    run_id = ingest_resp.json()["data"]["pipeline_run_id"]

    resp = client.get(
        f"/api/v1/pipeline/status/{org_b['id']}",
        headers=_headers(org_b["id"]),
        params={"pipeline_run_id": run_id},
    )
    assert resp.status_code == 404
    assert resp.json()["code"] == "PIPELINE_RUN_NOT_FOUND"


@pytest.mark.asyncio
async def test_verify_mock_skips_external_http_call(client: TestClient, db_session, monkeypatch) -> None:
    """With VERIFY_MOCK=true (conftest), verify_user must never be invoked."""

    async def _fail_if_called(*args, **kwargs):
        raise AssertionError("verify_user should not be called when VERIFY_MOCK=true")

    monkeypatch.setattr("iso_robot.handlers.pipeline_auth.verify_user", _fail_if_called)

    org = await _create_org(db_session)
    resp = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("mock.pdf", io.BytesIO(_pdf_bytes("verify-mock")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_cancel_active_pipeline_run(client: TestClient, db_session) -> None:
    org = await _create_org(db_session)
    ingest_resp = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("stuck.pdf", io.BytesIO(_pdf_bytes("cancel-1")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    run_id = ingest_resp.json()["data"]["pipeline_run_id"]

    cancel_resp = client.post(f"/api/v1/pipeline/cancel/{org['id']}", headers=_headers(org["id"]))
    assert cancel_resp.status_code == 200
    data = cancel_resp.json()["data"]
    assert data["pipeline_run_id"] == run_id
    assert data["status"] == "failed"
    assert data["error"] == "Cancelled by user"

    retry_resp = client.post(
        f"/api/v1/ingest/{org['id']}",
        headers=_headers(org["id"]),
        files={"file": ("retry.pdf", io.BytesIO(_pdf_bytes("cancel-2")), "application/pdf")},
        data={"save_to_storage": "false"},
    )
    assert retry_resp.status_code == 202


@pytest.mark.asyncio
async def test_cancel_pipeline_no_active_run_404(client: TestClient, db_session) -> None:
    org = await _create_org(db_session)
    resp = client.post(f"/api/v1/pipeline/cancel/{org['id']}", headers=_headers(org["id"]))
    assert resp.status_code == 404
    assert resp.json()["code"] == "NO_ACTIVE_PIPELINE_RUN"
