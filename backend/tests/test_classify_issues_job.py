"""Tests for classify_issues_job reclassify behaviour."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from iso_robot.config import get_settings
from iso_robot.domain.classify_issues import classify_issues_job
from iso_robot.repositories.issue_repository import IssueClassificationRepository, IssueRepository


@pytest.mark.asyncio
async def test_classify_issues_job_skips_already_classified_when_reclassify_false(
    db_session, org, monkeypatch: pytest.MonkeyPatch,
) -> None:
    issues = IssueRepository(db_session)
    cls_repo = IssueClassificationRepository(db_session)

    classified_id = str(uuid.uuid4())
    unclassified_id = str(uuid.uuid4())
    for iid, title in ((classified_id, "Already done"), (unclassified_id, "Needs work")):
        await issues.insert(
            issue_id=iid,
            risk_source_id=None,
            title=title,
            body="body",
            region_hint=None,
            raw_payload={"origin": "from_controls", "client_org_id": org["id"]},
            client_org_id=org["id"],
        )
    await cls_repo.insert(
        row_id=str(uuid.uuid4()),
        issue_id=classified_id,
        classification={"pestel_items": []},
        model_version="test",
    )

    mock_classify = AsyncMock(return_value={"pestel_items": []})
    monkeypatch.setattr("iso_robot.domain.classify_issues.classify_issue", mock_classify)

    count = await classify_issues_job(
        get_settings(),
        db_session,
        [classified_id, unclassified_id],
        reclassify=False,
    )

    assert count == 1
    mock_classify.assert_awaited_once()
    assert mock_classify.await_args.args[2] == unclassified_id


@pytest.mark.asyncio
async def test_classify_issues_job_reclassifies_all_when_reclassify_true(
    db_session, org, monkeypatch: pytest.MonkeyPatch,
) -> None:
    issues = IssueRepository(db_session)
    cls_repo = IssueClassificationRepository(db_session)

    issue_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    for iid in issue_ids:
        await issues.insert(
            issue_id=iid,
            risk_source_id=None,
            title="Issue",
            body="body",
            region_hint=None,
            raw_payload={"origin": "from_controls", "client_org_id": org["id"]},
            client_org_id=org["id"],
        )
        await cls_repo.insert(
            row_id=str(uuid.uuid4()),
            issue_id=iid,
            classification={"pestel_items": []},
            model_version="test",
        )

    mock_classify = AsyncMock(return_value={"pestel_items": []})
    monkeypatch.setattr("iso_robot.domain.classify_issues.classify_issue", mock_classify)

    count = await classify_issues_job(
        get_settings(),
        db_session,
        issue_ids,
        reclassify=True,
    )

    assert count == 2
    assert mock_classify.await_count == 2
