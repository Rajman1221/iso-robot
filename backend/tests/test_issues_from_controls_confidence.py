from __future__ import annotations

from iso_robot.domain.issue_confidence import CONFIDENCE_SOURCE_HEURISTIC, CONFIDENCE_SOURCE_LLM
from iso_robot.domain.issues_from_controls import _heuristic_batch, _normalize_llm_issues


def test_normalize_llm_issues_uses_llm_confidence() -> None:
    valid_ids = {"ctrl-1"}
    raw = {
        "issues": [
            {
                "title": "Access review gaps",
                "body": "Periodic reviews are incomplete.",
                "scope": "internal",
                "confidence": 0.82,
                "control_ids": ["ctrl-1"],
            }
        ]
    }
    out = _normalize_llm_issues(raw, valid_ids)
    assert len(out) == 1
    assert out[0]["confidence"] == 0.82
    assert out[0]["confidence_source"] == CONFIDENCE_SOURCE_LLM


def test_normalize_llm_issues_null_when_confidence_missing() -> None:
    valid_ids = {"ctrl-1"}
    raw = {
        "issues": [
            {
                "title": "Access review gaps",
                "body": "Periodic reviews are incomplete.",
                "scope": "internal",
                "control_ids": ["ctrl-1"],
            }
        ]
    }
    out = _normalize_llm_issues(raw, valid_ids)
    assert len(out) == 1
    assert out[0]["confidence_source"] == CONFIDENCE_SOURCE_HEURISTIC
    assert out[0]["confidence"] is None


def test_heuristic_batch_null_confidence() -> None:
    batch = [
        {"id": "ctrl-1", "control_text": "Internal audit reviews access quarterly."},
        {"id": "ctrl-2", "control_text": "Management approves privileged accounts."},
    ]
    out = _heuristic_batch(batch, sector_default="Finance", region_default="EU")
    assert len(out) == 1
    assert out[0]["confidence_source"] == CONFIDENCE_SOURCE_HEURISTIC
    assert out[0]["confidence"] is None
