"""Unit tests for pipeline progress helpers: stage_summary and nested_stages."""

from __future__ import annotations

from iso_robot.observability.pipeline_progress import nested_stages, stage_summary


def test_nested_stages_empty() -> None:
    assert nested_stages([]) == []


def test_nested_stages_omits_finalize_from_batches() -> None:
    raw = [
        {
            "stage": "classify_issues",
            "batch_index": 0,
            "status": "completed",
            "result_json": {"item_ids": ["a", "b"]},
            "error": None,
        },
        {
            "stage": "classify_issues",
            "batch_index": 1,
            "status": "completed",
            "result_json": {"item_ids": ["c"]},
            "error": None,
        },
        {
            "stage": "classify_issues",
            "batch_index": None,
            "status": "completed",
            "result_json": {"batches": 2},
            "error": None,
        },
    ]
    stages = nested_stages(raw)
    assert len(stages) == 1
    stage = stages[0]
    assert stage["stage"] == "classify_issues"
    assert stage["status"] == "completed"
    assert stage["batch_count"] == 2
    assert stage["failed_batches"] == 0
    assert stage["batches"] == [
        {"batch_index": 0, "status": "completed", "item_count": 2, "error": None},
        {"batch_index": 1, "status": "completed", "item_count": 1, "error": None},
    ]
    # Slim batch objects only expose the four planned fields.
    assert set(stage["batches"][0].keys()) == {"batch_index", "status", "item_count", "error"}


def test_nested_stages_failed_batches_and_running_status() -> None:
    raw = [
        {
            "stage": "score_risks",
            "batch_index": 0,
            "status": "completed",
            "result_json": {"item_ids": ["i1"]},
            "error": None,
        },
        {
            "stage": "score_risks",
            "batch_index": 1,
            "status": "failed",
            "result_json": {"item_ids": ["i2", "i3"]},
            "error": "boom",
        },
        {
            "stage": "score_risks",
            "batch_index": 2,
            "status": "running",
            "result_json": {"item_ids": ["i4"]},
            "error": None,
        },
    ]
    [stage] = nested_stages(raw)
    assert stage["status"] == "running"
    assert stage["batch_count"] == 3
    assert stage["failed_batches"] == 1
    assert stage["batches"][1]["error"] == "boom"
    assert stage["batches"][1]["item_count"] == 2


def test_nested_stages_orders_by_pipeline_stages() -> None:
    raw = [
        {"stage": "score_risks", "batch_index": 0, "status": "completed", "result_json": {}, "error": None},
        {"stage": "extract_controls", "batch_index": 0, "status": "completed", "result_json": {}, "error": None},
        {"stage": "ingest_register", "batch_index": None, "status": "completed", "result_json": {}, "error": None},
    ]
    stages = nested_stages(raw)
    assert [s["stage"] for s in stages] == ["ingest_register", "extract_controls", "score_risks"]
    assert stages[0]["batch_count"] == 0
    assert stages[0]["batches"] == []


def test_nested_stages_item_count_null_without_item_ids() -> None:
    raw = [
        {
            "stage": "generate_charts",
            "batch_index": 0,
            "status": "completed",
            "result_json": {"ok": True},
            "error": None,
        }
    ]
    [stage] = nested_stages(raw)
    assert stage["batches"][0]["item_count"] is None


def test_stage_summary_still_counts_finalize_rows() -> None:
    """stage_summary is unchanged: document_count includes finalize rows."""
    raw = [
        {"stage": "extract_controls", "status": "completed", "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T00:01:00Z"},
        {"stage": "extract_controls", "status": "completed", "started_at": "2026-01-01T00:01:00Z", "completed_at": "2026-01-01T00:01:01Z"},
    ]
    summary = stage_summary(raw)
    assert len(summary) == 1
    assert summary[0]["document_count"] == 2
