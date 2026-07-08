"""Builds the Celery canvas for one ingest run:

    ingest_register
      -> chord(extract_controls per new document) -> issues_from_controls
      -> classify_issues -> generate_charts -> risk_discovery -> score_risks
      -> risk_tagging -> risk_owner_assignment -> pipeline_complete

Every stage signature is immutable (`.si`) — stages read all the context they
need from `pipeline_runs`/`pipeline_document_steps`, not from a chained return
value, so retries/replays never resend documents by accident. `on_error` is
attached to every signature (not just the chain head) so a failure at any
point — including inside the extraction chord — reliably flips the run to
`failed` via `pipeline_failed`.
"""

from __future__ import annotations

from typing import Any, List

from celery import chain, chord, group
from celery.canvas import Signature

from iso_robot.pipeline import tasks


def _guarded(sig: Signature, run_id: str) -> Signature:
    return sig.on_error(tasks.pipeline_failed.si(run_id))


def build_pipeline_canvas(run_id: str, documents: List[dict[str, Any]]) -> Signature:
    """Return the (not-yet-dispatched) canvas signature for one pipeline run."""
    stages: List[Signature] = [_guarded(tasks.ingest_register.si(run_id, documents), run_id)]

    if documents:
        extract_group = group(
            _guarded(
                tasks.extract_controls.si(
                    run_id,
                    doc["document_id"],
                    doc.get("filename"),
                    doc.get("document_registry_id") or None,
                ),
                run_id,
            )
            for doc in documents
        )
        stages.append(
            chord(extract_group, _guarded(tasks.issues_from_controls.si(run_id, documents), run_id))
        )
    else:
        # No new documents this run (e.g. all duplicates without force_reprocess) —
        # still run the downstream stages so status reflects a completed no-op.
        stages.append(_guarded(tasks.issues_from_controls.si(run_id, documents), run_id))

    stages.extend(
        [
            _guarded(tasks.classify_issues.si(run_id, documents), run_id),
            _guarded(tasks.generate_charts.si(run_id, documents), run_id),
            _guarded(tasks.risk_discovery.si(run_id, documents), run_id),
            _guarded(tasks.score_risks.si(run_id, documents), run_id),
            _guarded(tasks.risk_tagging.si(run_id, documents), run_id),
            _guarded(tasks.risk_owner_assignment.si(run_id, documents), run_id),
            _guarded(tasks.pipeline_complete.si(run_id), run_id),
        ]
    )
    return chain(*stages)


def enqueue_pipeline(run_id: str, documents: List[dict[str, Any]]) -> str:
    """Dispatch the canvas and return the root Celery task id."""
    canvas = build_pipeline_canvas(run_id, documents)
    async_result = canvas.apply_async()
    return async_result.id
