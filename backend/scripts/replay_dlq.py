"""Replay dead-lettered pipeline batches.

When a batch task exhausts its retries, ``PipelineBatchTask``-style handling
publishes the failed batch's identity ({run_id, stage, batch_index}) to
``<queue>.dlq``. After fixing whatever caused the failure (bad credentials, a
downstream outage, …), run this to re-dispatch those batches on their original
queue.

Usage (from backend/):
    python scripts/replay_dlq.py --queue pipeline.llm.dlq
    python scripts/replay_dlq.py --queue pipeline.scoring.dlq --limit 50
    python scripts/replay_dlq.py --queue pipeline.extract.dlq --dry-run

It drains up to ``--limit`` messages (default: all currently queued) and, for
each, re-queues ``run_stage_batch(run_id, stage, batch_index)`` on the queue the
DLQ shadows (``pipeline.llm.dlq`` → ``pipeline.llm``).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from kombu import Connection, Queue  # noqa: E402

from iso_robot.config import get_settings  # noqa: E402


def _origin_queue(dlq_name: str) -> str:
    return dlq_name[:-4] if dlq_name.endswith(".dlq") else dlq_name


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay dead-lettered pipeline batches.")
    parser.add_argument("--queue", required=True, help="DLQ name, e.g. pipeline.llm.dlq")
    parser.add_argument("--limit", type=int, default=0, help="Max messages to replay (0 = all).")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be replayed; don't re-queue.")
    args = parser.parse_args()

    settings = get_settings()
    origin = _origin_queue(args.queue)

    # Import here so the CLI can run even if the app package is heavy to import.
    from iso_robot.pipeline.tasks_v2 import run_stage_batch

    replayed = 0
    with Connection(settings.celery_broker_url) as conn:
        dlq = Queue(args.queue, channel=conn.default_channel, no_declare=True)
        bound = dlq(conn.default_channel)
        while args.limit == 0 or replayed < args.limit:
            message = bound.get(no_ack=False)
            if message is None:
                break
            body = message.payload if isinstance(message.payload, dict) else {}
            run_id, stage, batch_index = body.get("run_id"), body.get("stage"), body.get("batch_index")
            if not (run_id and stage is not None and batch_index is not None):
                print(f"skip malformed DLQ message: {body!r}")
                message.ack()
                continue
            print(f"replay {stage}[{batch_index}] run={run_id} -> {origin}")
            if not args.dry_run:
                run_stage_batch.apply_async(args=[run_id, stage, batch_index], queue=origin)
            message.ack()
            replayed += 1

    print(f"\n{'Would replay' if args.dry_run else 'Replayed'} {replayed} message(s) from {args.queue}.")


if __name__ == "__main__":
    main()
