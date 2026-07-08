"""Pipeline v2 batching support — additive columns + dedup/uniqueness indexes.

Adds:
  * issues.source_fingerprint (+ unique index on (client_org_id, source_fingerprint))
    for incremental, idempotent issue generation.
  * pipeline_document_steps.batch_index for batch-level step rows.
  * pipeline_runs.stage_totals_json for per-stage batch progress + resumability.
  * unique index on document_registry (client_org_id, sha256) — the dedup ledger
    was only non-unique-indexed before.

Every step is guarded by an introspection check because revision 0001 builds the
schema straight from the live ORM metadata (``Base.metadata.create_all``): a
*fresh* database already has these columns after 0001, while a database created
by an *earlier* 0001 (before these columns existed) does not. Guarding makes this
revision a correct no-op in the first case and a real migration in the second.

Revision ID: 0002_pipeline_batching
Revises: 0001_initial_schema
Create Date: 2026-07-08
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002_pipeline_batching"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in _inspector().get_columns(table)}


def _has_index(table: str, index: str) -> bool:
    return index in {i["name"] for i in _inspector().get_indexes(table)}


def _dedupe_document_registry() -> None:
    """Drop duplicate (client_org_id, sha256) rows (keep earliest) before the
    unique index, so it can't fail on legacy dupes. Window functions are
    available on Postgres and SQLite >= 3.25."""
    op.execute(
        """
        DELETE FROM document_registry
        WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY client_org_id, sha256
                           ORDER BY created_at ASC, id ASC
                       ) AS rn
                FROM document_registry
            ) ranked
            WHERE ranked.rn > 1
        )
        """
    )


def upgrade() -> None:
    if not _has_column("issues", "source_fingerprint"):
        op.add_column("issues", sa.Column("source_fingerprint", sa.String(length=64), nullable=True))
    if not _has_index("issues", "uq_issues_org_fingerprint"):
        op.create_index(
            "uq_issues_org_fingerprint",
            "issues",
            ["client_org_id", "source_fingerprint"],
            unique=True,
        )

    if not _has_column("pipeline_document_steps", "batch_index"):
        op.add_column(
            "pipeline_document_steps", sa.Column("batch_index", sa.Integer(), nullable=True)
        )
    if not _has_column("pipeline_runs", "stage_totals_json"):
        op.add_column(
            "pipeline_runs",
            sa.Column("stage_totals_json", sa.JSON(), nullable=False, server_default="{}"),
        )

    if not _has_index("document_registry", "uq_document_registry_org_sha256"):
        _dedupe_document_registry()
        op.create_index(
            "uq_document_registry_org_sha256",
            "document_registry",
            ["client_org_id", "sha256"],
            unique=True,
        )


def downgrade() -> None:
    if _has_index("document_registry", "uq_document_registry_org_sha256"):
        op.drop_index("uq_document_registry_org_sha256", table_name="document_registry")
    if _has_column("pipeline_runs", "stage_totals_json"):
        op.drop_column("pipeline_runs", "stage_totals_json")
    if _has_column("pipeline_document_steps", "batch_index"):
        op.drop_column("pipeline_document_steps", "batch_index")
    if _has_index("issues", "uq_issues_org_fingerprint"):
        op.drop_index("uq_issues_org_fingerprint", table_name="issues")
    if _has_column("issues", "source_fingerprint"):
        op.drop_column("issues", "source_fingerprint")
