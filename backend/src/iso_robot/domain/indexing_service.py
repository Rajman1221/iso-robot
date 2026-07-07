"""Indexing service: keeps the Milvus knowledge index in sync with the DB.

Core idea (see chatbot.pdf): the SQLite DB stays the source of truth and this
service is the single place that turns a DB row into one or more embedded chunks
in Milvus, scoped per ``client_org_id``. Every stage calls it after a successful
write, and a full ``reindex_org`` backfills everything.

Every public method is error-isolated: an indexing failure logs and returns 0,
it never propagates into the request that triggered it.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.domain.embedding_service import embed_texts, is_embedding_configured
from iso_robot.helpers.text_chunk import chunk_by_chars
from iso_robot.integrations.milvus_client import get_milvus_client
from iso_robot.repositories.vector_repository import VectorRepository, make_chunk_id

logger = logging.getLogger(__name__)

# Semantic "stage" labels stored on each chunk. The retrieval layer boosts by
# these based on question intent (risk / issue / control / ...).
STAGE_ORG = "org"
STAGE_CONTROL = "control"
STAGE_ISSUE = "issue"
STAGE_CLASSIFICATION = "classification"
STAGE_RISK = "risk"
STAGE_TAG = "tag"
STAGE_ASSIGNMENT = "assignment"

# Per-chunk size. Comfortably under the embedding model's token limit and the
# Milvus VARCHAR limit, with a little overlap so requirements aren't cut in half.
_CHUNK_CHARS = 4000
_CHUNK_OVERLAP = 300


def _clean(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _join_lines(lines: Sequence[str]) -> str:
    return "\n".join(line for line in (l.strip() for l in lines) if line)


def _names(items: Any) -> List[str]:
    """Extract human-readable names from a list of tag dicts or strings."""
    out: List[str] = []
    for item in items or []:
        if isinstance(item, dict):
            name = item.get("name")
            for key in ("name", "process_name", "function_name", "department_name",
                        "kpi_name", "region_name", "control_family_name"):
                if item.get(key):
                    name = item[key]
                    break
            if name:
                out.append(str(name))
        elif isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


class IndexingService:
    def __init__(
        self,
        settings: Settings,
        vector_repo: VectorRepository,
        conn: Optional[AsyncSession] = None,
    ) -> None:
        self._settings = settings
        self._vectors = vector_repo
        self._conn = conn

    @property
    def active(self) -> bool:
        """True only when both Milvus and embeddings are configured."""
        return self._vectors.enabled and is_embedding_configured(self._settings)

    # Core: chunk -> embed -> (delete old) -> upsert

    async def _index_entity(
        self,
        *,
        client_org_id: str,
        entity_type: str,
        entity_id: str,
        stage: str,
        source_table: str,
        text: str,
        updated_at: Optional[str] = None,
    ) -> int:
        if not self.active or not client_org_id or not entity_id:
            return 0
        try:
            chunks = chunk_by_chars(text or "", max_chars=_CHUNK_CHARS, overlap=_CHUNK_OVERLAP)
            # Remove any prior chunks first so updates/deletions don't leave stragglers.
            await self._vectors.delete_by_entity(
                entity_type=entity_type, entity_id=entity_id, client_org_id=client_org_id
            )
            if not chunks:
                return 0

            vectors = await embed_texts(self._settings, chunks)
            stamp = updated_at or ""
            records = [
                {
                    "id": make_chunk_id(entity_type, entity_id, i),
                    "vector": vectors[i],
                    "client_org_id": client_org_id,
                    "stage": stage,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "source_table": source_table,
                    "updated_at": stamp,
                    "text": chunk,
                }
                for i, chunk in enumerate(chunks)
            ]
            return await self._vectors.upsert(records)
        except Exception:  # noqa: BLE001 — indexing must never break the caller
            logger.exception(
                "Indexing failed for %s:%s (org=%s)", entity_type, entity_id, client_org_id
            )
            return 0

    async def remove_entity(self, *, client_org_id: str, entity_type: str, entity_id: str) -> bool:
        if not self._vectors.enabled:
            return False
        return await self._vectors.delete_by_entity(
            entity_type=entity_type, entity_id=entity_id, client_org_id=client_org_id
        )

    # Per-entity mappers (stage 0..10)

    async def index_org_profile(
        self,
        client_org_id: str,
        *,
        org: Optional[Dict[str, Any]] = None,
        demography: Optional[Dict[str, Any]] = None,
    ) -> int:
        org = org or {}
        demo = demography or {}
        lines = [
            f"Organisation profile for {_clean(org.get('name')) or client_org_id}.",
            f"Industry: {_clean(demo.get('industry') or org.get('industry'))}",
            f"Sub-industry: {_clean(demo.get('sub_industry'))}",
            f"Region: {_clean(demo.get('regulatory_region') or org.get('region'))}",
            f"Headquarters: {_clean(demo.get('headquarters_city'))} {_clean(demo.get('headquarters_country'))}".strip(),
            f"Employees: {_clean(demo.get('employee_count'))}",
            f"Annual revenue: {_clean(demo.get('annual_revenue'))}",
            f"Ownership: {_clean(demo.get('ownership_type'))}",
            f"Website: {_clean(demo.get('website'))}",
        ]
        functions = _names(demo.get("functions")) or _names(demo.get("function_catalog"))
        if functions:
            lines.append("Functions: " + ", ".join(functions))
        if demo.get("processes"):
            lines.append("Processes: " + ", ".join(_names(demo.get("processes"))))
        if demo.get("locations"):
            lines.append("Locations: " + ", ".join(_names(demo.get("locations"))))
        if demo.get("regulatory_frameworks"):
            lines.append("Regulatory frameworks: " + ", ".join(_names(demo.get("regulatory_frameworks"))))
        if demo.get("notes"):
            lines.append("Notes: " + _clean(demo.get("notes")))

        return await self._index_entity(
            client_org_id=client_org_id,
            entity_type="org",
            entity_id=client_org_id,
            stage=STAGE_ORG,
            source_table="business_demography",
            text=_join_lines(lines),
            updated_at=_clean(demo.get("updated_at") or org.get("created_at")),
        )

    async def index_control(self, client_org_id: str, control: Dict[str, Any]) -> int:
        control_id = _clean(control.get("id"))
        page = control.get("source_page")
        lines = [
            f"Control{f' (section {_clean(control.get('section_ref'))})' if control.get('section_ref') else ''}:",
            _clean(control.get("control_text")),
            f"Framework: {_clean(control.get('framework'))}" if control.get("framework") else "",
            f"Source page: {page}" if page is not None else "",
        ]
        return await self._index_entity(
            client_org_id=client_org_id,
            entity_type="control",
            entity_id=control_id,
            stage=STAGE_CONTROL,
            source_table="controls",
            text=_join_lines(lines),
            updated_at=_clean(control.get("created_at")),
        )

    async def index_controls(self, client_org_id: str, controls: Sequence[Dict[str, Any]]) -> int:
        total = 0
        for control in controls or []:
            total += await self.index_control(client_org_id, control)
        return total

    async def reindex_controls(self, client_org_id: str) -> int:
        """Replace all control chunks for an org with the current DB set.

        Control rows get fresh UUIDs on every extraction, so we clear the org's
        control chunks first to avoid orphaned vectors, then re-index.
        """
        if not self.active or self._conn is None or not client_org_id:
            return 0
        from iso_robot.repositories.control_repository import ControlRepository

        await self._vectors.delete_by_entity_type(
            client_org_id=client_org_id, entity_type="control"
        )
        controls = await ControlRepository(self._conn).list_all(
            limit=10000, client_org_id=client_org_id
        )
        return await self.index_controls(client_org_id, controls)

    async def index_issue(
        self,
        client_org_id: str,
        issue: Dict[str, Any],
        *,
        classification: Optional[Dict[str, Any]] = None,
        assessment: Optional[Dict[str, Any]] = None,
    ) -> int:
        issue_id = _clean(issue.get("id"))
        lines = [
            f"Issue: {_clean(issue.get('title'))}",
            _clean(issue.get("body")),
            f"Region: {_clean(issue.get('region_hint'))}" if issue.get("region_hint") else "",
        ]
        if classification:
            lines.append(_classification_summary(classification))
        if assessment:
            lines.append(_assessment_summary(assessment))
        return await self._index_entity(
            client_org_id=client_org_id,
            entity_type="issue",
            entity_id=issue_id,
            stage=STAGE_ISSUE,
            source_table="issues",
            text=_join_lines(lines),
            updated_at=_clean(issue.get("created_at")),
        )

    async def index_aggregate(
        self, client_org_id: str, classifications: Sequence[Dict[str, Any]]
    ) -> int:
        """Index an org-level PESTEL/SWOT/TVRA roll-up across all classifications."""
        text = _aggregate_text(classifications)
        return await self._index_entity(
            client_org_id=client_org_id,
            entity_type="aggregate",
            entity_id=client_org_id,
            stage=STAGE_CLASSIFICATION,
            source_table="issue_classifications",
            text=text,
        )

    async def reindex_aggregate(self, client_org_id: str) -> int:
        """Rebuild only the org-level classification aggregate from the DB."""
        if not self.active or self._conn is None or not client_org_id:
            return 0
        from iso_robot.repositories.issue_repository import (
            IssueClassificationRepository,
            IssueRepository,
        )

        issues = await IssueRepository(self._conn).list_all(
            limit=10000, client_org_id=client_org_id
        )
        cls_map = await IssueClassificationRepository(self._conn).map_for_issues(
            [str(i["id"]) for i in issues]
        )
        classifications = [
            v["classification"] for v in cls_map.values() if v.get("classification")
        ]
        return await self.index_aggregate(client_org_id, classifications)

    async def index_published_risk(self, client_org_id: str, risk: Dict[str, Any]) -> int:
        risk_id = _clean(risk.get("id"))
        lines = [
            f"Risk: {_clean(risk.get('risk_title'))}",
            _clean(risk.get("risk_description")),
            f"Rating: {_clean(risk.get('risk_rating'))} | Score: {_clean(risk.get('risk_score'))}",
        ]
        for label, key in (
            ("Mapped controls", "mapped_controls"),
            ("Mapped functions", "mapped_functions"),
            ("Mapped locations", "mapped_locations"),
            ("Mapped processes", "mapped_processes"),
            ("Process tags", "process_tags"),
            ("Function tags", "function_tags"),
            ("Department tags", "department_tags"),
            ("KPI tags", "kpi_tags"),
            ("Region tags", "region_tags"),
            ("Control family tags", "control_family_tags"),
        ):
            values = _names(risk.get(key))
            if values:
                lines.append(f"{label}: " + ", ".join(values))
        owner = risk.get("owner") if isinstance(risk.get("owner"), dict) else None
        if owner and owner.get("name"):
            lines.append(f"Owner: {_clean(owner.get('name'))} ({_clean(owner.get('title'))})")
        return await self._index_entity(
            client_org_id=client_org_id,
            entity_type="risk",
            entity_id=risk_id,
            stage=STAGE_RISK,
            source_table="risks",
            text=_join_lines(lines),
            updated_at=_clean(risk.get("updated_at") or risk.get("created_at")),
        )

    async def index_risk_tag(
        self, client_org_id: str, tag: Dict[str, Any], *, risk_title: Optional[str] = None
    ) -> int:
        tag_id = _clean(tag.get("id"))
        lines = [f"Risk tagging for risk {_clean(risk_title) or _clean(tag.get('risk_id'))}:"]
        for dim in ("process", "function", "department", "kpi", "region", "control_family"):
            values = _names(tag.get(f"{dim}_tags"))
            if values:
                lines.append(f"{dim.replace('_', ' ').title()} tags: " + ", ".join(values))
        if tag.get("rationale"):
            lines.append("Rationale: " + _clean(tag.get("rationale")))
        evidence = tag.get("evidence")
        if isinstance(evidence, list) and evidence:
            lines.append("Evidence: " + ", ".join(_clean(e) for e in evidence))
        return await self._index_entity(
            client_org_id=client_org_id,
            entity_type="risk_tag",
            entity_id=tag_id or _clean(tag.get("risk_id")),
            stage=STAGE_TAG,
            source_table="risk_tags",
            text=_join_lines(lines),
            updated_at=_clean(tag.get("updated_at") or tag.get("created_at")),
        )

    async def index_assignment(
        self, client_org_id: str, assignment: Dict[str, Any], *, risk_title: Optional[str] = None
    ) -> int:
        assignment_id = _clean(assignment.get("id"))
        owner = assignment.get("recommended_owner") if isinstance(
            assignment.get("recommended_owner"), dict
        ) else {}
        lines = [
            f"Risk owner assignment for risk {_clean(risk_title) or _clean(assignment.get('risk_id'))}:",
            f"Recommended owner: {_clean(owner.get('name'))} ({_clean(owner.get('title'))})".strip(),
            f"Status: {_clean(assignment.get('assignment_status'))}",
        ]
        matched = assignment.get("matched_on")
        if isinstance(matched, list) and matched:
            lines.append("Matched on: " + ", ".join(_clean(m) for m in matched))
        if assignment.get("rationale"):
            lines.append("Rationale: " + _clean(assignment.get("rationale")))
        return await self._index_entity(
            client_org_id=client_org_id,
            entity_type="assignment",
            entity_id=assignment_id or _clean(assignment.get("risk_id")),
            stage=STAGE_ASSIGNMENT,
            source_table="risk_assignments",
            text=_join_lines(lines),
            updated_at=_clean(assignment.get("updated_at") or assignment.get("created_at")),
        )

    # Full backfill

    async def reindex_org(self, client_org_id: str) -> Dict[str, Any]:
        """Rebuild the entire Milvus index for one org from the DB. Idempotent."""
        if not self.active:
            return {"status": "skipped", "reason": "milvus_or_embeddings_not_configured"}
        if self._conn is None:
            return {"status": "skipped", "reason": "no_db_connection"}

        # Import here to avoid import cycles at module load.
        from iso_robot.repositories.control_repository import ControlRepository
        from iso_robot.repositories.issue_repository import (
            IssueClassificationRepository,
            IssueRepository,
        )
        from iso_robot.repositories.org_repository import (
            DemographyRepository,
            OrgRepository,
            RiskRepository,
        )
        from iso_robot.repositories.risk_assignment_repository import RiskAssignmentRepository
        from iso_robot.repositories.risk_tagging_repository import RiskTagRepository

        conn = self._conn
        counts: Dict[str, int] = {}

        # Clean slate so deleted rows never linger in the index.
        await self._vectors.delete_by_org(client_org_id)

        # Stage 02 — org profile + demography
        org = await OrgRepository(conn).get_by_id(client_org_id)
        demography = await DemographyRepository(conn).get_by_org(client_org_id)
        counts["org"] = await self.index_org_profile(
            client_org_id, org=org, demography=demography
        )

        # Controls
        controls = await ControlRepository(conn).list_all(limit=10000, client_org_id=client_org_id)
        counts["controls"] = await self.index_controls(client_org_id, controls)

        # Issues + per-issue classification, then an org-level aggregate
        issues = await IssueRepository(conn).list_all(limit=10000, client_org_id=client_org_id)
        cls_repo = IssueClassificationRepository(conn)
        cls_map = await cls_repo.map_for_issues([str(i["id"]) for i in issues])
        issue_chunks = 0
        classifications: List[Dict[str, Any]] = []
        for issue in issues:
            cls = cls_map.get(str(issue["id"]), {}).get("classification")
            if cls:
                classifications.append(cls)
            issue_chunks += await self.index_issue(client_org_id, issue, classification=cls)
        counts["issues"] = issue_chunks
        counts["aggregate"] = await self.index_aggregate(client_org_id, classifications)

        # Published risks
        risks = await RiskRepository(conn).list_for_org(client_org_id, limit=10000)
        risk_chunks = 0
        risk_titles: Dict[str, str] = {}
        for risk in risks:
            risk_titles[str(risk["id"])] = _clean(risk.get("risk_title"))
            risk_chunks += await self.index_published_risk(client_org_id, risk)
        counts["risks"] = risk_chunks

        # Applied risk tags (latest per risk)
        tags = await RiskTagRepository(conn).list_for_org(client_org_id, status="applied", limit=10000)
        seen_tag_risks: set[str] = set()
        tag_chunks = 0
        for tag in tags:
            rid = str(tag.get("risk_id"))
            if rid in seen_tag_risks:
                continue
            seen_tag_risks.add(rid)
            tag_chunks += await self.index_risk_tag(
                client_org_id, tag, risk_title=risk_titles.get(rid)
            )
        counts["risk_tags"] = tag_chunks

        # Risk owner assignments (latest assigned per risk)
        assignments = await RiskAssignmentRepository(conn).list_for_org(
            client_org_id, status="assigned", limit=10000
        )
        seen_assign_risks: set[str] = set()
        assign_chunks = 0
        for assignment in assignments:
            rid = str(assignment.get("risk_id"))
            if rid in seen_assign_risks:
                continue
            seen_assign_risks.add(rid)
            assign_chunks += await self.index_assignment(
                client_org_id, assignment, risk_title=risk_titles.get(rid)
            )
        counts["assignments"] = assign_chunks

        total = sum(counts.values())
        return {"status": "completed", "client_org_id": client_org_id, "chunks": total, "by_entity": counts}


# Text builders


def _classification_summary(classification: Dict[str, Any]) -> str:
    parts: List[str] = []
    pestel = classification.get("pestel_items")
    if isinstance(pestel, list) and pestel:
        cats = sorted({_clean(i.get("category")) for i in pestel if isinstance(i, dict) and i.get("category")})
        if cats:
            parts.append("PESTEL categories: " + ", ".join(cats))
    swot = classification.get("swot")
    if isinstance(swot, dict):
        for key in ("strengths", "weaknesses", "opportunities", "threats"):
            titles = [_clean(x.get("title")) for x in (swot.get(key) or []) if isinstance(x, dict) and x.get("title")]
            if titles:
                parts.append(f"SWOT {key}: " + "; ".join(titles[:5]))
    tvra = classification.get("tvra")
    if isinstance(tvra, dict):
        threats = [_clean(x.get("title")) for x in (tvra.get("threats") or []) if isinstance(x, dict) and x.get("title")]
        if threats:
            parts.append("TVRA threats: " + "; ".join(threats[:5]))
    return _join_lines(parts)


def _assessment_summary(assessment: Dict[str, Any]) -> str:
    fields = [
        ("Inherent risk", assessment.get("inherent_risk")),
        ("Residual risk", assessment.get("residual_risk")),
        ("Likelihood", assessment.get("likelihood")),
        ("Consequence", assessment.get("consequence")),
        ("Risk response", assessment.get("risk_response")),
    ]
    parts = [f"{label}: {_clean(value)}" for label, value in fields if value]
    return ("Risk assessment — " + "; ".join(parts)) if parts else ""


def _aggregate_text(classifications: Sequence[Dict[str, Any]]) -> str:
    if not classifications:
        return ""
    pestel_counts: Dict[str, int] = {}
    swot_titles: Dict[str, List[str]] = {"strengths": [], "weaknesses": [], "opportunities": [], "threats": []}
    tvra_threats: List[str] = []
    for cls in classifications:
        for item in cls.get("pestel_items") or []:
            if isinstance(item, dict) and item.get("category"):
                cat = _clean(item["category"])
                pestel_counts[cat] = pestel_counts.get(cat, 0) + 1
        swot = cls.get("swot") if isinstance(cls.get("swot"), dict) else {}
        for key in swot_titles:
            for x in swot.get(key) or []:
                if isinstance(x, dict) and x.get("title"):
                    swot_titles[key].append(_clean(x["title"]))
        tvra = cls.get("tvra") if isinstance(cls.get("tvra"), dict) else {}
        for x in tvra.get("threats") or []:
            if isinstance(x, dict) and x.get("title"):
                tvra_threats.append(_clean(x["title"]))

    lines = ["Organisation risk classification summary (PESTEL / SWOT / TVRA)."]
    if pestel_counts:
        lines.append(
            "PESTEL distribution: "
            + ", ".join(f"{cat} ({n})" for cat, n in sorted(pestel_counts.items()))
        )
    for key, titles in swot_titles.items():
        if titles:
            uniq = list(dict.fromkeys(titles))[:10]
            lines.append(f"SWOT {key}: " + "; ".join(uniq))
    if tvra_threats:
        uniq = list(dict.fromkeys(tvra_threats))[:10]
        lines.append("Top TVRA threats: " + "; ".join(uniq))
    return _join_lines(lines)


# Factory


def build_indexing_service(
    settings: Settings, conn: Optional[AsyncSession] = None
) -> IndexingService:
    """Construct an IndexingService with a fresh Milvus-backed vector repo.

    Convenience for domain code (e.g. job runners) that already holds settings +
    a DB connection but not the DI-provided repositories.
    """
    vector_repo = VectorRepository(get_milvus_client(settings), settings)
    return IndexingService(settings, vector_repo, conn)
