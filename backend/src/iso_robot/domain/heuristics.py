"""
Rule-based fallbacks when Azure OpenAI or Document Intelligence are unavailable (e.g. 401).
Produces the same JSON shapes as LLM-driven pipelines so the UI stays consistent.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

# --- Classification ---

_PESTEL_KEYWORDS: Dict[str, List[str]] = {
    "Political": ["political", "sanction", "government", "geopolit", "policy", "regulation", "authority", "minister"],
    "Economic": ["economic", "trade", "market", "financial", "cost", "revenue", "price", "inflation", "tariff", "gdp"],
    "Social": ["social", "crew", "labour", "workforce", "community", "health", "safety", "welfare", "personnel"],
    "Technological": ["technolog", "cyber", "digital", "software", "system", "ict", "automation", "data", "ai ", "it "],
    "Environmental": ["environment", "climate", "emission", "carbon", "pollution", "sustainability", "green", "ecological"],
    "Legal": ["legal", "compliance", "regulation", "law", "liability", "contract", "audit", "standard", "iso ", "imo "],
}

# Two template items per PESTEL category: (title_template, description_template, impact, direction)
_PESTEL_TEMPLATES: Dict[str, List[Tuple[str, str, str, str]]] = {
    "Political": [
        ("Regulatory and government policy risk", "Changes in government policy and regulatory frameworks may directly affect operational requirements and compliance obligations. Organisations must monitor policy shifts and engage with authorities proactively.", "high", "negative"),
        ("Geopolitical instability exposure", "Ongoing geopolitical tensions in key operating regions can disrupt trade lanes, trigger sanctions, and increase operational risk. Contingency planning and scenario modelling are required.", "high", "negative"),
        ("Sanctions and trade restriction risk", "Evolving sanctions regimes may restrict access to suppliers, markets, or counterparties. Compliance with OFAC, EU, and UN sanctions lists must be continuously verified.", "extreme", "negative"),
    ],
    "Economic": [
        ("Operational cost volatility", "Fluctuations in fuel, freight, and input costs affect operating margins. Cost hedging strategies and supplier diversification are important mitigants.", "high", "negative"),
        ("Market demand uncertainty", "Shifts in global trade volumes and consumer demand can impact revenue forecasts and capacity utilisation. Regular market intelligence updates are needed.", "medium", "mixed"),
        ("Supply chain financial risk", "Financial instability among key suppliers or counterparties creates concentration risk. Credit assessments and supply chain monitoring are essential controls.", "high", "negative"),
    ],
    "Social": [
        ("Workforce capability and retention", "Attracting and retaining skilled personnel remains a challenge. Talent shortages can delay operations and increase training costs.", "medium", "negative"),
        ("Health, safety, and welfare obligations", "Regulatory and ethical obligations around crew and personnel health and safety require robust management systems and regular audits.", "high", "negative"),
        ("Stakeholder expectations and reputation risk", "Growing stakeholder expectations around ESG performance and transparency require proactive communication and management.", "medium", "mixed"),
    ],
    "Technological": [
        ("Cybersecurity and data breach risk", "Increasing digitalisation exposes operations to cyber threats including ransomware, phishing, and unauthorised access. Layered security controls and incident response planning are essential.", "extreme", "negative"),
        ("Legacy systems and technical debt", "Aging IT infrastructure may limit operational agility and increase the risk of system failures. A structured modernisation roadmap should be maintained.", "high", "negative"),
        ("Technology adoption and disruption", "Emerging technologies (AI, IoT, automation) present opportunities for efficiency gains but require investment and change management to realise benefits.", "medium", "mixed"),
    ],
    "Environmental": [
        ("Climate change transition risk", "Regulatory pressure to reduce emissions and transition to lower-carbon operations may require significant capital investment and operational changes.", "high", "negative"),
        ("Environmental incident and pollution liability", "Operations carry exposure to environmental incidents that could trigger regulatory penalties, clean-up costs, and reputational damage.", "extreme", "negative"),
        ("Physical climate risk exposure", "Extreme weather events and changing climate patterns may disrupt operations, supply chains, and infrastructure. Resilience planning is required.", "medium", "negative"),
    ],
    "Legal": [
        ("Regulatory compliance obligations", "Non-compliance with applicable legal frameworks and standards can result in fines, sanctions, and operational restrictions. A compliance management system should be in place.", "high", "negative"),
        ("Contractual liability and dispute risk", "Complex contractual arrangements with suppliers, clients, and partners create potential liability exposure. Legal review and clear dispute resolution mechanisms are important.", "medium", "negative"),
        ("Evolving international standards requirements", "International standards (ISO, IMO, ILO) are regularly updated. Failure to maintain certification or adapt to new requirements creates legal and reputational risk.", "medium", "negative"),
    ],
}

_GEO_KEYWORDS = [
    ("red sea", "Red_Sea"),
    ("gulf", "Middle_East_Gulf"),
    ("middle east", "Middle_East"),
    ("europe", "Europe"),
    ("uk ", "United_Kingdom"),
    ("united kingdom", "United_Kingdom"),
    ("china", "China"),
    ("russia", "Russia"),
    ("ukraine", "Ukraine"),
    ("africa", "Africa"),
    ("americas", "Americas"),
    ("imo", "IMO_Global"),
]

_GLOBAL_KEYWORDS = [
    ("sanction", "Trade_compliance"),
    ("cyber", "Cyber_global"),
    ("climate", "Climate_transition"),
    ("supply chain", "Supply_chain"),
    ("pandemic", "Health_systemic"),
    ("maritime", "Maritime_Shipping"),
]

_NEGATIVE_HINTS = (
    "tension", "war", "sanction", "penalty", "fine", "breach", "attack", "disruption",
    "delay", "decline", "volatility", "spike", "ransomware", "phishing", "violation",
    "non-conform", "outage", "exposure", "risk", "threat",
)

_SWOT_TEMPLATES = {
    "strengths": [
        ("Established compliance framework", "The organisation has existing compliance processes that provide a structured baseline for managing this risk."),
        ("Experienced management team", "Experienced leadership with sector knowledge supports informed decision-making and risk mitigation."),
        ("Established supplier and partner network", "Long-standing relationships with key suppliers and partners provide resilience during disruptions."),
        ("ISO/international certification", "Existing certifications demonstrate a commitment to quality and risk management best practices."),
        ("Documented risk management procedures", "Formalised risk registers and procedures enable systematic monitoring and escalation of issues."),
    ],
    "weaknesses": [
        ("Dependency on external data and feeds", "Reliance on third-party data sources creates vulnerability if feeds are delayed, inaccurate, or unavailable."),
        ("Limited internal monitoring capacity", "Current staffing and tooling may be insufficient for continuous monitoring of rapidly evolving risk signals."),
        ("Manual processes and data silos", "Manual workflows and disconnected data systems slow response times and increase the risk of errors."),
        ("Incomplete risk coverage across subsidiaries", "Inconsistent risk management practices across business units create gaps in overall risk posture."),
        ("Insufficient incident response testing", "Lack of regular simulation exercises limits confidence in incident response and business continuity plans."),
    ],
    "opportunities": [
        ("Early warning and proactive risk management", "Timely identification of this risk enables proactive mitigation before operational impact occurs."),
        ("Technology-enabled monitoring improvements", "Adoption of automated monitoring and analytics tools can improve risk detection speed and accuracy."),
        ("Regulatory engagement and positioning", "Proactive engagement with regulators can shape favourable policy outcomes and demonstrate good governance."),
        ("Supply chain diversification", "Addressing this risk creates an opportunity to diversify suppliers and reduce concentration exposure."),
        ("Enhanced stakeholder confidence", "Demonstrating robust risk management in this area strengthens relationships with investors, clients, and regulators."),
    ],
    "threats": [
        ("Regulatory enforcement action", "Increased regulatory scrutiny could result in fines, licence conditions, or operational restrictions."),
        ("Reputational and media risk", "Failure to manage this issue effectively could attract negative media coverage and damage brand reputation."),
        ("Competitive disadvantage", "Competitors that adapt more quickly to emerging risks may gain market advantage."),
        ("Cascading supply chain disruption", "This risk has potential to trigger downstream disruptions across interconnected supply chain partners."),
        ("Cost escalation", "Remediation costs, insurance premiums, and compliance expenditure may increase if this risk is not managed proactively."),
    ],
}

_TVRA_THREAT_TEMPLATES = [
    {"title": "Insider threat and unauthorised access", "actor": "Malicious insider", "vectors": [], "likelihood": "medium", "impact": "high"},
    {"title": "Phishing and social engineering attack", "actor": "Cybercriminal group", "vectors": ["T1566", "T1078"], "likelihood": "high", "impact": "high"},
    {"title": "Ransomware and data encryption attack", "actor": "Ransomware gang", "vectors": ["T1486", "T1190"], "likelihood": "medium", "impact": "extreme"},
    {"title": "Third-party and supply chain compromise", "actor": "Nation-state / criminal", "vectors": ["T1195"], "likelihood": "medium", "impact": "high"},
    {"title": "Regulatory enforcement and sanctions action", "actor": "Regulatory authority", "vectors": [], "likelihood": "medium", "impact": "high"},
]

_TVRA_VULN_TEMPLATES = [
    {"title": "Insufficient access control and privilege management", "description": "Weak identity and access management controls increase the risk of unauthorised access to sensitive systems and data."},
    {"title": "Unpatched systems and software vulnerabilities", "description": "Delayed patch management leaves known vulnerabilities exploitable, increasing the attack surface."},
    {"title": "Inadequate third-party risk assessment", "description": "Insufficient due diligence on suppliers and partners creates exposure to third-party risks that may propagate into operations."},
    {"title": "Limited real-time monitoring and alerting", "description": "Gaps in monitoring coverage reduce detection capability for threats and anomalous activity."},
    {"title": "Business continuity and recovery gaps", "description": "Incomplete or untested business continuity plans may extend recovery time following a disruptive event."},
]


def heuristic_classify_issue(
    title: Optional[str],
    body: Optional[str],
    region_hint: Optional[str],
) -> Dict[str, Any]:
    text = f"{title or ''} {body or ''} {region_hint or ''}".lower()
    title_short = (title or "this issue")[:80]

    # Build pestel_items: always generate items for all 6 categories,
    # prioritising categories whose keywords appear in the text.
    pestel_items: List[Dict[str, Any]] = []
    for cat, keywords in _PESTEL_KEYWORDS.items():
        matched = any(k in text for k in keywords)
        templates = _PESTEL_TEMPLATES[cat]
        for i, (tmpl_title, tmpl_desc, tmpl_impact, tmpl_dir) in enumerate(templates):
            # For unmatched categories, still include but tone down impact
            if not matched and i >= 2:
                continue  # Only include first 2 templates for non-matched categories
            negative = any(k in text for k in _NEGATIVE_HINTS)
            direction = tmpl_dir if (matched or negative) else "neutral"
            impact = tmpl_impact if matched else "medium"
            pestel_items.append({
                "category": cat,
                "title": tmpl_title,
                "description": tmpl_desc,
                "impact": impact,
                "direction": direction,
            })

    # SWOT
    swot: Dict[str, List[Dict[str, Any]]] = {}
    for quad, templates in _SWOT_TEMPLATES.items():
        swot[quad] = [{"title": t, "description": d} for t, d in templates]

    # TVRA
    threats = [dict(t) for t in _TVRA_THREAT_TEMPLATES]
    vulns = [dict(v) for v in _TVRA_VULN_TEMPLATES]

    # Add issue-specific threat from title
    if title:
        t = title.strip()[:80]
        threats.insert(0, {
            "title": f"Risk materialisation: {t}",
            "actor": "Multiple threat actors",
            "vectors": [],
            "likelihood": "high" if any(k in text for k in _NEGATIVE_HINTS) else "medium",
            "impact": "high",
        })

    # Geo
    geopolitical: List[str] = []
    for key, tag in _GEO_KEYWORDS:
        if key in text:
            geopolitical.append(tag)
    if region_hint and region_hint.strip():
        geopolitical.append(region_hint.strip().replace(" ", "_")[:48])

    global_labels: List[str] = []
    for key, tag in _GLOBAL_KEYWORDS:
        if key in text and tag not in global_labels:
            global_labels.append(tag)
    if not global_labels:
        global_labels = ["Operational_monitoring"]

    tvra = {
        "threats": threats[:6],
        "vulnerabilities": vulns,
        "actors": ["Regulatory authority", "Cybercriminal group", "Nation-state actor", "Disgruntled insider"],
    }

    return {
        "pestel_items": pestel_items,
        "swot": swot,
        "tvra": tvra,
        "geopolitical": geopolitical[:8] or ["Unspecified_region"],
        "global_labels": global_labels[:8],
        "_source": "heuristic_fallback",
    }


def _guess_threat(fragment: Optional[str]) -> str:
    if not fragment:
        return "Unknown_threat"
    t = fragment.strip()[:80]
    return re.sub(r"[^\w\s\-]", "", t).replace(" ", "_")[:48] or "Context_threat"


def _unique_short(items: list[str]) -> list[str]:
    out: list[str] = []
    for x in items:
        if x and x not in out:
            out.append(x)
    return out[:12]


# --- Controls from raw text ---

_CONTROL_LINE = re.compile(
    r"^(?:\d+(?:\.\d+)*|[A-Z]\.|Annex\s+[A-Z]?\d*|[•\-–]\s*)\s*.+", re.I | re.M
)
_ACTION_WORDS = re.compile(
    r"\b(?:shall|must|should|required|ensure|ensuring|comply|compliance|"
    r"recommended|advised|recipients\s+are\s+advised|required\s+to|"
    r"not\s+later\s+than|no\s+later\s+than)\b",
    re.I,
)


def heuristic_controls_from_text(chunk: str, chunk_index: int) -> List[Dict[str, Any]]:
    """Cheap extraction: regulatory / IMO-style lines with obligation language or bullets."""
    out: List[Dict[str, Any]] = []
    if not chunk or not chunk.strip():
        return out
    for line in chunk.splitlines():
        line = line.strip()
        if len(line) < 18:
            continue
        low = line.lower()
        looks_numbered = bool(_CONTROL_LINE.match(line))
        looks_action = bool(_ACTION_WORDS.search(line))
        looks_obligation = (
            looks_action
            or " shall " in f" {low} "
            or " must " in f" {low} "
            or " should " in f" {low} "
        )
        if looks_numbered and (looks_obligation or len(line) > 45):
            out.append(
                {
                    "control_text": line[:2000],
                    "section_ref": f"heuristic chunk {chunk_index}",
                    "framework": None,
                    "source_page": None,
                }
            )
            continue
        if looks_obligation and len(line) >= 25:
            out.append(
                {
                    "control_text": line[:2000],
                    "section_ref": f"heuristic chunk {chunk_index}",
                    "framework": None,
                    "source_page": None,
                }
            )
    if not out and len(chunk) > 120:
        # Fallback: first substantial paragraph (IMO circulars often lack shall/must per line)
        para = chunk[:2000].strip()
        block = para.split("\n\n")[0].strip()
        if len(block) >= 40:
            out.append(
                {
                    "control_text": block[:2000],
                    "section_ref": f"summary chunk {chunk_index}",
                    "framework": None,
                    "source_page": None,
                }
            )
    return out[:40]


# --- Risk discovery ---

def heuristic_candidate_risks(bundle: List[dict[str, Any]]) -> List[dict[str, Any]]:
    """One candidate risk per issue when LLM fails; light merge by title prefix."""
    merged: dict[str, dict[str, Any]] = {}
    for item in bundle:
        iid = str(item.get("issue_id") or "")
        title = (item.get("title") or "Untitled issue").strip()
        if not iid:
            continue
        key = title[:48].lower()
        if key not in merged:
            merged[key] = {
                "title": title[:200],
                "description": (str(item.get("body") or ""))[:2000],
                "domain": "External_monitoring",
                "confidence": 0.45,
                "reasoning": "Fallback heuristic confidence because LLM candidate risk generation was unaviable",
                "issue_ids": [iid],
            }
        else:
            if iid not in merged[key]["issue_ids"]:
                merged[key]["issue_ids"].append(iid)
    return list(merged.values())


def heuristic_library_match(
    candidate: dict[str, Any],
    shortlist: List[Tuple[dict[str, Any], float]],
) -> dict[str, Any]:
    """BM25-only triage when OpenAI match fails."""
    if not shortlist:
        return {"match": "new", "library_id": None, "rationale": "No library shortlist (heuristic)."}
    top_row, score = shortlist[0]
    top_id = str(top_row["id"])
    if score >= 1.0:
        return {
            "match": "existing",
            "library_id": top_id,
            "rationale": f"Heuristic: strongest BM25 match (score={score:.3f}); Azure match skipped.",
        }
    if score > 0:
        return {
            "match": "ambiguous",
            "library_id": None,
            "rationale": f"Weak BM25 signal (score={score:.3f}); review suggested.",
        }
    # Scores often zero when query tokens missing from corpus; still surface top lexical row as ambiguous.
    return {
        "match": "ambiguous",
        "library_id": None,
        "rationale": "No strong BM25 score; review top library entries manually (heuristic).",
    }
