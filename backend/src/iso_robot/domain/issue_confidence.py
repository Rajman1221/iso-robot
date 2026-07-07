from __future__ import annotations

from typing import Any, Optional

CONFIDENCE_SOURCE_LLM = "llm"
CONFIDENCE_SOURCE_HEURISTIC = "heuristic"


def normalize_llm_confidence(value: Any) -> Optional[float]:
    """Parse and clamp an LLM-reported confidence to [0.0, 1.0]."""
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return round(max(0.0, min(1.0, num)), 2)


__all__ = [
    "CONFIDENCE_SOURCE_HEURISTIC",
    "CONFIDENCE_SOURCE_LLM",
    "normalize_llm_confidence",
]
