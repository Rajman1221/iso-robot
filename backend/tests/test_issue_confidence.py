from __future__ import annotations

import pytest

from iso_robot.domain.issue_confidence import normalize_llm_confidence


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.78, 0.78),
        (1.2, 1.0),
        (-0.1, 0.0),
        ("0.65", 0.65),
        ("bad", None),
        (None, None),
    ],
)
def test_normalize_llm_confidence(value, expected) -> None:
    assert normalize_llm_confidence(value) == expected
