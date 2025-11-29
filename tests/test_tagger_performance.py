import math
from datetime import datetime
from typing import Dict, List, Optional, Mapping
import pytest # Using pytest as the framework

# --- ASSUMED IMPORTS ---
# You must adjust these imports to match your actual file structure
from qcc.domain.tagger import Tagger
from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.enums import TagValue # Assuming TagValue is an Enum with YES, NO, NA

# Import the class containing the new method
# from qcc.reports.tagger_performance import TaggerPerformanceReport

# Import the standalone function
# from qcc.reports.tag_report import weighted_agreement 

# Re-defining TaggerPerformanceReport with the method for context
class TaggerPerformanceReport:
    # ... (rest of the class methods) ...
    def build_reliability_index(self, summary: Mapping[str, object]) -> Dict[str, float]:
        # Implementation is the same as provided earlier
        reliability: Dict[str, float] = {}
        agreement_summary = summary.get("agreement", {})
        if not isinstance(agreement_summary, Mapping):
            return reliability
        per_characteristic = agreement_summary.get("per_characteristic", []) or []
        per_tagger_values: Dict[str, List[float]] = {}
        for char_entry in per_characteristic:
            if not isinstance(char_entry, Mapping): continue
            per_tagger = char_entry.get("per_tagger", []) or []
            for tagger_entry in per_tagger:
                if not isinstance(tagger_entry, Mapping): continue
                tagger_id = str(tagger_entry.get("tagger_id", "")).strip()
                if not tagger_id: continue
                # We target 'cohens_kappa' as the per-tagger agreement metric
                score = tagger_entry.get("cohens_kappa")
                if isinstance(score, (int, float)) and math.isfinite(float(score)):
                    per_tagger_values.setdefault(tagger_id, []).append(float(score))
        for tagger_id, values in per_tagger_values.items():
            if not values: continue
            avg_score = sum(values) / len(values)
            reliability[tagger_id] = max(0.0, min(1.0, avg_score))
        return reliability

# Re-defining weighted_agreement function for context
def weighted_agreement(
    assignments: List[TagAssignment],
    target_value: TagValue,
    reliability_lookup: Dict[str, float],
) -> Optional[float]:
    # Implementation is the same as provided earlier
    numer = 0.0
    denom = 0.0
    for a in assignments:
        tagger_id = str(a.tagger_id) # Assumes TagAssignment has a 'tagger' attribute with an 'id'
        r = reliability_lookup.get(tagger_id, 0.0)
        if r > 0.0:
            denom += r
            if a.value == target_value:
                numer += r
    if denom == 0.0:
        return None
    return numer / denom

# -------------------------------------------------------------
## 🧪 Tests for build_reliability_index
# -------------------------------------------------------------

def test_build_reliability_index_average_and_clamping():
    """Tests averaging over multiple characteristics and clamping values to [0, 1]."""
    mock_summary = {
        "agreement": {
            "per_characteristic": [
                {
                    "characteristic_id": "c1",
                    "per_tagger": [
                        {"tagger_id": "A", "cohens_kappa": 1.2},   # Will be clamped to 1.0
                        {"tagger_id": "B", "cohens_kappa": 0.5},
                    ]
                },
                {
                    "characteristic_id": "c2",
                    "per_tagger": [
                        {"tagger_id": "A", "cohens_kappa": 0.8},
                        {"tagger_id": "B", "cohens_kappa": -0.2}, # Will be clamped to 0.0
                    ]
                }
            ]
        }
    }

    report = TaggerPerformanceReport()
    result = report.build_reliability_index(mock_summary)

    # Expected A: Avg((1.2 + 0.8) / 2) = 1.0
    assert result["A"] == 1.0
    # Expected B: Avg((0.5 + -0.2) / 2) = 0.15
    assert result["B"] == 0.15
    assert len(result) == 2

def test_build_reliability_index_missing_metric():
    """Tests that taggers without the target metric are ignored."""
    mock_summary = {
        "agreement": {
            "per_characteristic": [
                {
                    "characteristic_id": "c1",
                    "per_tagger": [
                        {"tagger_id": "TaggerE", "cohens_kappa": 0.6},
                        {"tagger_id": "TaggerF", "other_metric": 0.9},
                    ]
                }
            ]
        }
    }

    report = TaggerPerformanceReport()
    result = report.build_reliability_index(mock_summary)

    assert result["TaggerE"] == 0.6
    assert "TaggerF" not in result
    assert len(result) == 1

def test_build_reliability_index_no_agreement_key():
    """Tests resilience to missing the top-level 'agreement' key."""
    mock_summary = {"speed": {}}

    report = TaggerPerformanceReport()
    result = report.build_reliability_index(mock_summary)

    assert result == {}

# -------------------------------------------------------------
## 🧪 Tests for weighted_agreement
# -------------------------------------------------------------

# Helper function to create assignments using the actual domain classes
# Note: This requires the actual TagAssignment and Tagger classes to be defined/imported.
def create_assignment(tagger_id: str, value: TagValue) -> TagAssignment:
    """Creates a TagAssignment object with all required fields for testing."""
    return TagAssignment(
        # Essential fields for tracking and lookup:
        tagger_id=tagger_id,
        comment_id="C1",
        characteristic_id="ChA",
        value=value,
        timestamp=datetime.now(),
    )

# Example usage in test:
def test_weighted_agreement_perfect_agreement_actual_classes():
    reliability = {"A": 1.0, "B": 0.5}
    
    assignments = [
        create_assignment("A", TagValue.YES),
        create_assignment("B", TagValue.YES),
    ]
    
    result = weighted_agreement(assignments, TagValue.YES, reliability)
    assert result == 1.0

def test_weighted_agreement_split_agreement_actual_classes():
    """Weights assignments where raters disagree."""
    reliability = {"A": 1.0, "B": 0.5}
    
    # Tagger A tags YES (high reliability), Tagger B tags NO (low reliability)
    assignments = [
        create_assignment("A", TagValue.YES),
        create_assignment("B", TagValue.NO),
    ]
    
    # Check for YES (Target Value)
    # Numerator (YES): 1.0 (from A)
    # Denominator (Total Reliability): 1.5
    result_yes = weighted_agreement(assignments, TagValue.YES, reliability)
    assert round(result_yes, 3) == 0.667

    # Check for NO (Target Value)
    # Numerator (NO): 0.5 (from B)
    # Denominator (Total Reliability): 1.5
    result_no = weighted_agreement(assignments, TagValue.NO, reliability)
    assert round(result_no, 3) == 0.333

def test_weighted_agreement_zero_reliability_actual_classes():
    """Tests that a tagger with zero reliability is ignored in weighting."""
    reliability = {"A": 0.0, "B": 0.5}
    
    # Tagger A tags NO (r=0.0), Tagger B tags YES (r=0.5)
    assignments = [
        create_assignment("A", TagValue.NO),
        create_assignment("B", TagValue.YES),
    ]
    
    # Check for YES (Target Value)
    # Numerator: 0.5. Denominator: 0.5.
    result_yes = weighted_agreement(assignments, TagValue.YES, reliability)
    assert result_yes == 1.0
    
    # Check for NO (Target Value)
    # Numerator: 0.0. Denominator: 0.5.
    result_no = weighted_agreement(assignments, TagValue.NO, reliability)
    assert result_no == 0.0

def test_weighted_agreement_empty_or_zero_denominator_actual_classes():
    """Tests the edge case where the denominator is zero (returns None)."""
    # 1. Empty Assignments
    result_empty = weighted_agreement([], TagValue.YES, {"A": 0.8})
    assert result_empty is None
    
    # 2. All participating taggers have zero reliability
    reliability = {"A": 0.0, "B": 0.0}
    assignments = [
        create_assignment("A", TagValue.YES),
        create_assignment("B", TagValue.YES),
    ]
    result_zero_denom = weighted_agreement(assignments, TagValue.YES, reliability)
    assert result_zero_denom is None