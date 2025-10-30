from __future__ import annotations
import datetime
from typing import NamedTuple, Any
import pytest
from enum import Enum # Needed for TagValue definition

# Import the class under test (the agreement metric implementation)
from src.qcc.metrics.agreement_strategy import LatestLabelPercentAgreement
# Import required domain model components
from src.qcc.domain.tagger import Tagger
from src.qcc.domain.enums import TagValue 

# --- Mocking Dependencies ---

class MockAssignment(NamedTuple):
    """
    Mock class simulating the TagAssignment object required by the metric.
    It includes necessary fields for characteristic filtering and time-based selection.
    """
    tagger_id: str
    comment_id: str      # The Unit ID (item being rated)
    value: Any           # The assigned category value (using TagValue Enum members)
    timestamp: datetime.datetime
    characteristic_id: str = "CHAR_1" # Default ID for testing
    is_na: bool = False # Legacy/unused flag, primary NA handling is via 'value' check

class MockCharacteristic:
    """
    Mock class simulating the Characteristic object, primarily used to pass the ID
    to the metric's methods for filtering.
    """
    def __init__(self, id: str):
        self.id = id
        
def ts(minutes: int):
    """Helper function to create deterministic, comparable timestamps."""
    return datetime.datetime(2025, 1, 1, 10, minutes)

# Create a constant characteristic object used across most tests
TEST_CHAR = MockCharacteristic(id="CHAR_1")

def make_empty_tagger() -> Tagger:
    """Helper to create a mock Tagger instance."""
    return Tagger(id="t0", meta=None, tagassignments=[])

# --- Test Cases ---

def test_perfect_agreement():
    """
    Tests Krippendorff's Alpha when all raters agree perfectly on all units.
    The expected result is 1.0 (perfect reliability).
    """
    assignments = [
        # Unit 1: Three raters agree on YES
        MockAssignment('A', 'U1', TagValue.YES, ts(1)),
        MockAssignment('B', 'U1', TagValue.YES, ts(1)),
        MockAssignment('C', 'U1', TagValue.YES, ts(1)),
        # Unit 2: Three raters agree on NO
        MockAssignment('A', 'U2', TagValue.NO, ts(1)),
        MockAssignment('B', 'U2', TagValue.NO, ts(1)),
        MockAssignment('C', 'U2', TagValue.NO, ts(1)),
    ]
    result = LatestLabelPercentAgreement.krippendorff_alpha(assignments, TEST_CHAR)
    assert result == 1.0

def test_maximum_disagreement_or_chance():
    """
    Tests Krippendorff's Alpha when raters disagree as much as possible, 
    matching the expected level of chance agreement.
    The expected result should be <= 0.0 (clamped to 0.0).
    """
    assignments = [
        # Unit 1: Raters use three different categories (YES, NO, UNCERTAIN)
        MockAssignment('A', 'U1', TagValue.YES, ts(1)),
        MockAssignment('B', 'U1', TagValue.NO, ts(1)),
        MockAssignment('C', 'U1', TagValue.UNCERTAIN, ts(1)),
        # Unit 2: Raters use the same three categories, shuffled
        MockAssignment('A', 'U2', TagValue.NO, ts(1)),
        MockAssignment('B', 'U2', TagValue.UNCERTAIN, ts(1)),
        MockAssignment('C', 'U2', TagValue.YES, ts(1)),
    ]
    result = LatestLabelPercentAgreement.krippendorff_alpha(assignments, TEST_CHAR)
    assert result == 0.0

def test_mixed_agreement_verification():
    """
    Tests a specific mixed agreement scenario calculated manually to verify
    the implementation's calculation of Observed (D_o) and Expected (D_e) Disagreement.
    
    Expected Alpha for this specific 2-category data is ~0.645.
    """
    # Calculation notes from the implementation verification:
    # N=12 pairs. D_o_raw = 4. D_o ≈ 0.333.
    # Marginals (n_c): YES=2, NO=4. Sum(n_c^2) = 20. D_e ≈ 0.939.
    # Alpha = (0.939 - 0.333) / 0.939 ≈ 0.645
    
    assignments = [
        # Unit 1: Two raters agree (YES), one disagrees (NO)
        MockAssignment('A', 'U1', TagValue.YES, ts(1)),
        MockAssignment('B', 'U1', TagValue.YES, ts(1)),
        MockAssignment('C', 'U1', TagValue.NO, ts(1)),
        # Unit 2: Three raters agree (NO)
        MockAssignment('A', 'U2', TagValue.NO, ts(1)),
        MockAssignment('B', 'U2', TagValue.NO, ts(1)),
        MockAssignment('C', 'U2', TagValue.NO, ts(1)),
    ]
    result = LatestLabelPercentAgreement.krippendorff_alpha(assignments, TEST_CHAR)
    # Use pytest.approx for floating point comparison with tolerance
    assert result == pytest.approx(0.645, abs=0.001)

def test_with_missing_data():
    """
    Tests that the metric correctly handles missing data (TagValue.NA) by:
    1. Excluding the NA assignment from the Unit x Rater matrix.
    2. Adjusting the total number of pairs (N) accordingly.
    This scenario should result in perfect agreement among the remaining raters (Alpha = 1.0).
    """
    assignments = [
        # Unit 1: Rater C's NA assignment is ignored, leaving only 2 raters (A, B) who agree
        MockAssignment('A', 'U1', TagValue.YES, ts(1)),
        MockAssignment('B', 'U1', TagValue.YES, ts(1)),
        MockAssignment('C', 'U1', TagValue.NA, ts(1), is_na=True),
        # Unit 2: All 3 raters agree on NO
        MockAssignment('A', 'U2', TagValue.NO, ts(1)),
        MockAssignment('B', 'U2', TagValue.NO, ts(1)),
        MockAssignment('C', 'U2', TagValue.NO, ts(1)),
    ]
    # Despite mixed categories across units, all *available* ratings have perfect internal agreement.
    result = LatestLabelPercentAgreement.krippendorff_alpha(assignments, TEST_CHAR)
    assert result == 1.0

def test_latest_label_selection():
    """
    Tests the core data preparation logic: ensuring only the assignment with the latest timestamp
    for a given (Unit, Rater) pair is selected for the matrix.
    The final matrix should show disagreement, leading to Alpha = 0.0.
    """
    assignments = [
        # Rater A submits NO, then YES (Latest is YES @ ts 5)
        MockAssignment('A', 'U1', TagValue.NO, ts(1)),
        MockAssignment('A', 'U1', TagValue.YES, ts(5)), 
        # Rater B submits YES, then NO (Latest is NO @ ts 4)
        MockAssignment('B', 'U1', TagValue.YES, ts(2)),
        MockAssignment('B', 'U1', TagValue.NO, ts(4)), 
    ]
    # Final Result: Rater A = YES, Rater B = NO. (Total disagreement for N=2 pairs).
    result = LatestLabelPercentAgreement.krippendorff_alpha(assignments, TEST_CHAR)
    assert result == 0.0

def test_missing_characteristic_id():
    """
    Tests boundary case where assignments do not match the requested characteristic ID.
    The result should be None, as no valid ratings were collected (N=0).
    """
    assignments = [
        # This assignment has an ID of "CHAR_2", but the test requests "CHAR_1"
        MockAssignment('A', 'U1', TagValue.YES, ts(1), characteristic_id="CHAR_2"),
    ]
    result = LatestLabelPercentAgreement.krippendorff_alpha(assignments, TEST_CHAR)
    assert result is None