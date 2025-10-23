import pytest
from datetime import datetime
from datetime import timedelta
from qcc.domain.tag import Tag
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.enums import TagValue


def make_assignment(tagger_id, value):
    """Helper to quickly create TagAssignments for tests."""
    return TagAssignment(
        tagger_id=tagger_id,
        comment_id="c1",
        characteristic_id="ch1",
        value=value,
        timestamp=datetime.now(),
    )


def test_krippendorff_alpha_perfect_agreement():
    """
    Test perfect agreement scenario where all taggers assign the same value.
    
    This tests the ideal case where there is complete consensus among taggers.
    With 3 taggers all assigning YES, we expect:
    - Observed agreement: 100% (all pairs agree)
    - Expected agreement: High due to value homogeneity
    - Alpha should be 1.0 indicating perfect inter-rater reliability
    
    This validates that the method correctly identifies and reports perfect consensus.
    """
    tag = Tag(id="t1", comment_id="c1", characteristic_id="ch1")
    tag.assignments = [
        make_assignment("u1", TagValue.YES),
        make_assignment("u2", TagValue.YES),
        make_assignment("u3", TagValue.YES),
    ]
    assert tag.krippendorff_alpha() == 1.0


def test_krippendorff_alpha_partial_agreement():
    """
    Test partial agreement scenario with majority consensus but some disagreement.
    
    This tests a realistic case where most taggers agree but there's some dissent.
    With 2 YES and 1 NO assignments:
    - Observed agreement: Only 1 agreeing pair out of 3 total pairs (33%)
    - Expected agreement: Higher due to YES being the majority value
    - Mathematically, alpha would be negative due to small sample size
    - We clamp negative values to 0.0 for practical reporting
    
    This validates that the method handles mixed consensus correctly and applies
    the clamping logic for small sample edge cases.
    """
    tag = Tag(id="t2", comment_id="c1", characteristic_id="ch1")
    tag.assignments = [
        make_assignment("u1", TagValue.YES),
        make_assignment("u2", TagValue.YES),
        make_assignment("u3", TagValue.NO),
    ]
    alpha = tag.krippendorff_alpha()
    assert alpha is not None
    # For 2 YES, 1 NO with n=3, alpha is mathematically negative
    # but we clamp it to 0 for reporting
    assert alpha == 0.0


def test_krippendorff_alpha_no_agreement():
    """
    Test complete disagreement scenario with no consensus.
    
    This tests the case where taggers fundamentally disagree.
    With 1 YES and 1 NO assignment:
    - Observed agreement: 0% (no agreeing pairs)
    - Expected agreement: Moderate due to balanced value distribution
    - Alpha should be 0.0 indicating no agreement beyond chance
    
    This validates that the method correctly identifies complete disagreement
    and returns the minimum agreement value.
    """
    tag = Tag(id="t3", comment_id="c1", characteristic_id="ch1")
    tag.assignments = [
        make_assignment("u1", TagValue.YES),
        make_assignment("u2", TagValue.NO),
    ]
    alpha = tag.krippendorff_alpha()
    # For 1 YES, 1 NO, alpha should be 0 (no agreement beyond chance)
    assert alpha == 0.0


def test_krippendorff_alpha_not_enough_data():
    """
    Test insufficient data scenario where agreement cannot be calculated.
    
    This tests the edge case where there aren't enough assignments to measure
    inter-rater agreement. Krippendorff's alpha requires at least 2 assignments
    to form comparison pairs.
    
    With only 1 assignment:
    - No pairs can be formed for comparison
    - Method should return None to indicate calculation is not possible
    - This prevents misleading or undefined agreement metrics
    
    This validates proper handling of insufficient data and boundary conditions.
    """
    tag = Tag(id="t4", comment_id="c1", characteristic_id="ch1")
    tag.assignments = [make_assignment("u1", TagValue.YES)]
    assert tag.krippendorff_alpha() is None


@pytest.fixture
def sample_assignments():
    """Return a set of TagAssignments for one comment-characteristic pair."""
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    return [
        TagAssignment(tagger_id="T1", comment_id="C1", characteristic_id="CH1", value=TagValue.YES, timestamp=base_time),
        TagAssignment(tagger_id="T2", comment_id="C1", characteristic_id="CH1", value=TagValue.YES, timestamp=base_time + timedelta(seconds=10)),
        TagAssignment(tagger_id="T3", comment_id="C1", characteristic_id="CH1", value=TagValue.NO, timestamp=base_time + timedelta(seconds=20)),
    ]


@pytest.fixture
def tag(sample_assignments):
    """Return a Tag aggregate built from sample assignments."""
    return Tag.from_assignments(
        id="TAG1",
        comment_id="C1",
        characteristic_id="CH1",
        assignments=sample_assignments,
    )


# --- Basic collection management -------------------------------------------------

def test_num_assignments(tag):
    assert tag.num_assignments() == 3


def test_num_unique_taggers(tag):
    assert tag.num_unique_taggers() == 3


def test_add_assignment_valid(tag):
    new_assignment = TagAssignment(
        tagger_id="T4",
        comment_id="C1",
        characteristic_id="CH1",
        value=TagValue.YES,
        timestamp=datetime.now()
    )
    tag.add_assignment(new_assignment)
    assert tag.num_assignments() == 4


def test_add_assignment_invalid(tag):
    from qcc.domain.tagassignment import TagAssignment
    bad = TagAssignment(
        tagger_id="T5",
        comment_id="DIFFERENT",
        characteristic_id="CH1",
        value=TagValue.YES,
        timestamp=datetime.now()
    )
    with pytest.raises(ValueError):
        tag.add_assignment(bad)


# --- Value counts and proportions -------------------------------------------------

def test_yesno_counts(tag):
    counts = tag.yesno_counts()
    assert counts[TagValue.YES] == 2
    assert counts[TagValue.NO] == 1


def test_yesno_proportions(tag):
    proportions = tag.yesno_proportions()
    total = sum(proportions.values())
    assert pytest.approx(total, 0.01) == 1.0
    assert proportions[TagValue.YES] > proportions[TagValue.NO]


# --- Consensus metrics ------------------------------------------------------------

def test_consensus_value(tag):
    assert tag.consensus_value() == TagValue.YES


def test_consensus_ratio(tag):
    assert tag.consensus_ratio() == 2 / 3


def test_agreement_percent(tag):
    # There are 3 assignments: two YES, one NO.
    # Pairs: (YES,YES)=agree, (YES,NO)=disagree, (YES,NO)=disagree → 1/3 agreement
    result = tag.agreement_percent()
    assert pytest.approx(result, 0.01) == 1 / 3


# --- Prevalence -------------------------------------------------------------------

def test_prevalence_uses_consensus(tag):
    """When no explicit focal value, it falls back to consensus_value (YES)."""
    assert tag.prevalence() == 2 / 3


def test_prevalence_with_meta(tag):
    """If meta has focal_value, it uses that instead of consensus."""
    tag.meta["focal_value"] = TagValue.NO
    assert tag.prevalence() == 1 / 3


# --- Dict and serialization -------------------------------------------------------

def test_to_dict_has_expected_keys(tag):
    d = tag.to_dict()
    expected_keys = {
        "id",
        "comment_id",
        "characteristic_id",
        "num_assignments",
        "num_unique_taggers",
        "value_distribution",
        "consensus_value",
        "consensus_ratio",
        "agreement_percent",
        "meta",
    }
    assert expected_keys.issubset(d.keys())


def test_to_dict_values_are_serializable(tag):
    """Ensure TagValue keys converted to string in JSON output."""
    d = tag.to_dict()
    for k in d["value_distribution"].keys():
        assert isinstance(k, str)