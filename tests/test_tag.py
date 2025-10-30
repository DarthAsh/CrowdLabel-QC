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