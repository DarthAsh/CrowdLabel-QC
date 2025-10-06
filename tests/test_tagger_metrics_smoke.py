"""Minimal smoke tests for Tagger metrics: speed, agreement, patterns."""
from datetime import datetime, timedelta

from qcc.domain.enums import TagValue
from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger


def make_assign(tagger_id: str, comment_id: str, char_id: str, value: TagValue, ts: datetime):
    return TagAssignment(tagger_id, comment_id, char_id, value, ts)


def test_tagging_speed_nonzero():
    now = datetime.utcnow()
    t = Tagger(id="t1", tagassignments=[
        make_assign("t1", "c1", "char1", TagValue.YES, now),
        make_assign("t1", "c2", "char1", TagValue.NO, now + timedelta(seconds=2)),
        make_assign("t1", "c3", "char1", TagValue.YES, now + timedelta(seconds=5)),
    ])
    assert t.tagging_speed() > 0.0
    assert t.seconds_per_tag() > 0.0


def test_seconds_per_tag_one_second_interval():
    now = datetime.utcnow()
    # Two assignments exactly 1 second apart -> log2(1) == 0 -> seconds_per_tag == 1
    t = Tagger(id="t1", tagassignments=[
        make_assign("t1", "c1", "char1", TagValue.YES, now),
        make_assign("t1", "c2", "char1", TagValue.NO, now + timedelta(seconds=1)),
    ])
    assert t._compute_log_intervals() != []
    assert abs(t.seconds_per_tag() - 1.0) < 1e-6


def test_tagging_speed_insufficient_intervals():
    now = datetime.utcnow()
    # Only one timestamp -> insufficient data
    t = Tagger(id="t1", tagassignments=[
        make_assign("t1", "c1", "char1", TagValue.YES, now),
    ])
    assert t._compute_log_intervals() == []
    assert t.tagging_speed() == 0.0


def test_agreement_simple():
    char = Characteristic("char1", "Test")
    now = datetime.utcnow()
    # Both tag c1 and c2; disagree on c2
    t1 = Tagger(id="t1", tagassignments=[
        make_assign("t1", "c1", "char1", TagValue.YES, now),
        make_assign("t1", "c2", "char1", TagValue.YES, now + timedelta(seconds=1)),
    ])
    t2 = Tagger(id="t2", tagassignments=[
        make_assign("t2", "c1", "char1", TagValue.YES, now + timedelta(seconds=2)),
        make_assign("t2", "c2", "char1", TagValue.NO, now + timedelta(seconds=3)),
    ])
    # Overlap on c1 and c2 -> 1 match of 2 -> 0.5
    assert t1.agreement_with(t2, char) == 0.5


def test_pattern_long_run_and_alternation():
    char = Characteristic("char1", "Test")
    now = datetime.utcnow()
    # Create a long run of YES
    assigns = [make_assign("t1", f"c{i}", "char1", TagValue.YES, now + timedelta(seconds=i)) for i in range(12)]
    t = Tagger(id="t1", tagassignments=assigns)
    p = t.pattern_signals(char)
    assert p["patterns_found"] is True
    assert p["longest_run"]["length"] >= 10

    # Alternation case
    assigns2 = [make_assign("t2", f"a{i}", "char1", (TagValue.YES if i % 2 == 0 else TagValue.NO), now + timedelta(seconds=i)) for i in range(12)]
    t2 = Tagger(id="t2", tagassignments=assigns2)
    p2 = t2.pattern_signals(char)
    assert p2["patterns_found"] is True
    assert p2["alternations"]["ratio"] >= 0.9


def test_tie_timestamp_deterministic():
    char = Characteristic("char1", "Test")
    now = datetime.utcnow()
    # Single tagger has two assignments for the same comment at identical timestamps.
    # Deterministic tie-break within a tagger should pick the assignment with the
    # lexicographically greater value name. Create t1 with two assignments (NO, then YES)
    # at the same timestamp; t2 has a single YES; agreement should be 1.0 because
    # t1's latest chosen value will be 'YES'.
    t1 = Tagger(id="t1", tagassignments=[
        make_assign("t1", "c1", "char1", TagValue.NO, now),
        make_assign("t1", "c1", "char1", TagValue.YES, now),
    ])
    t2 = Tagger(id="t2", tagassignments=[make_assign("t2", "c1", "char1", TagValue.YES, now)])
    assert t1.agreement_with(t2, char) == 1.0


def test_ngram_repeats_trip_threshold():
    char = Characteristic("char1", "Test")
    now = datetime.utcnow()
    # Create sequence where 'Y','N','Y' repeats 6 times -> 3-gram repeated 6
    assigns = []
    for r in range(6):
        assigns.append(make_assign("t1", f"c_{r}_0", "char1", TagValue.YES, now + timedelta(seconds=3 * r)))
        assigns.append(make_assign("t1", f"c_{r}_1", "char1", TagValue.NO, now + timedelta(seconds=3 * r + 1)))
        assigns.append(make_assign("t1", f"c_{r}_2", "char1", TagValue.YES, now + timedelta(seconds=3 * r + 2)))
    t = Tagger(id="t1", tagassignments=assigns)
    p = t.pattern_signals(char)
    assert p["patterns_found"] is True
    assert any(entry["type"] == "repeated_ngrams" for entry in p["runs_summary"]) or p["top_repeats"]


def test_short_sequence():
    char = Characteristic("char1", "Test")
    now = datetime.utcnow()
    # Only two tags -> n < 3 -> no patterns
    t = Tagger(id="t1", tagassignments=[
        make_assign("t1", "c1", "char1", TagValue.YES, now),
        make_assign("t1", "c2", "char1", TagValue.NO, now + timedelta(seconds=1)),
    ])
    p = t.pattern_signals(char)
    assert p["patterns_found"] is False


def test_agreement_na_handling():
    char = Characteristic("char1", "Test")
    now = datetime.utcnow()
    t1 = Tagger(id="t1", tagassignments=[
        make_assign("t1", "c1", "char1", TagValue.NA, now),
    ])
    t2 = Tagger(id="t2", tagassignments=[
        make_assign("t2", "c1", "char1", TagValue.YES, now + timedelta(seconds=1)),
    ])
    # NA should be excluded from consideration -> no considered pairs -> agreement 0.0
    assert t1.agreement_with(t2, char) == 0.0


def test_no_overlap_agreement():
    char = Characteristic("char1", "Test")
    t1 = Tagger(id="t1", tagassignments=[make_assign("t1", "c1", "char1", TagValue.YES, datetime.utcnow())])
    t2 = Tagger(id="t2", tagassignments=[make_assign("t2", "c2", "char1", TagValue.YES, datetime.utcnow())])
    assert t1.agreement_with(t2, char) == 0.0
