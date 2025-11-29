from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime

from qcc.reports.tag_report import (
    group_by_comment,
    group_by_comment_and_characteristic,
    taggers_who_touched_comment,
    count_yes_no,
    alpha_for_item,
    TagReportRow,
)
from qcc.domain.comment import Comment
from qcc.domain.characteristic import Characteristic
from qcc.domain.tagger import Tagger
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.enums import TagValue


def make_assignment(**kwargs):
    # Simple helper to create a lightweight assignment-like object.
    return SimpleNamespace(**kwargs)


def test_group_by_comment_with_comment_objects():
    c1 = Comment(id="c1", text="hello", prompt_id="p1", tagassignments=[])
    c2 = Comment(id="c2", text="bye", prompt_id="p1", tagassignments=[])

    a1 = make_assignment(comment=c1, comment_id="c1")
    a2 = make_assignment(comment=c1, comment_id="c1")
    a3 = make_assignment(comment=c2, comment_id="c2")

    grouped = group_by_comment([a1, a2, a3])
    assert set(grouped.keys()) == {"c1", "c2"}
    assert len(grouped["c1"]) == 2
    assert len(grouped["c2"]) == 1


def test_group_by_comment_creates_placeholders_for_ids():
    now = datetime.utcnow()
    t1 = TagAssignment(tagger_id="u1", comment_id="c1", characteristic_id="ch", value=TagValue.YES, timestamp=now)
    t2 = TagAssignment(tagger_id="u2", comment_id="c1", characteristic_id="ch", value=TagValue.NO, timestamp=now)
    t3 = TagAssignment(tagger_id="u3", comment_id="c2", characteristic_id="ch", value=TagValue.YES, timestamp=now)

    grouped = group_by_comment([t1, t2, t3])
    keys = set(grouped.keys())
    assert keys == {"c1", "c2"}
    # placeholders should contain corresponding assignments
    for k, v in grouped.items():
        assert all(getattr(a, "comment_id", None) == k for a in v)


def test_group_by_comment_and_characteristic_with_ids_and_objects():
    now = datetime.utcnow()
    t1 = TagAssignment(tagger_id="u1", comment_id="c1", characteristic_id="ch1", value=TagValue.YES, timestamp=now)
    t2 = TagAssignment(tagger_id="u2", comment_id="c1", characteristic_id="ch2", value=TagValue.NO, timestamp=now)
    # also a simple enriched object
    c1 = Comment(id="c1", text="foo", prompt_id="p", tagassignments=[])
    ch = Characteristic(id="ch1", name="ch1")
    a3 = make_assignment(comment=c1, characteristic=ch, comment_id="c1", characteristic_id="ch1")

    grouped = group_by_comment_and_characteristic([t1, t2, a3])
    assert len(grouped) >= 2
    # keys should be tuples of (comment_id, characteristic_id) strings
    for (comment_id, characteristic_id), items in grouped.items():
        assert isinstance(comment_id, str)
        assert isinstance(characteristic_id, str)


def test_taggers_who_touched_comment_supports_ids_and_objects():
    now = datetime.utcnow()
    ta1 = TagAssignment(tagger_id="u1", comment_id="c1", characteristic_id="ch", value=TagValue.YES, timestamp=now)
    ta2 = TagAssignment(tagger_id="u2", comment_id="c1", characteristic_id="ch", value=TagValue.NO, timestamp=now)
    # create an assignment with a Tagger object directly
    t = Tagger(id="u3", tagassignments=[])
    enriched = make_assignment(tagger=t, tagger_id="u3")

    seen = taggers_who_touched_comment([ta1, ta2, enriched])
    assert isinstance(seen, set)
    assert seen == {"u1", "u2", "u3"}


def test_count_yes_no_counts_only_yes_no():
    now = datetime.utcnow()
    a_yes = TagAssignment(tagger_id="u1", comment_id="c", characteristic_id="ch", value=TagValue.YES, timestamp=now)
    a_no = TagAssignment(tagger_id="u2", comment_id="c", characteristic_id="ch", value=TagValue.NO, timestamp=now)
    a_na = TagAssignment(tagger_id="u3", comment_id="c", characteristic_id="ch", value=TagValue.NA, timestamp=now)

    yes, no = count_yes_no([a_yes, a_no, a_na])
    assert yes == 1 and no == 1


def test_alpha_for_item_returns_none_for_one_tagger_and_value_for_two():
    now = datetime.utcnow()
    # Single tagger => None
    a1 = TagAssignment(tagger_id="u1", comment_id="c1", characteristic_id="ch", value=TagValue.YES, timestamp=now)
    c = Characteristic(id="ch", name="ch")
    assert alpha_for_item([a1], c) is None

    # Two taggers agreeing => alpha should be defined (non-None, expected 1.0)
    a2 = TagAssignment(tagger_id="u2", comment_id="c1", characteristic_id="ch", value=TagValue.YES, timestamp=now)
    val = alpha_for_item([a1, a2], c)
    assert isinstance(val, float)
    # In the simple same-value case alpha is 1.0 per the algorithm
    assert val == 1.0
