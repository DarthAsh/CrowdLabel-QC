from __future__ import annotations

import pytest
from datetime import datetime, timedelta

from src.qcc.domain.characteristic import Characteristic
from src.qcc.metrics.pattern_strategy import VerticalPatternDetection, HorizontalPatternDetection
from src.qcc.domain.tagger import Tagger
from src.qcc.domain.comment import Comment
from src.qcc.domain.enums import TagValue
from src.qcc.domain.tagassignment import TagAssignment
# from src.qcc.domain.

def make_empty_tagger() -> Tagger:
    return Tagger(id="t0", meta=None, tagassignments=[])

def generate_mock_data():
    # create comments
    comment1 =  Comment("1", "Comment 1", "1", [])
    comment2 =  Comment("2", "Comment 2", "1", [])
    # create characteristics
    char1 = Characteristic("1", "Char 1")
    char2 = Characteristic("2", "Char 2")
    char3 = Characteristic("3", "Char 3")
    # char1 = Characteristic("1", "Char 1")

    # create taggers
    tagger1 = Tagger("1", [])
    # tagger2 = Tagger("2", [])

    time1 = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    time2 = datetime(year=2025, month=10, day=28, hour=0, minute=31)
    time3 = datetime(year=2025, month=10, day=28, hour=0, minute=32)
    time4 = datetime(year=2025, month=10, day=28, hour=0, minute=33)
    time5 = datetime(year=2025, month=10, day=28, hour=0, minute=34)
    time6 = datetime(year=2025, month=10, day=28, hour=0, minute=35)
    # time7 = datetime(year=2025, month=10, day=28, hour=0, minute=36)
    # time8 = datetime(year=2025, month=10, day=28, hour=0, minute=37)

def create_YES_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)

    assignments = []

    for i in range(n_assignments):
        time_param = start_time + i * interval
        cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
        assignments.append(cur_assignment)

    # link tagassignments with taggers
    for assignment in assignments:
        tagger1.tagassignments.append(assignment)

def create_NO_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)

    assignments = []

    for i in range(n_assignments):
        time_param = start_time + i * interval
        cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
        assignments.append(cur_assignment)

    # link tagassignments with taggers
    for assignment in assignments:
        tagger1.tagassignments.append(assignment)

def create_YN_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)

    assignments = []

    for i in range(n_assignments):
        time_param = start_time + i * interval
        if i % 2 == 0:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
        else:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
        assignments.append(cur_assignment)

    # link tagassignments with taggers
    for assignment in assignments:
        tagger1.tagassignments.append(assignment)

def create_YNNY_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)

    assignments = []

    for i in range(n_assignments):
        time_param = start_time + i * interval
        if i % 4 == 0:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
        elif i % 4 == 1:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
        elif i % 4 == 2:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
        else:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)

        assignments.append(cur_assignment)

    # link tagassignments with taggers
    for assignment in assignments:
        tagger1.tagassignments.append(assignment)

def create_NYYN_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)

    assignments = []

    for i in range(n_assignments):
        time_param = start_time + i * interval
        if i % 4 == 0:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
        elif i % 4 == 1:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
        elif i % 4 == 2:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
        else:
            cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)

        assignments.append(cur_assignment)

    # link tagassignments with taggers
    for assignment in assignments:
        tagger1.tagassignments.append(assignment)

# def create_YNYN_assignme

def test_vertical_pattern_detection():
    strategy = VerticalPatternDetection()
    
def test_simple_sequence_patterns_signature():
    """
    Smoke test: Ensures the SimpleSequencePatterns class and its
    analyze method exist and correctly raise NotImplementedError.
    """
    # Scenarios

    # no patterns of any kind

    # 1 of each pattern

    # 1 of specific patterns, and not others

    # > 1 for each pattern (but not same values)

    # >1 for specific patterns, and 0 for others
        
@pytest.mark.xfail(reason="Logic for SimpleSequencePatterns not yet implemented")
def test_simple_sequence_patterns_logic_placeholder():
    """Placeholder for future implementation tests when the logic is added."""
    # This test will be skipped (expected to fail) until the logic is written.
    assert False

# def test_speed_strategy_is_pure_by_contract():
#     strategy = SimpleSequencePatterns()
#     t = make_empty_tagger()
#     with pytest.raises(NotImplementedError):
#         strategy.analyze(t)
#     with pytest.raises(NotImplementedError):
#         strategy.analyze(t)