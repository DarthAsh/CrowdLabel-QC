from __future__ import annotations

import pytest
from datetime import datetime, timedelta

from src.qcc.domain.characteristic import Characteristic
from src.qcc.metrics.utils.pattern import PatternCollection
from src.qcc.metrics.pattern_strategy import VerticalPatternDetection, HorizontalPatternDetection
from src.qcc.domain.tagger import Tagger
from src.qcc.domain.comment import Comment
from src.qcc.domain.enums import TagValue
from src.qcc.domain.tagassignment import TagAssignment
# from src.qcc.domain.

def make_empty_tagger() -> Tagger:
    return Tagger(id="t0", meta=None, tagassignments=[])

# wondering - how to test?
# scenarios - scenario 1 (sanity check) - are patterns getting detected? - 1 pattern detection
# scenario 2 - counting repeats of singular pattern
# scenario 3 - counting repeats of multiple patterns
# scenario 4 - counting repeats of some patterns, and 0 for other patterns

# issues -> pattern overlap making it hard to automate without manually creating patterns, i.e for example 
# if we intend to test for 2 repeats of YN, and our string built is YNYN, then a pattern for NYN will also be detected

# should I account for runtime measurement as well? if I'm to implement counting subsets algorithm I had in mind, then we may want to know
# how much slower that is
@pytest.fixture(scope="module")
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

    return {"tagger" : tagger1, "comment1": comment1, "comment2": comment2, "char1": char1, "char2": char2, "char3": char3}
    # time1 = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    # time2 = datetime(year=2025, month=10, day=28, hour=0, minute=31)
    # time3 = datetime(year=2025, month=10, day=28, hour=0, minute=32)
    # time4 = datetime(year=2025, month=10, day=28, hour=0, minute=33)
    # time5 = datetime(year=2025, month=10, day=28, hour=0, minute=34)
    # time6 = datetime(year=2025, month=10, day=28, hour=0, minute=35)
    # time7 = datetime(year=2025, month=10, day=28, hour=0, minute=36)
    # time8 = datetime(year=2025, month=10, day=28, hour=0, minute=37)

def create_custom_assignment_pattern(pattern: str, pattern_occurrences: int, tagger: Tagger, comment: Comment, charac: Characteristic):
    if pattern_occurrences == 0:
        raise ValueError("Pattern needs to occur at least once!")
    
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)
    assignments = []
    counter = 0

    # TODO logic flaw - allowing more than one tag assignment per person for one comment, and one characteristic

    # function creates tag assignment in chronological order as per given pattern for pattern_occurrences number of times
    for _ in range(pattern_occurrences):
        for tag_val_char in pattern:
            time_param = start_time + counter * interval
            counter += 1

            if tag_val_char == "Y":
                cur_assignment = TagAssignment(tagger, comment, charac, TagValue.YES, time_param)
            elif tag_val_char == "N":
                cur_assignment = TagAssignment(tagger, comment, charac, TagValue.NO, time_param)
            else:
                raise ValueError("Invalid pattern character! Can only be Y or N.")
            
            assignments.append(cur_assignment)

    tagger.tagassignments = []
    for assignment in assignments:
        tagger.tagassignments.append(assignment)

def append_custom_assignment_pattern(pattern: str, pattern_occurrences: int, tagger: Tagger, comment: Comment, charac: Characteristic):
    if pattern_occurrences == 0:
        raise ValueError("Pattern needs to occur at least once!")
    
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)
    assignments = []
    counter = 0

    # TODO logic flaw - allowing more than one tag assignment per person for one comment, and one characteristic

    # function creates tag assignment in chronological order as per given pattern for pattern_occurrences number of times
    for _ in range(pattern_occurrences):
        for tag_val_char in pattern:
            time_param = start_time + counter * interval
            counter += 1

            if tag_val_char == "Y":
                cur_assignment = TagAssignment(tagger, comment, charac, TagValue.YES, time_param)
            elif tag_val_char == "N":
                cur_assignment = TagAssignment(tagger, comment, charac, TagValue.NO, time_param)
            else:
                raise ValueError("Invalid pattern character! Can only be Y or N.")
            
            assignments.append(cur_assignment)
    
    for assignment in assignments:
        tagger.tagassignments.append(assignment)

# def create_YNYN_assignme

def test_detects_only_single_vertical_pattern_single_comment(generate_mock_data):
    tagger = generate_mock_data["tagger"]
    comment1 = generate_mock_data["comment1"]
    comment2 = generate_mock_data["comment2"]
    char1 = generate_mock_data["char1"]
    char2 = generate_mock_data["char2"]
    char3 = generate_mock_data["char3"]

    all_patterns = PatternCollection.return_all_patterns()

    strategy = VerticalPatternDetection()

    # passes
    for pattern in all_patterns: 
        create_custom_assignment_pattern(pattern, 1, tagger, comment1, char1)
        actual = strategy.analyze(tagger, char1)
        expected = {pattern : 1}

        assert actual == expected
        # tagger.tagassignments = []

def test_detects_only_single_vertical_pattern_multiple_comments(generate_mock_data):
    tagger = generate_mock_data["tagger"]
    comment1 = generate_mock_data["comment1"]
    comment2 = generate_mock_data["comment2"]
    char1 = generate_mock_data["char1"]
    char2 = generate_mock_data["char2"]
    char3 = generate_mock_data["char3"]

    all_patterns = PatternCollection.return_all_patterns()

    strategy = VerticalPatternDetection()

    count = 0
    # passes
    for pattern in all_patterns: 
        if count % 2 == 0:
            cur_comment = comment1
        else:
            cur_comment = comment2

        create_custom_assignment_pattern(pattern, 1, tagger, cur_comment, char1)
        actual = strategy.analyze(tagger, char1)
        expected = {pattern : 1}

        assert actual == expected
        count += 1


def test_detects_multiple_single_vertical_patterns_single_comment(generate_mock_data):
    tagger = generate_mock_data["tagger"]
    comment1 = generate_mock_data["comment1"]
    comment2 = generate_mock_data["comment2"]
    char1 = generate_mock_data["char1"]
    char2 = generate_mock_data["char2"]
    char3 = generate_mock_data["char3"]

    all_patterns = PatternCollection.return_all_patterns()

    strategy = VerticalPatternDetection()

    for pattern in all_patterns:
        # create_custom_assignment_pattern(pattern)
        append_custom_assignment_pattern(pattern, 1, tagger, comment1, char1)
    
    # Total sequence -> Y,N,YN,YNY,YNN,YNNY,YYYN,YNNN
    expected = {
            "YN": 7,
            "YNY" : 3,
            "YNN" : 3,
            "YNNY" : 1,
            "YYYN" : 1,
            "YNNN" : 1
        }
    
    actual = strategy.analyze(tagger, char1)
    
    assert actual == expected

def test_detects_multiple_single_vertical_patterns_multiple_comments(generate_mock_data):
    tagger = generate_mock_data["tagger"]
    comment1 = generate_mock_data["comment1"]
    comment2 = generate_mock_data["comment2"]
    char1 = generate_mock_data["char1"]
    char2 = generate_mock_data["char2"]
    char3 = generate_mock_data["char3"]

    all_patterns = PatternCollection.return_all_patterns()

    strategy = VerticalPatternDetection()

    count = 0
    for pattern in all_patterns:
        if count % 2 == 0:
            cur_comment = comment1
        else:
            cur_comment = comment2
        # create_custom_assignment_pattern(pattern)
        append_custom_assignment_pattern(pattern, 1, tagger, cur_comment, char1)
        count += 1
    
    # Total sequence -> Y,N,YN,YNY,YNN,YNNY,YYYN,YNNN
    expected = {
            "YN": 7,
            "YNY" : 3,
            "YNN" : 3,
            "YNNY" : 1,
            "YYYN" : 1,
            "YNNN" : 1
        }
    
    actual = strategy.analyze(tagger, char1)
    
    assert actual == expected

def test_detects_only_single_horizontal_pattern_one_comment(generate_mock_data):
    tagger = generate_mock_data["tagger"]
    comment1 = generate_mock_data["comment1"]
    comment2 = generate_mock_data["comment2"]
    char1 = generate_mock_data["char1"]
    char2 = generate_mock_data["char2"]
    char3 = generate_mock_data["char3"]

    all_patterns = PatternCollection.return_all_patterns()

    strategy = VerticalPatternDetection()

    for pattern in all_patterns: 
        create_custom_assignment_pattern(pattern, 1, tagger, comment1, char1)
        actual = strategy.analyze(tagger, char1)
        expected = {pattern : 1}

        assert actual == expected

def test_simple_sequence_patterns_signature(generate_mock_data):
    """
    Smoke test: Ensures the SimpleSequencePatterns class and its
    analyze method exist and correctly raise NotImplementedError.
    """

    
    # Scenarios

    # no patterns of any kind

    # 1 of each pattern - use the patterncollection.return_all_patterns() method to get list of patterns: iterate through
    # for each pattern in patterncollectoin.return_all_patterns():
        # generate sequence of assignments as part of pattern...(use clearer language)

    # 1 of specific patterns, and not others
    # for each pattern in patterncollectoin.return_all_patterns():
        # generate a sequence of assignments with that pattern only
        
    # assert pattern detection results in 1 count for each pattern

    # > 1 for each pattern (but not same values)
    # for each pattern in patterncollection.return_all_patterns():
        # use random number generator to determine pattern repeats and store the random num in a dict
        # generate sequence of assignments for repeats
    
    # assert dict of random nums is equal to result of pattern detection

    # >1 for specific patterns, and 0 for others
    # for each pattern in patterncollection.return_all_patterns():
        # generate sequence of assignments such that pattern is repeated twice
        # assert pattern detection results in 1 key-val pair of pattern:2, and no other dict entries
        
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







# def create_YES_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
#     start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
#     interval = timedelta(minutes=1)

#     assignments = []

#     for i in range(n_assignments):
#         time_param = start_time + i * interval
#         cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
#         assignments.append(cur_assignment)

#     # link tagassignments with taggers
#     for assignment in assignments:
#         tagger1.tagassignments.append(assignment)

# def create_NO_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
#     start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
#     interval = timedelta(minutes=1)

#     assignments = []

#     for i in range(n_assignments):
#         time_param = start_time + i * interval
#         cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
#         assignments.append(cur_assignment)

#     # link tagassignments with taggers
#     for assignment in assignments:
#         tagger1.tagassignments.append(assignment)

# def create_YN_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
#     start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
#     interval = timedelta(minutes=1)

#     assignments = []

#     for i in range(n_assignments):
#         time_param = start_time + i * interval
#         if i % 2 == 0:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
#         else:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
#         assignments.append(cur_assignment)

#     # link tagassignments with taggers
#     for assignment in assignments:
#         tagger1.tagassignments.append(assignment)

# def create_YNNY_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
#     start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
#     interval = timedelta(minutes=1)

#     assignments = []

#     for i in range(n_assignments):
#         time_param = start_time + i * interval
#         if i % 4 == 0:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
#         elif i % 4 == 1:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
#         elif i % 4 == 2:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
#         else:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)

#         assignments.append(cur_assignment)

#     # link tagassignments with taggers
#     for assignment in assignments:
#         tagger1.tagassignments.append(assignment)

# def create_NYYN_assignments(tagger1: Tagger, comment1: Comment, char1: Characteristic, n_assignments: int):
#     start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
#     interval = timedelta(minutes=1)

#     assignments = []

#     for i in range(n_assignments):
#         time_param = start_time + i * interval
#         if i % 4 == 0:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)
#         elif i % 4 == 1:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
#         elif i % 4 == 2:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.YES, time_param)
#         else:
#             cur_assignment = TagAssignment(tagger1, comment1, char1, TagValue.NO, time_param)

#         assignments.append(cur_assignment)

#     # link tagassignments with taggers
#     for assignment in assignments:
#         tagger1.tagassignments.append(assignment)