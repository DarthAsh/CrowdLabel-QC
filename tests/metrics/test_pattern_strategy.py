from __future__ import annotations

import pytest
from datetime import datetime, timedelta

from qcc.domain.characteristic import Characteristic
from qcc.metrics.pattern_strategy import VerticalPatternDetection, HorizontalPatternDetection
from qcc.domain.tagger import Tagger
from qcc.domain.comment import Comment
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
# from src.qcc.domain.

def make_empty_tagger() -> Tagger:
    return Tagger(id="t0", meta=None, tagassignments=[])

@pytest.fixture(scope="module")
def generate_mock_data():
    # create comments
    comments = generate_comments()
    comment1 =  Comment("1", "Comment 1", "1", [])
    comment2 =  Comment("2", "Comment 2", "1", [])
    # create characteristics
    char1 = Characteristic("1", "Char 1")
    char2 = Characteristic("2", "Char 2")
    char3 = Characteristic("3", "Char 3")

    chars = []
    chars.append(char1)
    chars.append(char2)
    chars.append(char3)

    # char1 = Characteristic("1", "Char 1")

    # create taggers
    tagger1 = Tagger("1", [])
    # tagger2 = Tagger("2", [])

    return {"tagger" : tagger1, "comments": comments, "chars": chars, "search_char" : chars[0]}

def generate_comments():
    comments = []
    for i in range(10):
        index = str(i)
        cur_comment = Comment(index, "Comment " + index, index, [])
        comments.append(cur_comment)
    
    return comments

def create_custom_assignment_pattern_vertical(pattern: str, pattern_occurrences: int, tagger: Tagger, comments: list[Comment], charac: Characteristic):
    if pattern_occurrences == 0:
        raise ValueError("Pattern needs to occur at least once!")
    
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)
    assignments = []
    counter = 0

    # TODO logic flaw - allowing more than one tag assignment per person for one comment, and one characteristic

    # function creates tag assignment in chronological order as per given pattern for pattern_occurrences number of times

    # create assignment on various comments
    comment_counter = 0
    tagger_id = tagger.id
    charac_id = charac.id
    for _ in range(pattern_occurrences):
        for tag_val_char in pattern:
            time_param = start_time + counter * interval
            counter += 1

            comment = comments[comment_counter]
            comment_counter += 1
            comment_counter = comment_counter % len(comments)

            if tag_val_char == "Y":
                cur_assignment = TagAssignment(tagger_id, comment.id, charac_id, TagValue.YES, time_param)
            elif tag_val_char == "N":
                cur_assignment = TagAssignment(tagger_id, comment.id, charac_id, TagValue.NO, time_param)
            else:
                raise ValueError("Invalid pattern character! Can only be Y or N.")
            
            assignments.append(cur_assignment)

    tagger.tagassignments = []
    for assignment in assignments:
        tagger.tagassignments.append(assignment)

def create_custom_assignment_pattern_horizontal(pattern: str, pattern_occurrences: int, tagger: Tagger, comments: list[Comment], characs: list[Characteristic]):
    if pattern_occurrences == 0:
        raise ValueError("Pattern needs to occur at least once!")
    
    start_time = datetime(year=2025, month=10, day=28, hour=0, minute=30)
    interval = timedelta(minutes=1)
    assignments = []
    counter = 0

    # TODO logic flaw - allowing more than one tag assignment per person for one comment, and one characteristic

    # function creates tag assignment in chronological order as per given pattern for pattern_occurrences number of times
    # vary characteristic
    # vary comment

    comment_counter = 0
    char_counter = 0
    tagger_id = tagger.id
    for _ in range(pattern_occurrences):
        for tag_val_char in pattern:
            time_param = start_time + counter * interval
            counter += 1

            comment = comments[comment_counter]
            comment_counter += 1
            comment_counter = comment_counter % len(comments)

            charac = characs[char_counter]
            char_counter += 1
            char_counter = char_counter % len(characs)

            if tag_val_char == "Y":
                cur_assignment = TagAssignment(tagger_id, comment.id, charac.id, TagValue.YES, time_param)
            elif tag_val_char == "N":
                cur_assignment = TagAssignment(tagger_id, comment.id, charac.id, TagValue.NO, time_param)
            else:
                raise ValueError("Invalid pattern character! Can only be Y or N.")
            
            assignments.append(cur_assignment)

    tagger.tagassignments = []
    for assignment in assignments:
        tagger.tagassignments.append(assignment)

def set_scenario(pattern: str, strategy: str, mock_data):
    strategy_obj = None
    
    tagger = mock_data["tagger"]
    comments = mock_data["comments"]
    chars = mock_data["chars"]
    search_char = mock_data["search_char"]

    if strategy == "V":
        strategy_obj = VerticalPatternDetection()
        create_custom_assignment_pattern_vertical(pattern, 1, tagger, comments, chars[0])
    elif strategy == "H":
        strategy_obj = HorizontalPatternDetection()
        create_custom_assignment_pattern_horizontal(pattern, 1, tagger, comments, chars)

    return strategy_obj
    
# scenarios
# only length-2 patterns: YYNYYN | YY: 2, YN: 2
# length-2 : YN -> YN : 1
# length-2 & 3 : YNY -> YN/NY : 1, YNY : 1
# length-3 & 4 : YNYN -> YN : 2, YNY: 1, NYN: 1, YNYN : 4
# YYNYYNNY -> YY: 2, NN:1, YN: 2, YYN/NYY: 2, YNY: 1, NNY/YNN: 1, YYNY: 1, YNNY : 1, YYNN: 1
# YNNYYNNY -> YN:2, NN: 2, YY: 1, YNN/NNY:2, NYY/YYN: 1, NNYY/YYNN: 1

# def test_pattern_detection_algo():

@pytest.mark.parametrize(
    "pattern_str, expected",
    [
        ("YNYYNYYNYYNY", { "YNY" : 1}),
        ("NYNYYNYYNYYNY", { "YNY" : 1}),
        ("YYNYYNYYNYYNY", { "YYN" : 1}),                              
        ("YNYYYNYYYNYY", { "YNYY" : 1}),
        ("NYNYYYNYYYNYY", { "YNYY" : 1}),
        ("YNYYNYYNYYNYNNYNYYNYYNYYNY", { "YNY" : 2}),
        ("YNYYNYYNYYNYYYNYYYNYYYNYY", { "YYNY" : 1}),
        ("YNYYNYYNYYNYNYYNYYYNYYYNY", { "YNY" : 1, "YYNY": 1}),                
        ("YNYYYNYYYNYY", { "YNYY" : 1}),
        ("YNYYYNYYYNYYYNYYYNYYYNYY", { "YNYY" : 2}),
        ("YNYYYNYYYNYYNYYNYNYNYNYNYN", { "YNYY" : 1, "YNYN" : 1}),
        ("NNYYNNNYYYNN", {}),
        ("YNYYNYYNYYNYNNYYNNNYYYNNYNYYNYYNYYNY", { "YNY" : 2}),
        ("YNYYYNYYYNYYNNYYNNNYYYNNYNYYYNYYYNYY", { "YNYY" : 2})
    ]    
)
def test_vertical_pattern_detection(pattern_str, expected, generate_mock_data):
    strategy = set_scenario(pattern_str, "V", generate_mock_data)
    tagger = generate_mock_data["tagger"]
    chars = generate_mock_data["chars"]

    actual = strategy.analyze(tagger, chars[0])

    assert actual == expected

@pytest.mark.parametrize(
    "pattern_str, expected",
    [
        ("YNYYNYYNYYNY", { "YNY" : 1}),
        ("NYNYYNYYNYYNY", { "YNY" : 1}),
        ("YYNYYNYYNYYNY", { "YYN" : 1}),                              
        ("YNYYYNYYYNYY", { "YNYY" : 1}),
        ("NYNYYYNYYYNYY", { "YNYY" : 1}),
        ("YNYYNYYNYYNYNNYNYYNYYNYYNY", { "YNY" : 2}),
        ("YNYYNYYNYYNYYYNYYYNYYYNYY", { "YYNY" : 1}),
        ("YNYYNYYNYYNYNYYNYYYNYYYNY", { "YNY" : 1, "YYNY": 1}),                
        ("YNYYYNYYYNYY", { "YNYY" : 1}),
        ("YNYYYNYYYNYYYNYYYNYYYNYY", { "YNYY" : 2}),
        ("YNYYYNYYYNYYNYYNYNYNYNYNYN", { "YNYY" : 1, "YNYN" : 1}),
        ("NNYYNNNYYYNN", {}),
        ("YNYYNYYNYYNYNNYYNNNYYYNNYNYYNYYNYYNY", { "YNY" : 2}),
        ("YNYYYNYYYNYYNNYYNNNYYYNNYNYYYNYYYNYY", { "YNYY" : 2})
    ]    
)
def test_horizontal_pattern_detection(pattern_str, expected, generate_mock_data):
    strategy = set_scenario(pattern_str, "H", generate_mock_data)
    tagger = generate_mock_data["tagger"]
    chars = generate_mock_data["chars"]

    actual = strategy.analyze(tagger)

    assert actual == expected
    