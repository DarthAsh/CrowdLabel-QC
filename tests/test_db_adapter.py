"""Tests for the MySQL database adapter."""

from datetime import datetime, timedelta, timezone

import pytest

import csv

from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.data_ingestion.mysql_importer import DEFAULT_TAG_PROMPT_TABLES
from qcc.io.db_adapter import DBAdapter
from qcc.domain.enums import TagValue
from qcc.domain.characteristic import Characteristic
from qcc.domain.comment import Comment
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.reports.tagger_performance import TaggerPerformanceReport


class FakeImporter:
    """Simple importer stub that returns predefined table data."""

    def __init__(self, tables):
        self._tables = tables

    def fetch_table(self, table_name, limit=None):
        rows = list(self._tables.get(table_name, []))
        if limit is not None:
            return rows[: int(limit)]
        return rows

    def import_tables(self, table_names, limit=None):
        return {name: self.fetch_table(name, limit=limit) for name in table_names}


def _make_adapter(tables):
    config = MySQLConfig(host="localhost", user="user", password="pw", database="db")
    importer = FakeImporter(tables)
    return DBAdapter(config, importer=importer, tables=DEFAULT_TAG_PROMPT_TABLES)


def test_db_adapter_merges_answers_with_tags():
    answers = [
        {
            "id": 101,
            "question_id": 7,
            "answer": "Yes",
            "comments": "First answer",
            "response_id": 501,
        },
        {
            "id": 102,
            "question_id": 8,
            "comments": "Second answer",
            "response_id": None,
        },
    ]
    answer_tags = [
        {
            "id": 1,
            "comment_id": 101,
            "tag_prompt_deployment_id": 5,
            "user_id": 42,
            "value": "1",
            "created_at": datetime(2024, 1, 1, 12, 0, 0),
        },
        {
            "id": 2,
            "comment_id": 102,
            "tag_prompt_deployment_id": 5,
            "user_id": 99,
            "value": "0",
            "created_at": datetime(2024, 1, 1, 12, 5, 0),
        },
    ]
    tag_prompt_deployments = [
        {
            "id": 5,
            "tag_prompt_id": 12,
            "assignment_id": 7,
            "questionnaire_id": 88,
            "question_type": "boolean",
            "created_at": datetime(2023, 12, 1, 0, 0, 0),
            "updated_at": datetime(2023, 12, 2, 0, 0, 0),
        }
    ]
    tag_prompts = [
        {
            "id": 12,
            "prompt": "Spam?",
            "desc": "Mark spam answers",
            "control_type": "yes_no",
            "created_at": datetime(2023, 11, 1, 0, 0, 0),
            "updated_at": datetime(2023, 11, 2, 0, 0, 0),
        }
    ]
    questions = [
        {
            "id": 7,
            "txt": "Is this spam?",
            "questionnaire_id": 88,
            "seq": 1,
            "type": "boolean",
            "max_label": "Yes",
            "min_label": "No",
        }
    ]

    adapter = _make_adapter(
        {
            "answer_tags": answer_tags,
            "answers": answers,
            "tag_prompt_deployments": tag_prompt_deployments,
            "tag_prompts": tag_prompts,
            "questions": questions,
        }
    )

    domain_objects = adapter.read_domain_objects()

    assignments = domain_objects["assignments"]
    assert len(assignments) == 2
    assert assignments[0].comment_id == "101"
    assert assignments[0].value == TagValue.YES
    assert assignments[0].question_id == "7"
    assert assignments[0].questionnaire_id == "88"
    assert assignments[1].comment_id == "102"
    assert assignments[1].value == TagValue.SKIP
    assert assignments[1].question_id == "8"
    assert assignments[1].questionnaire_id == "88"

    comments = {comment.id: comment for comment in domain_objects["comments"]}
    assert comments["101"].text == "First answer"
    assert comments["101"].prompt_id == "7"
    assert comments["102"].text == "Second answer"
    assert comments["102"].prompt_id == "8"

    answers_output = {answer["id"]: answer for answer in domain_objects["answers"]}
    assert answers_output["101"]["question_id"] == "7"
    assert answers_output["102"]["question_id"] == "8"
    assert answers_output["101"]["text"] == "First answer"

    characteristics = {c.id: c for c in domain_objects["characteristics"]}
    assert characteristics["5"].name == "Spam?"

    prompts = {prompt["id"]: prompt for prompt in domain_objects["prompts"]}
    assert prompts["12"]["prompt"] == "Spam?"

    deployments = {
        deployment["id"]: deployment for deployment in domain_objects["prompt_deployments"]
    }
    assert deployments["5"]["question_id"] == "7"
    assert deployments["5"]["prompt_label"] == "Spam?"

    report = TaggerPerformanceReport(domain_objects["assignments"])
    summary = report.generate_summary_report(
        domain_objects.get("taggers", []),
        domain_objects.get("characteristics", []),
    )
    assert "tagger_speed" in summary
    speed_summary = summary["tagger_speed"]
    assert speed_summary["strategy"] == "LogTrimTaggingSpeed"
    assert "per_tagger" in speed_summary


def test_read_assignments_applies_limit():
    answers = [
        {"id": 1, "question_id": 1, "comments": "Comment", "response_id": 11}
    ]
    answer_tags = [
        {
            "id": 1,
            "comment_id": 1,
            "tag_prompt_deployment_id": 5,
            "user_id": 7,
            "value": "1",
            "created_at": datetime(2024, 1, 1, 0, 0, 0),
        },
        {
            "id": 2,
            "comment_id": 1,
            "tag_prompt_deployment_id": 5,
            "user_id": 8,
            "value": "0",
            "created_at": datetime(2024, 1, 1, 0, 1, 0),
        },
    ]

    adapter = _make_adapter({"answer_tags": answer_tags, "answers": answers})

    limited_assignments = adapter.read_assignments(limit=1)
    assert len(limited_assignments) == 1
    assert limited_assignments[0].tagger_id == "7"
    assert limited_assignments[0].value == TagValue.YES


def test_db_adapter_normalizes_negative_numeric_tag_values():
    answers = [
        {"id": 1, "question_id": 99, "comments": "First", "response_id": 123},
        {"id": 2, "question_id": 100, "comments": "Second", "response_id": 124},
    ]
    answer_tags = [
        {
            "id": 1,
            "comment_id": 1,
            "tag_prompt_deployment_id": 10,
            "user_id": 7,
            "value": "-1",
            "created_at": datetime(2024, 1, 2, 0, 0, 0),
        },
        {
            "id": 2,
            "comment_id": 2,
            "tag_prompt_deployment_id": 10,
            "user_id": 8,
            "value": "-1.0",
            "created_at": datetime(2024, 1, 2, 0, 1, 0),
        },
    ]

    adapter = _make_adapter({"answer_tags": answer_tags, "answers": answers})

    assignments = adapter.read_assignments()

    assert [assignment.value for assignment in assignments] == [
        TagValue.NO,
        TagValue.NO,
    ]


def test_summary_includes_speed_metrics():
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assignments = [
        TagAssignment(
            tagger_id="42",
            comment_id="comment-1",
            characteristic_id="5",
            value=TagValue.YES,
            timestamp=now,
        ),
        TagAssignment(
            tagger_id="42",
            comment_id="comment-2",
            characteristic_id="5",
            value=TagValue.NO,
            timestamp=now + timedelta(seconds=8),
        ),
        TagAssignment(
            tagger_id="99",
            comment_id="comment-3",
            characteristic_id="7",
            value=TagValue.YES,
            timestamp=now + timedelta(seconds=30),
        ),
    ]

    domain_objects = {
        "assignments": assignments,
        "comments": [
            Comment(id="comment-1", text="c1", prompt_id="p1", tagassignments=[assignments[0]]),
            Comment(id="comment-2", text="c2", prompt_id="p1", tagassignments=[assignments[1]]),
            Comment(id="comment-3", text="c3", prompt_id="p2", tagassignments=[assignments[2]]),
        ],
        "taggers": [
            Tagger(id="42", meta=None, tagassignments=assignments[:2]),
            Tagger(id="99", meta=None, tagassignments=[assignments[2]]),
        ],
        "characteristics": [],
        "answers": [],
        "prompt_deployments": [],
        "prompts": [],
        "questions": [],
    }

    report = TaggerPerformanceReport(domain_objects["assignments"])
    summary = report.generate_summary_report(
        domain_objects.get("taggers", []),
        domain_objects.get("characteristics", []),
    )

    speed_metrics = summary["tagger_speed"]
    per_tagger = {entry["tagger_id"]: entry for entry in speed_metrics["per_tagger"]}
    assert len(per_tagger) == 1
    assert per_tagger["42"]["mean_log2"] == pytest.approx(3.0)
    assert per_tagger["42"]["seconds_per_tag"] == pytest.approx(8.0)


def test_summary_includes_pattern_metrics(tmp_path):
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assignments = [
        TagAssignment(
            tagger_id="42",
            comment_id="comment-1",
            characteristic_id="5",
            value=TagValue.YES,
            timestamp=now,
        ),
        TagAssignment(
            tagger_id="42",
            comment_id="comment-2",
            characteristic_id="5",
            value=TagValue.NO,
            timestamp=now + timedelta(seconds=8),
        ),
        TagAssignment(
            tagger_id="42",
            comment_id="comment-3",
            characteristic_id="5",
            value=TagValue.YES,
            timestamp=now + timedelta(seconds=16),
        ),
        TagAssignment(
            tagger_id="42",
            comment_id="comment-4",
            characteristic_id="5",
            value=TagValue.NO,
            timestamp=now + timedelta(seconds=24),
        ),
        TagAssignment(
            tagger_id="99",
            comment_id="comment-5",
            characteristic_id="7",
            value=TagValue.NO,
            timestamp=now + timedelta(seconds=32),
        ),
    ]

    domain_objects = {
        "assignments": assignments,
        "comments": [],
        "taggers": [
            Tagger(id="42", meta=None, tagassignments=assignments[:4]),
            Tagger(id="99", meta=None, tagassignments=[assignments[4]]),
        ],
        "characteristics": [],
        "answers": [],
        "prompt_deployments": [],
        "prompts": [],
        "questions": [],
    }

    report = TaggerPerformanceReport(domain_objects["assignments"])
    summary = report.generate_summary_report(
        domain_objects.get("taggers", []),
        domain_objects.get("characteristics", []),
    )

    pattern_summary = summary["pattern_detection"]
    assert pattern_summary["strategy"] == "HorizontalPatternDetection"
    assert pattern_summary["patterns_tracked"] == [
        "Y",
        "N",
        "YN",
        "YNY",
        "YNN",
        "YNNY",
        "YYYN",
        "YNNN",
    ]
    per_tagger = {entry["tagger_id"]: entry for entry in pattern_summary["per_tagger"]}
    assert per_tagger["42"]["patterns"] == {"YN": 2}

    csv_path = tmp_path / "summary.csv"
    report.export_to_csv(summary, csv_path)

    with csv_path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    assert all(row["user_id"] != "aggregate" for row in rows)

    tagger_row = next(row for row in rows if row["user_id"] == "42")
    assert tagger_row["pattern_strategy"] == "HorizontalPatternDetection"
    assert tagger_row["pattern_count_YN"] == "2"


def test_db_adapter_handles_camel_cased_answer_identifiers():
    answers = [
        {
            "answerId": 501,
            "questionId": 321,
            "responseId": 654,
            "comments": "Camel",
        },
    ]
    answer_tags = [
        {
            "ID": 7,
            "commentId": 501,
            "tagPromptDeploymentId": 11,
            "userId": 9001,
            "value": "TRUE",
            "createdAt": datetime(2024, 3, 1, 9, 30, 0),
        }
    ]

    adapter = _make_adapter({"answer_tags": answer_tags, "answers": answers})

    domain_objects = adapter.read_domain_objects()

    assignments = domain_objects["assignments"]
    assert len(assignments) == 1
    assert assignments[0].comment_id == "501"
    assert assignments[0].tagger_id == "9001"
    assert assignments[0].characteristic_id == "11"
    assert assignments[0].value == TagValue.YES

    answers_output = domain_objects["answers"]
    assert answers_output[0]["id"] == "501"
    assert answers_output[0]["question_id"] == "321"


def test_db_adapter_uses_answer_id_for_comment_mapping():
    answers = [
        {
            "id": 777,
            "question_id": 12,
            "comments": "Answer text",
        }
    ]
    answer_tags = [
        {
            "id": 3,
            "answer_id": 777,
            "tag_prompt_deployment_id": 15,
            "user_id": 54,
            "value": -1,
            "created_at": datetime(2024, 5, 1, 8, 0, 0),
        },
        {
            "id": 4,
            "answerId": 777,
            "tag_prompt_deployment_id": 15,
            "user_id": 54,
            "value": 0,
            "created_at": datetime(2024, 5, 1, 8, 5, 0),
        },
    ]

    adapter = _make_adapter({"answer_tags": answer_tags, "answers": answers})

    domain_objects = adapter.read_domain_objects()

    assignments = domain_objects["assignments"]
    assert len(assignments) == 2
    assert {assignment.comment_id for assignment in assignments} == {"777"}
    assert {assignment.value for assignment in assignments} == {
        TagValue.NO,
        TagValue.SKIP,
    }


def test_db_adapter_skips_rows_without_user_id(caplog):
    caplog.set_level("INFO")

    answers = [
        {
            "id": 555,
            "question_id": 101,
            "comments": "Answer body",
        }
    ]
    questions = [
        {
            "id": 101,
            "questionnaire_id": 303,
            "text": "Question text",
        }
    ]
    assignment_questionnaires = [
        {
            "assignment_id": 9001,
            "questionnaire_id": 303,
            # deliberately omit user_id to force skipping
        }
    ]
    answer_tags = [
        {
            "id": 12,
            "answer_id": 555,
            "tag_prompt_deployment_id": 31,
            "value": 1,
            "created_at": datetime(2024, 5, 2, 9, 0, 0),
        }
    ]

    adapter = _make_adapter(
        {
            "answer_tags": answer_tags,
            "answers": answers,
            "questions": questions,
            "assignment_questionnaires": assignment_questionnaires,
        }
    )

    domain_objects = adapter.read_domain_objects()

    assert domain_objects["assignments"] == []
    assert "Skipping assignment row with no user_id/tagger" in caplog.text


def test_db_adapter_creates_skip_for_answers_without_tags():
    answers = [
        {
            "id": 202,
            "question_id": 88,
            "comments": "Untouched answer",
            "created_at": datetime(2024, 6, 1, 12, 0, 0),
        }
    ]
    questions = [
        {
            "id": 88,
            "questionnaire_id": 1001,
        }
    ]
    assignment_questionnaires = [
        {
            "assignment_id": 555,
            "questionnaire_id": 1001,
            "user_id": 77,
        }
    ]
    tag_prompt_deployments = [
        {
            "id": 42,
            "assignment_id": 88,
            "questionnaire_id": 1001,
        }
    ]

    adapter = _make_adapter(
        {
            "answers": answers,
            "questions": questions,
            "assignment_questionnaires": assignment_questionnaires,
            "tag_prompt_deployments": tag_prompt_deployments,
            "answer_tags": [],
        }
    )

    domain_objects = adapter.read_domain_objects()

    assignments = domain_objects["assignments"]
    assert len(assignments) == 1
    skip_assignment = assignments[0]
    assert skip_assignment.comment_id == "202"
    assert skip_assignment.characteristic_id == "42"
    assert skip_assignment.value == TagValue.SKIP
    assert skip_assignment.assignment_id == "555"
    assert skip_assignment.tagger_id == "77"
    assert skip_assignment.timestamp == datetime(2024, 6, 1, 12, 0, 0)


def test_questionnaire_root_assignments_default_identities():
    assignment_questionnaires = [
        {"assignment_id": 1205, "questionnaire_id": 77, "user_id": 55}
    ]
    questions = [
        {"id": 123, "questionnaire_id": 77, "text": "Q text"},
    ]
    answers = [
        {"id": 321, "question_id": 123, "comments": "answer body"},
    ]
    tag_prompt_deployments = [
        {
            "id": 11,
            "assignment_id": 123,
            "questionnaire_id": 77,
            "tag_prompt_id": 3,
        }
    ]
    tag_prompts = [{"id": 3, "prompt": "Spam?"}]
    answer_tags = [
        {
            "id": 7,
            "comment_id": 321,
            "tag_prompt_deployment_id": 11,
            "value": 1,
            "created_at": datetime(2024, 7, 1, 12, 0, 0),
        },
        {
            "id": 9,
            "comment_id": 9999,  # unrelated answer, should be ignored
            "tag_prompt_deployment_id": 11,
            "value": 1,
            "created_at": datetime(2024, 7, 1, 13, 0, 0),
        },
    ]

    adapter = _make_adapter(
        {
            "assignment_questionnaires": assignment_questionnaires,
            "questions": questions,
            "answers": answers,
            "tag_prompt_deployments": tag_prompt_deployments,
            "tag_prompts": tag_prompts,
            "answer_tags": answer_tags,
        }
    )

    assignments = adapter.read_assignments_from_questionnaires()

    assert len(assignments) == 1
    assignment = assignments[0]
    assert assignment.tagger_id == "55"
    assert assignment.assignment_id == "1205"
    assert assignment.characteristic_id == "11"
    assert assignment.comment_id == "321"
    assert assignment.value == TagValue.YES


def test_questionnaire_root_domain_objects_builds_comments_and_taggers():
    assignment_questionnaires = [
        {"assignment_id": 1205, "questionnaire_id": 5, "user_id": 101}
    ]
    questions = [
        {"id": 2, "questionnaire_id": 5, "text": "Is this spam?"},
    ]
    answers = [
        {"id": 8, "question_id": 2, "comments": "Sample body"},
    ]
    tag_prompt_deployments = [
        {"id": 66, "assignment_id": 2, "questionnaire_id": 5, "tag_prompt_id": 5}
    ]
    tag_prompts = [{"id": 5, "prompt": "Spam?"}]
    answer_tags = [
        {
            "id": 1,
            "comment_id": 8,
            "tag_prompt_deployment_id": 66,
            "value": 0,
            "created_at": datetime(2024, 7, 2, 9, 0, 0),
        }
    ]

    adapter = _make_adapter(
        {
            "assignment_questionnaires": assignment_questionnaires,
            "questions": questions,
            "answers": answers,
            "tag_prompt_deployments": tag_prompt_deployments,
            "tag_prompts": tag_prompts,
            "answer_tags": answer_tags,
        }
    )

    domain_objects = adapter.read_domain_objects_from_questionnaires()

    assignments = domain_objects["assignments"]
    assert len(assignments) == 1
    assert assignments[0].tagger_id == "101"
    assert assignments[0].assignment_id == "1205"

    comments = domain_objects["comments"]
    assert len(comments) == 1
    assert comments[0].id == "8"
    assert comments[0].text == "Sample body"

    taggers = domain_objects["taggers"]
    assert len(taggers) == 1
    assert taggers[0].id == "101"
    assert taggers[0].tagassignments == assignments


def test_questionnaire_root_filters_assignment_questionnaires_by_assignment_id():
    assignment_questionnaires = [
        {"assignment_id": 1205, "questionnaire_id": 1, "user_id": 10},
        {"assignment_id": 777, "questionnaire_id": 2, "user_id": 11},
    ]
    questions = [
        {"id": 11, "questionnaire_id": 1, "text": "Q1"},
        {"id": 22, "questionnaire_id": 2, "text": "Q2"},
    ]
    answers = [
        {"id": 101, "question_id": 11, "comments": "A1"},
        {"id": 202, "question_id": 22, "comments": "A2"},
    ]
    tag_prompt_deployments = [
        {"id": 5, "assignment_id": 11, "questionnaire_id": 1, "tag_prompt_id": 9},
        {"id": 6, "assignment_id": 22, "questionnaire_id": 2, "tag_prompt_id": 9},
    ]
    tag_prompts = [{"id": 9, "prompt": "Spam?"}]
    answer_tags = [
        {
            "id": 1,
            "comment_id": 101,
            "tag_prompt_deployment_id": 5,
            "value": 1,
            "created_at": datetime(2024, 8, 1, 10, 0, 0),
        },
        {
            "id": 2,
            "comment_id": 202,
            "tag_prompt_deployment_id": 6,
            "value": 0,
            "created_at": datetime(2024, 8, 1, 10, 5, 0),
        },
    ]

    adapter = _make_adapter(
        {
            "assignment_questionnaires": assignment_questionnaires,
            "questions": questions,
            "answers": answers,
            "tag_prompt_deployments": tag_prompt_deployments,
            "tag_prompts": tag_prompts,
            "answer_tags": answer_tags,
        }
    )

    assignments = adapter.read_assignments_from_questionnaires()

    assert len(assignments) == 1
    assignment = assignments[0]
    assert assignment.assignment_id == "1205"
    assert assignment.tagger_id == "10"
    assert assignment.comment_id == "101"
    assert assignment.value == TagValue.YES

def test_db_adapter_logs_invalid_rows(caplog):
    caplog.set_level("ERROR")

    tables = {
        "answer_tags": [
            {
                "id": 1,
                "comment_id": 101,
                "tag_prompt_deployment_id": 5,
                "user_id": "trouble-user",
                # intentionally omit "value" to trigger a parsing error
            }
        ]
    }

    adapter = _make_adapter(tables)

    with pytest.raises(ValueError):
        adapter.read_assignments()

    messages = [record.getMessage() for record in caplog.records]
    assert any("user_id=trouble-user" in message for message in messages)


def test_tagger_performance_report_includes_agreement_summary():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    assignments = [
        TagAssignment(
            tagger_id="1",
            comment_id="c1",
            characteristic_id="char",
            value=TagValue.YES,
            timestamp=base_time,
        ),
        TagAssignment(
            tagger_id="2",
            comment_id="c1",
            characteristic_id="char",
            value=TagValue.YES,
            timestamp=base_time + timedelta(minutes=1),
        ),
        TagAssignment(
            tagger_id="1",
            comment_id="c2",
            characteristic_id="char",
            value=TagValue.NO,
            timestamp=base_time + timedelta(minutes=2),
        ),
        TagAssignment(
            tagger_id="2",
            comment_id="c2",
            characteristic_id="char",
            value=TagValue.NO,
            timestamp=base_time + timedelta(minutes=3),
        ),
    ]

    taggers = [
        Tagger(id="1", meta=None, tagassignments=[assignments[0], assignments[2]]),
        Tagger(id="2", meta=None, tagassignments=[assignments[1], assignments[3]]),
    ]
    characteristics = [Characteristic("char", "Spam?")]

    report = TaggerPerformanceReport(assignments)
    summary = report.generate_summary_report(
        taggers,
        characteristics,
        include_agreement=True,
    )

    agreement = summary.get("agreement")
    assert agreement["strategy"] == "LatestLabelPercentAgreement"
    per_characteristic = agreement["per_characteristic"]
    assert len(per_characteristic) == 1

    char_entry = per_characteristic[0]
    assert char_entry["characteristic_id"] == "char"
    assert char_entry["percent_agreement"] == pytest.approx(1.0)
    assert char_entry["cohens_kappa"] == pytest.approx(1.0)
    assert char_entry["krippendorffs_alpha"] == 1.0
    per_tagger = {entry["tagger_id"]: entry for entry in char_entry["per_tagger"]}
    assert per_tagger["1"]["percent_agreement"] == pytest.approx(1.0)
    assert per_tagger["1"]["cohens_kappa"] == pytest.approx(1.0)
    assert per_tagger["2"]["percent_agreement"] == pytest.approx(1.0)
    assert per_tagger["2"]["cohens_kappa"] == pytest.approx(1.0)
