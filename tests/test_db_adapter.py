"""Tests for the MySQL database adapter."""

from datetime import datetime, timedelta, timezone

import pytest

from qcc.cli.main import _build_summary
from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.data_ingestion.mysql_importer import DEFAULT_TAG_PROMPT_TABLES
from qcc.io.db_adapter import DBAdapter
from qcc.domain.enums import TagValue
from qcc.domain.comment import Comment
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger


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
            "answer_id": 101,
            "tag_prompt_deployment_id": 5,
            "user_id": 42,
            "value": "1",
            "created_at": datetime(2024, 1, 1, 12, 0, 0),
        },
        {
            "id": 2,
            "answer_id": 102,
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
    assert assignments[1].comment_id == "102"
    assert assignments[1].value == TagValue.NO

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

    summary = _build_summary(domain_objects)
    assert summary["total_assignments"] == 2
    assert summary["total_answers"] == 2
    assert summary["total_prompts"] == 1
    assert summary["total_prompt_deployments"] == 1
    assert summary["total_questions"] == 1
    assert summary["assignments_by_value"] == {"YES": 1, "NO": 1}
    assert summary["characteristic_labels"] == {"5": "Spam?"}
    assert summary["prompt_control_types"] == {"yes_no": 1}
    assert summary["table_row_counts"] == {
        "answer_tags": 2,
        "answers": 2,
        "tag_prompt_deployments": 1,
        "tag_prompts": 1,
        "questions": 1,
    }


def test_read_assignments_applies_limit():
    answers = [
        {"id": 1, "question_id": 1, "comments": "Comment", "response_id": 11}
    ]
    answer_tags = [
        {
            "id": 1,
            "answer_id": 1,
            "tag_prompt_deployment_id": 5,
            "user_id": 7,
            "value": "1",
            "created_at": datetime(2024, 1, 1, 0, 0, 0),
        },
        {
            "id": 2,
            "answer_id": 1,
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
            "answer_id": 1,
            "tag_prompt_deployment_id": 10,
            "user_id": 7,
            "value": "-1",
            "created_at": datetime(2024, 1, 2, 0, 0, 0),
        },
        {
            "id": 2,
            "answer_id": 2,
            "tag_prompt_deployment_id": 10,
            "user_id": 8,
            "value": "-1.0",
            "created_at": datetime(2024, 1, 2, 0, 1, 0),
        },
    ]

    adapter = _make_adapter({"answer_tags": answer_tags, "answers": answers})

    assignments = adapter.read_assignments()

    assert [assignment.value for assignment in assignments] == [
        TagValue.NA,
        TagValue.NA,
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

    summary = _build_summary(domain_objects)

    speed_metrics = summary["tagger_speed_metrics"]
    assert speed_metrics["taggers_with_speed"] == 1
    assert speed_metrics["mean_log2_by_tagger"]["42"] == pytest.approx(3.0)
    assert speed_metrics["seconds_per_tag_by_tagger"]["42"] == pytest.approx(8.0)
    assert speed_metrics["mean_seconds_per_tag"] == pytest.approx(8.0)
    assert speed_metrics["median_seconds_per_tag"] == pytest.approx(8.0)


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
            "answerId": 501,
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

