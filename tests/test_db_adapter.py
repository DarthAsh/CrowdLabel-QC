"""Tests for the MySQL database adapter."""

from datetime import datetime

from qcc.cli.main import _build_summary
from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.io.db_adapter import DBAdapter
from qcc.domain.enums import TagValue


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
    return DBAdapter(config, importer=importer, tables=tuple(tables.keys()))


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

    adapter = _make_adapter({"answer_tags": answer_tags, "answers": answers})

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

    summary = _build_summary(domain_objects)
    assert summary["total_assignments"] == 2
    assert summary["total_answers"] == 2
    assert summary["assignments_by_value"] == {"YES": 1, "NO": 1}
    assert summary["table_row_counts"] == {"answer_tags": 2, "answers": 2}


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

