from unittest.mock import MagicMock

from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.io.csv_adapter import CSVAdapter
from qcc.io.db_adapter import DBAdapter


def test_csv_adapter_preserves_assignment_id(tmp_path):
    csv_path = tmp_path / "assignments.csv"
    csv_path.write_text(
        """
assignment_id,team_id,tagger_id,comment_id,prompt_id,characteristic,value,tagged_at,comment_text,prompt_text
assign-1,team-1,worker-1,comment-1,prompt-1,char-1,YES,2024-01-01T00:00:00Z,hello,prompt
""".strip()
    )

    adapter = CSVAdapter()
    assignments = adapter.read_assignments(csv_path)

    assert assignments[0].assignment_id == "assign-1"
    assert assignments[0].prompt_id == "prompt-1"
    assert assignments[0].team_id == "team-1"


def test_db_adapter_preserves_assignment_id():
    adapter = DBAdapter(MySQLConfig("host", "user", "password", "database"), importer=MagicMock(), tables=["assignments"])

    assignment = adapter._row_to_assignment(
        {
            "tagger_id": "worker-1",
            "comment_id": "comment-1",
            "characteristic_id": "char-1",
            "value": 1,
            "tagged_at": "2024-01-01T00:00:00Z",
            "assignment_id": "assign-2",
            "prompt_id": "prompt-2",
            "team_id": "team-2",
        }
    )

    assert assignment.assignment_id == "assign-2"
    assert assignment.prompt_id == "prompt-2"
    assert assignment.team_id == "team-2"


def test_db_adapter_resolves_assignment_via_questionnaire_chain():
    importer = MagicMock()
    adapter = DBAdapter(
        MySQLConfig("host", "user", "password", "database"),
        importer=importer,
        tables=[
            "assignments",
            "answers",
            "questions",
            "assignment_questionnaires",
            "tag_prompt_deployments",
        ],
    )

    assignments, _ = adapter._build_assignments(
        [
            {
                "tagger_id": "worker-1",
                "comment_id": "comment-1",
                "characteristic_id": "deployment-1",
                "value": 1,
                "tagged_at": "2024-01-01T00:00:00Z",
                "assignment_id": "assign-from-row",
            }
        ],
        {
            "answers": [
                {"id": "comment-1", "question_id": "question-1"},
            ],
            "questions": [
                {"id": "question-1", "questionnaire_id": "questionnaire-1"}
            ],
            "assignment_questionnaires": [
                {
                    "questionnaire_id": "questionnaire-1",
                    "assignment_id": "assign-from-questionnaire",
                }
            ],
            "tag_prompt_deployments": [
                {"id": "deployment-1", "assignment_id": "assign-from-deployment"}
            ],
        },
    )

    assert len(assignments) == 1
    assert assignments[0].assignment_id == "assign-from-questionnaire"


def test_db_adapter_uses_questionnaire_user_when_tagger_missing():
    importer = MagicMock()
    adapter = DBAdapter(
        MySQLConfig("host", "user", "password", "database"),
        importer=importer,
        tables=[
            "assignments",
            "answers",
            "questions",
            "assignment_questionnaires",
            "tag_prompt_deployments",
        ],
    )

    assignments, _ = adapter._build_assignments(
        [
            {
                "comment_id": "comment-1",
                "characteristic_id": "deployment-1",
                "value": 1,
                "tagged_at": "2024-01-01T00:00:00Z",
            }
        ],
        {
            "answers": [
                {"id": "comment-1", "question_id": "question-1"},
            ],
            "questions": [
                {"id": "question-1", "questionnaire_id": "questionnaire-1"}
            ],
            "assignment_questionnaires": [
                {
                    "questionnaire_id": "questionnaire-1",
                    "assignment_id": "assign-from-questionnaire",
                    "user_id": "worker-99",
                }
            ],
            "tag_prompt_deployments": [
                {"id": "deployment-1", "assignment_id": "assign-from-deployment"}
            ],
        },
    )

    assert len(assignments) == 1
    assert assignments[0].assignment_id == "assign-from-questionnaire"
    assert assignments[0].tagger_id == "worker-99"


def test_db_adapter_uses_deployment_when_questionnaire_missing():
    importer = MagicMock()
    adapter = DBAdapter(
        MySQLConfig("host", "user", "password", "database"),
        importer=importer,
        tables=[
            "assignments",
            "answers",
            "questions",
            "assignment_questionnaires",
            "tag_prompt_deployments",
        ],
    )

    assignments, _ = adapter._build_assignments(
        [
            {
                "tagger_id": "worker-1",
                "comment_id": "comment-1",
                "characteristic_id": "deployment-1",
                "value": 1,
                "tagged_at": "2024-01-01T00:00:00Z",
                "assignment_id": "assign-from-row",
            }
        ],
        {
            "answers": [
                {"id": "comment-1", "question_id": "question-1"},
            ],
            "questions": [
                {"id": "question-1"}
            ],
            "assignment_questionnaires": [],
            "tag_prompt_deployments": [
                {"id": "deployment-1", "assignment_id": "assign-from-deployment"}
            ],
        },
    )

    assert len(assignments) == 1
    assert assignments[0].assignment_id == "assign-from-deployment"

