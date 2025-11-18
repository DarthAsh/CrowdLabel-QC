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


def test_db_adapter_uses_deployment_assignment_id():
    importer = MagicMock()
    adapter = DBAdapter(
        MySQLConfig("host", "user", "password", "database"),
        importer=importer,
        tables=["assignments", "tag_prompt_deployments"],
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
            "tag_prompt_deployments": [
                {
                    "id": "deployment-1",
                    "assignment_id": "assign-from-deployment",
                }
            ]
        },
    )

    assert len(assignments) == 1
    assert assignments[0].assignment_id == "assign-from-deployment"


def test_db_adapter_prefers_assignment_questionnaires_over_deployment():
    importer = MagicMock()
    adapter = DBAdapter(
        MySQLConfig("host", "user", "password", "database"),
        importer=importer,
        tables=["assignments", "assignment_questionnaires", "tag_prompt_deployments"],
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
            "assignment_questionnaires": [
                {"assignment_id": "assign-from-questionnaire", "user_id": "worker-1"}
            ],
            "tag_prompt_deployments": [
                {
                    "id": "deployment-1",
                    "assignment_id": "assign-from-deployment",
                }
            ],
        },
    )

    assert len(assignments) == 1
    assert assignments[0].assignment_id == "assign-from-questionnaire"

