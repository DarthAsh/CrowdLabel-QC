from datetime import datetime, timezone

from qcc.data_ingestion import TagPromptDeploymentDataset, DEFAULT_TAG_PROMPT_TABLES
from qcc.domain.enums import TagValue


def _make_rows():
    answers_table = DEFAULT_TAG_PROMPT_TABLES[0]
    rows = [
        {
            "tagger_id": "worker-1",
            "comment_id": "comment-1",
            "characteristic_id": "char-a",
            "value": "YES",
            "created_at": "2024-01-01T00:00:00Z",
        },
        {
            "user_id": "worker-1",
            "item_id": "comment-2",
            "tag_prompt_deployment_characteristic_id": "char-a",
            "response": "no",
            "updated_at": datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
        },
        {
            # Missing timestamp -> ignored
            "tagger_id": "worker-2",
            "comment_id": "comment-3",
            "characteristic_id": "char-b",
            "value": "YES",
        },
    ]
    return answers_table, rows


def test_dataset_groups_assignments_by_tagger():
    table_name, rows = _make_rows()
    dataset = TagPromptDeploymentDataset.from_mysql_tables({table_name: rows})

    assert len(dataset.assignments) == 2
    assert {assignment.comment_id for assignment in dataset.assignments} == {"comment-1", "comment-2"}

    assert len(dataset.taggers) == 1
    tagger = dataset.taggers[0]
    assert tagger.id == "worker-1"
    assert len(tagger.tagassignments) == 2
    assert tagger.tagassignments[0].value is TagValue.YES
    assert tagger.tagassignments[1].value is TagValue.NO


def test_as_domain_dict_structure():
    table_name, rows = _make_rows()
    dataset = TagPromptDeploymentDataset.from_mysql_tables({table_name: rows})

    domain = dataset.as_domain_dict()
    assert set(domain.keys()) == {"assignments", "taggers", "comments", "prompts", "characteristics"}
    assert domain["assignments"] == dataset.assignments
    assert domain["taggers"] == dataset.taggers
