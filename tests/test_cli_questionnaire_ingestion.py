"""Ensure CLI uses questionnaire-rooted ingestion for MySQL inputs."""

from qcc.cli import main
from qcc.config.schema import InputConfig, MySQLInputConfig


def test_read_domain_objects_prefers_questionnaire_root(monkeypatch):
    captured = {}

    class FakeAdapter:
        def __init__(self, mysql_config):
            captured["config"] = mysql_config

        def read_domain_objects_from_questionnaires(self, limit=None):
            captured["limit"] = limit
            return {"assignments": ["questionnaire-rooted"]}

    monkeypatch.setattr(main, "DBAdapter", FakeAdapter)

    input_config = InputConfig(
        format="mysql", mysql=MySQLInputConfig(dsn="mysql://user:pass@db/qcc")
    )

    domain_objects, source = main._read_domain_objects(None, input_config)

    assert domain_objects == {"assignments": ["questionnaire-rooted"]}
    assert source == "mysql://user:pass@db/qcc"
    assert captured["limit"] is None
    assert captured["config"].database == "qcc"
