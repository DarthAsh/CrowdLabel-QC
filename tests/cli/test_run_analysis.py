from pathlib import Path

import pytest

from qcc.cli.main import run_analysis
from qcc.config.schema import QCCConfig


def _make_db_config(dsn: str) -> QCCConfig:
    return QCCConfig.model_validate({
        "input": {"format": "db", "path": dsn},
        "output": {"directory": "reports", "format": "json"},
    })


def test_run_analysis_raises_value_error_when_mysql_connector_missing(monkeypatch, tmp_path):
    config = _make_db_config("mysql://user:pass@localhost/database")

    def _raise_missing_connector(_config):
        raise ModuleNotFoundError("mysql connector missing")

    monkeypatch.setattr(
        "qcc.cli.main.import_tag_prompt_deployment_tables",
        _raise_missing_connector,
    )

    with pytest.raises(ValueError) as excinfo:
        run_analysis(
            input_path=Path("input.csv"),
            output_dir=tmp_path,
            config=config,
        )

    assert "mysql-connector-python" in str(excinfo.value)
