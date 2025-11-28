"""Tests for pattern report fixer integration within the CLI run."""

import importlib
import sys
import types
from pathlib import Path

from qcc.config.schema import InputConfig, LoggingConfig, QCCConfig


def test_run_analysis_invokes_pattern_report_fixer_with_defaults(tmp_path, monkeypatch):
    """Ensure the pattern report fixer runs with default connection kwargs for CSV input."""

    # Provide lightweight stubs for optional dependencies imported by the fixer helper.
    monkeypatch.setitem(sys.modules, "pandas", types.SimpleNamespace())
    mysql_module = types.SimpleNamespace()
    mysql_connector = types.SimpleNamespace(connect=lambda **_: None)
    mysql_module.connector = mysql_connector
    monkeypatch.setitem(sys.modules, "mysql", mysql_module)
    monkeypatch.setitem(sys.modules, "mysql.connector", mysql_connector)

    cli_main = importlib.import_module("qcc.cli.main")

    calls = {}

    def fake_fill(csv_path, **kwargs):
        calls["csv_path"] = csv_path
        calls["kwargs"] = kwargs

    monkeypatch.setattr(cli_main, "fill_team_ids_and_tags", fake_fill)

    output_dir = tmp_path / "output"
    input_file = Path("tests/data/min.csv")

    config = QCCConfig(
        input=InputConfig(format="csv", path=None),
        logging=LoggingConfig(file=None),
    )

    result = cli_main.run_analysis(input_file, output_dir, config)

    assert calls["csv_path"] == result["assignment_pattern_csv_path"]
    assert calls["kwargs"] == {}

    fix_status = result.get("pattern_report_fix", {})
    assert fix_status.get("attempted") is True
    assert fix_status.get("succeeded") is True
    assert fix_status.get("connection_kwargs") == {}
    assert fix_status.get("error") is None
