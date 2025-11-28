"""Tests for pattern report post-processing in the CLI."""

import sys
import types
from pathlib import Path


class _DummyCursor:
    def execute(self, *args, **kwargs):
        return None

    def fetchone(self):
        return None

    def close(self):
        return None


class _DummyConnection:
    def cursor(self):
        return _DummyCursor()

    def close(self):
        return None


def _install_dummy_dependencies():
    pandas_stub = types.SimpleNamespace(read_csv=lambda *args, **kwargs: None)
    mysql_connector_stub = types.SimpleNamespace(connect=lambda **kwargs: _DummyConnection())
    mysql_module = types.ModuleType("mysql")
    mysql_module.connector = mysql_connector_stub

    sys.modules.setdefault("pandas", pandas_stub)
    sys.modules.setdefault("mysql", mysql_module)
    sys.modules.setdefault("mysql.connector", mysql_connector_stub)


_install_dummy_dependencies()

from qcc.cli import main as cli_main
from qcc.config.schema import InputConfig, MySQLInputConfig, QCCConfig


def test_run_analysis_invokes_report_fixer(monkeypatch, tmp_path):
    calls = {}

    def fake_read_domain_objects(input_path, input_config):
        return {}, "mysql"

    class DummyTaggerReport:
        def __init__(self, assignments):
            self.assignments = assignments

        def generate_summary_report(self, *args, **kwargs):
            return {"summary": True}

        def export_to_csv(self, report_data, output_path):
            Path(output_path).write_text("summary")

    class DummyPatternReport:
        def __init__(self, assignments):
            self.assignments = assignments

        def generate_assignment_report(self, *args, **kwargs):
            return {"patterns": True}

        def export_to_csv(self, report_data, output_path):
            Path(output_path).write_text("patterns")

    def fake_fill(csv_path, **kwargs):
        calls["csv_path"] = Path(csv_path)
        calls["kwargs"] = kwargs

    monkeypatch.setattr(cli_main, "_read_domain_objects", fake_read_domain_objects)
    monkeypatch.setattr(cli_main, "TaggerPerformanceReport", DummyTaggerReport)
    monkeypatch.setattr(cli_main, "PatternDetectionReport", DummyPatternReport)
    monkeypatch.setattr(cli_main, "fill_team_ids_and_tags", fake_fill)

    config = QCCConfig(
        input=InputConfig(
            format="mysql",
            mysql=MySQLInputConfig(
                host="db.local",
                port=3307,
                user="app",
                password="secret",
                database="quality_control",
                charset="utf8mb4",
                use_pure=True,
            ),
        )
    )

    result = cli_main.run_analysis(None, tmp_path, config)

    assert calls["csv_path"].exists()
    assert calls["csv_path"].name.startswith("pattern-detections-")

    assert calls["kwargs"] == {
        "host": "db.local",
        "port": 3307,
        "user": "app",
        "password": "secret",
        "database": "quality_control",
        "charset": "utf8mb4",
        "use_pure": True,
    }

    assert result.get("pattern_report_fix_applied") is True
    assert result.get("mysql_config", {}).get("host") == "db.local"
