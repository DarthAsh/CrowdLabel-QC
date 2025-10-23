"""Tests for applying CLI overrides to the runtime configuration."""

from pathlib import Path

from qcc.cli.main import _apply_run_overrides, create_argument_parser
from qcc.config.schema import QCCConfig


def _parse_args(args):
    parser = create_argument_parser()
    return parser.parse_args(args)


def test_apply_run_overrides_sets_mysql_dsn_and_format(tmp_path):
    """Providing MySQL overrides should update the config without mutating the original."""

    config = QCCConfig()
    args = _parse_args(
        [
            "run",
            "--out",
            str(tmp_path / "out"),
            "--format",
            "mysql",
            "--mysql-dsn",
            "mysql://user:pass@db.example.com:3307/labels",
        ]
    )

    updated = _apply_run_overrides(config, args)

    assert updated is not config
    assert updated.input.format == "mysql"
    assert updated.input.mysql.dsn == "mysql://user:pass@db.example.com:3307/labels"
    # Original configuration should remain unchanged
    assert config.input.format == "csv"
    assert config.input.mysql.dsn is None


def test_apply_run_overrides_sets_mysql_connection_details(tmp_path):
    """Individual MySQL parameters should be overridden when provided via CLI."""

    config = QCCConfig()
    args = _parse_args(
        [
            "run",
            "--out",
            str(tmp_path / "out"),
            "--format",
            "mysql",
            "--mysql-host",
            "db.local",
            "--mysql-port",
            "3310",
            "--mysql-user",
            "svc",
            "--mysql-password",
            "secret",
            "--mysql-database",
            "analytics",
            "--mysql-charset",
            "utf8mb4",
            "--mysql-env-prefix",
            "QCC_MYSQL",
            "--no-mysql-use-pure",
        ]
    )

    updated = _apply_run_overrides(config, args)
    mysql_settings = updated.input.mysql

    assert mysql_settings.host == "db.local"
    assert mysql_settings.port == 3310
    assert mysql_settings.user == "svc"
    assert mysql_settings.password == "secret"
    assert mysql_settings.database == "analytics"
    assert mysql_settings.charset == "utf8mb4"
    assert mysql_settings.env_prefix == "QCC_MYSQL"
    assert mysql_settings.use_pure is False


def test_apply_run_overrides_sets_csv_path(tmp_path):
    """CSV runs should pick up the CLI-provided file path."""

    input_path = tmp_path / "assignments.csv"
    input_path.write_text("assignment_id,tagger_id,comment_id,value\n")

    config = QCCConfig()
    args = _parse_args(
        [
            "run",
            "--out",
            str(tmp_path / "out"),
            "--format",
            "csv",
            "--in",
            str(input_path),
        ]
    )

    updated = _apply_run_overrides(config, args)

    assert updated.input.format == "csv"
    assert updated.input.path == str(input_path)
    # The original config should still have no path set
    assert config.input.path is None
