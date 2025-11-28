"""Command-line interface for QCC (Quality Control of Crowd labeling)."""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Mapping, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import yaml

from qcc.config.schema import InputConfig, LoggingConfig, QCCConfig
from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.io.csv_adapter import CSVAdapter
from qcc.io.db_adapter import DBAdapter
from qcc.reports.tagger_performance import TaggerPerformanceReport
from qcc.reports.pattern_detection_report import PatternDetectionReport
from report_fixer import fill_team_ids_and_tags


def main() -> int:
    """Main CLI entry point.
    
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    parser = create_argument_parser()
    args = parser.parse_args()

    if getattr(args, "command", None) != "run":
        parser.print_help()
        return 1

    try:
        # Load configuration
        config = load_config(args.config)
        config = _apply_run_overrides(config, args)

        log_path = setup_logging(config.logging, args.output)

        # Run the analysis
        result = run_analysis(
            input_path=args.input,
            output_dir=args.output,
            config=config
        )

        result.setdefault("metadata", {})
        if isinstance(result["metadata"], dict):
            result["metadata"]["log_file"] = str(log_path)
        else:  # pragma: no cover - defensive fallback
            result["log_file"] = str(log_path)

        # Write summary
        pattern_csv = write_summary(result, args.output)
        print("Running team id and tags available fix for pattern_csv")
        try:
            fill_team_ids_and_tags(str(pattern_csv))
            print("Fix applied successfully.")
        except Exception as e:
            print(f"Fix failed: {e}")
        print(
            "Analysis completed successfully. Results saved to"
            f" {args.output} (logs: {log_path})"
        )
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def create_argument_parser() -> argparse.ArgumentParser:
    """Create the command-line argument parser.
    
    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        prog="qcc",
        description="Quality Control of Crowd labeling - Analyze and report on crowd labeling quality metrics"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run quality control analysis")
    run_parser.add_argument(
        "--in",
        dest="input",
        required=False,
        default=None,
        type=Path,
        help="Path to input CSV file (optional when using MySQL input)"
    )
    run_parser.add_argument(
        "--out",
        dest="output",
        required=True,
        type=Path,
        help="Output directory for reports"
    )
    run_parser.add_argument(
        "--format",
        dest="input_format",
        choices=["csv", "mysql"],
        help="Override the configured input format"
    )
    run_parser.add_argument(
        "--mysql-dsn",
        dest="mysql_dsn",
        help="MySQL DSN (e.g., mysql://user:pass@host:3306/dbname)"
    )
    run_parser.add_argument(
        "--mysql-host",
        dest="mysql_host",
        help="MySQL server hostname"
    )
    run_parser.add_argument(
        "--mysql-port",
        dest="mysql_port",
        type=int,
        help="MySQL server port"
    )
    run_parser.add_argument(
        "--mysql-user",
        dest="mysql_user",
        help="MySQL user name"
    )
    run_parser.add_argument(
        "--mysql-password",
        dest="mysql_password",
        help="MySQL user password"
    )
    run_parser.add_argument(
        "--mysql-database",
        dest="mysql_database",
        help="MySQL database name"
    )
    run_parser.add_argument(
        "--mysql-charset",
        dest="mysql_charset",
        help="Character set for the MySQL connection"
    )
    run_parser.add_argument(
        "--mysql-env-prefix",
        dest="mysql_env_prefix",
        help="Environment variable prefix for missing MySQL settings"
    )
    run_parser.add_argument(
        "--mysql-use-pure",
        dest="mysql_use_pure",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Force mysql.connector to use the pure Python implementation"
            " (use --no-mysql-use-pure to disable)"
        )
    )
    run_parser.add_argument(
        "--config",
        type=Path,
        default=Path("src/qcc/config/default.yml"),
        help="Path to configuration file (default: src/qcc/config/default.yml)"
    )
    
    return parser


def load_config(config_path: Path) -> QCCConfig:
    """Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Loaded QCCConfig instance
        
    Raises:
        FileNotFoundError: If the config file doesn't exist
        yaml.YAMLError: If the config file is invalid YAML
        ValueError: If the config data is invalid
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)

    return QCCConfig(**config_data)


def _apply_run_overrides(config: QCCConfig, args: argparse.Namespace) -> QCCConfig:
    """Apply CLI overrides to the loaded configuration for the run command."""

    if getattr(args, "command", None) != "run":
        return config

    try:  # Prefer Pydantic v2 API when available
        updated = config.model_copy(deep=True)  # type: ignore[attr-defined]
    except AttributeError:  # pragma: no cover - fallback for Pydantic v1
        updated = config.copy(deep=True)

    if getattr(args, "input_format", None):
        updated.input.format = args.input_format

    input_format = updated.input.format.strip().lower()

    if input_format == "csv" and args.input:
        updated.input.path = str(args.input)

    if input_format == "mysql":
        mysql_settings = updated.input.mysql
        if getattr(args, "mysql_env_prefix", None):
            mysql_settings.env_prefix = args.mysql_env_prefix
        if getattr(args, "mysql_dsn", None):
            mysql_settings.dsn = args.mysql_dsn
        if getattr(args, "mysql_host", None):
            mysql_settings.host = args.mysql_host
        if getattr(args, "mysql_port", None) is not None:
            mysql_settings.port = int(args.mysql_port)
        if getattr(args, "mysql_user", None):
            mysql_settings.user = args.mysql_user
        if getattr(args, "mysql_password", None):
            mysql_settings.password = args.mysql_password
        if getattr(args, "mysql_database", None):
            mysql_settings.database = args.mysql_database
        if getattr(args, "mysql_charset", None):
            mysql_settings.charset = args.mysql_charset
        if getattr(args, "mysql_use_pure", None) is not None:
            mysql_settings.use_pure = bool(args.mysql_use_pure)

    return updated


def setup_logging(logging_config: LoggingConfig, output_dir: Path) -> Path:
    """Configure logging so messages are persisted to a file."""

    output_dir.mkdir(parents=True, exist_ok=True)

    log_file = logging_config.file
    if log_file:
        configured_path = Path(log_file)
        log_path = (
            configured_path
            if configured_path.is_absolute()
            else output_dir / configured_path
        )
    else:
        log_path = output_dir / "qcc.log"

    log_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        level_value = getattr(logging, logging_config.level.upper())
        if not isinstance(level_value, int):  # pragma: no cover - defensive
            raise AttributeError
    except AttributeError:  # pragma: no cover - invalid level fallback
        level_value = logging.INFO

    handlers = [
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(),
    ]

    logging.basicConfig(
        level=level_value,
        format=logging_config.format,
        handlers=handlers,
        force=True,
    )

    return log_path


def run_analysis(
    input_path: Optional[Path],
    output_dir: Path,
    config: QCCConfig
) -> dict:
    """Run the quality control analysis.
    
    Args:
        input_path: Path to the input CSV file
        output_dir: Directory to write output reports
        config: Configuration settings
        
    Returns:
        Dictionary containing analysis results
    """
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read input data
    domain_objects, input_source = _read_domain_objects(input_path, config.input)
    
    assignments = list(domain_objects.get("assignments", []) or [])
    taggers = list(domain_objects.get("taggers", []) or [])
    characteristics = list(domain_objects.get("characteristics", []) or [])

    report = TaggerPerformanceReport(assignments)
    summary = report.generate_summary_report(
        taggers,
        characteristics,
        include_agreement=True,
    )

    csv_path = _timestamped_tagging_report_csv_path(output_dir)
    report.export_to_csv(summary, csv_path)

    pattern_report = PatternDetectionReport(assignments)
    assignment_patterns = pattern_report.generate_assignment_report(
        taggers, characteristics
    )
    pattern_csv_path = _timestamped_pattern_report_csv_path(output_dir)
    pattern_report.export_to_csv(assignment_patterns, pattern_csv_path)

    fixer_connection_kwargs = _pattern_report_fixer_connection_kwargs(config.input)
    pattern_report_fix = {
        "attempted": True,
        "succeeded": True,
        "error": None,
        "connection_kwargs": fixer_connection_kwargs,
    }

    try:
        fill_team_ids_and_tags(str(pattern_csv_path), **fixer_connection_kwargs)
    except Exception as exc:  # pragma: no cover - exercised in integration
        logging.exception("Failed to apply pattern report fixer")
        pattern_report_fix["succeeded"] = False
        pattern_report_fix["error"] = str(exc)

    result = {
        "input_source": input_source,
        "output_directory": str(output_dir),
        "config": config.dict(),
        "summary": summary,
        "tagging_report_csv_path": str(csv_path),
        "assignment_pattern_report": assignment_patterns,
        "assignment_pattern_csv_path": str(pattern_csv_path),
        "pattern_report_fix": pattern_report_fix,
    }

    return result


def write_summary(result: dict, output_dir: Path) -> None:
    """Write analysis summary to output directory.
    
    Args:
        result: Analysis results dictionary
        output_dir: Directory to write the summary file
    """
    summary_path = output_dir / "summary.json"

    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, default=str)

    summary_data = result.get("summary") if isinstance(result, dict) else None
    if isinstance(summary_data, Mapping):
        report = TaggerPerformanceReport([])
        csv_path = _resolve_tagging_report_csv_path(result, output_dir)
        report.export_to_csv(summary_data, csv_path)

    pattern_data = result.get("assignment_pattern_report") if isinstance(result, dict) else None
    if isinstance(pattern_data, Mapping):
        pattern_report = PatternDetectionReport([])
        csv_path = _resolve_pattern_report_csv_path(result, output_dir)
        pattern_report.export_to_csv(pattern_data, csv_path)
    return csv_path


def _resolve_tagging_report_csv_path(result: Mapping[str, object], output_dir: Path) -> Path:
    csv_path = None
    if isinstance(result, Mapping):
        csv_path = result.get("tagging_report_csv_path") or result.get("summary_csv_path")
    if isinstance(csv_path, str) and csv_path:
        return Path(csv_path)

    return _timestamped_tagging_report_csv_path(output_dir)


def _resolve_pattern_report_csv_path(result: Mapping[str, object], output_dir: Path) -> Path:
    csv_path = None
    if isinstance(result, Mapping):
        csv_path = result.get("assignment_pattern_csv_path")
    if isinstance(csv_path, str) and csv_path:
        return Path(csv_path)

    return _timestamped_pattern_report_csv_path(output_dir)


def _timestamped_tagging_report_csv_path(output_dir: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return output_dir / f"tagging-report-{timestamp}.csv"


def _timestamped_pattern_report_csv_path(output_dir: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return output_dir / f"pattern-detections-{timestamp}.csv"


def _pattern_report_fixer_connection_kwargs(input_config: InputConfig) -> Dict[str, object]:
    input_format = input_config.format.strip().lower()
    if input_format != "mysql":
        return {}

    mysql_config = _build_mysql_config(input_config)
    connection_kwargs: Dict[str, object] = {
        "host": mysql_config.host,
        "port": mysql_config.port,
        "user": mysql_config.user,
        "password": mysql_config.password,
        "database": mysql_config.database,
        "use_pure": mysql_config.use_pure,
    }

    if mysql_config.charset:
        connection_kwargs["charset"] = mysql_config.charset

    return connection_kwargs

def _read_domain_objects(
    input_path: Optional[Path], input_config: InputConfig
) -> Tuple[dict, str]:
    """Load domain objects based on the configured input format."""

    input_format = input_config.format.strip().lower()
    if input_format == "csv":
        csv_path: Optional[Path]
        if input_config.path:
            csv_path = Path(input_config.path)
        else:
            csv_path = input_path
        if csv_path is None:
            raise ValueError("CSV input path must be provided via CLI or config")
        adapter = CSVAdapter()
        return adapter.read_domain_objects(csv_path), str(csv_path)

    if input_format == "mysql":
        mysql_config = _build_mysql_config(input_config)
        adapter = DBAdapter(mysql_config)
        source = input_config.mysql.dsn or mysql_config.host
        return adapter.read_domain_objects_from_questionnaires(), source or "mysql"

    raise ValueError(f"Unsupported input format: {input_config.format}")


def _build_mysql_config(input_config: InputConfig) -> MySQLConfig:
    """Construct a MySQLConfig using config values and environment variables."""

    settings = input_config.mysql
    prefix = settings.env_prefix or "MYSQL"

    config_values: Dict[str, Optional[str]] = {}
    fields_set = getattr(settings, "model_fields_set", None)
    if fields_set is None:  # pragma: no cover - backwards compatibility
        fields_set = getattr(settings, "__fields_set__", set())

    if settings.dsn:
        parsed = urlparse(settings.dsn)
        if parsed.scheme and not parsed.scheme.startswith("mysql"):
            raise ValueError("Only mysql DSNs are supported")
        if parsed.hostname:
            config_values["host"] = parsed.hostname
        if parsed.username:
            config_values["user"] = parsed.username
        if parsed.password:
            config_values["password"] = parsed.password
        if parsed.path and parsed.path != "/":
            config_values["database"] = parsed.path.lstrip("/")
        if parsed.port:
            config_values["port"] = str(parsed.port)
        query = parse_qs(parsed.query)
        if "charset" in query and query["charset"]:
            config_values["charset"] = query["charset"][0]

    def _set_if_provided(field_name: str, key: str, value: Optional[str]) -> None:
        if field_name in fields_set and value not in (None, ""):
            config_values[key] = value

    _set_if_provided("host", "host", settings.host)
    _set_if_provided(
        "port",
        "port",
        str(settings.port) if settings.port is not None else None,
    )
    _set_if_provided("user", "user", settings.user)
    _set_if_provided("password", "password", settings.password)
    _set_if_provided("database", "database", settings.database)
    _set_if_provided("charset", "charset", settings.charset)
    if "use_pure" in fields_set:
        config_values["use_pure"] = str(settings.use_pure)

    env_map = {
        "host": os.getenv(f"{prefix}_HOST"),
        "port": os.getenv(f"{prefix}_PORT"),
        "user": os.getenv(f"{prefix}_USER"),
        "password": os.getenv(f"{prefix}_PASSWORD"),
        "database": os.getenv(f"{prefix}_DATABASE"),
        "charset": os.getenv(f"{prefix}_CHARSET"),
        "use_pure": os.getenv(f"{prefix}_USE_PURE"),
    }

    for key, value in env_map.items():
        if key not in config_values or config_values[key] in (None, ""):
            if value not in (None, ""):
                config_values[key] = value

    required = {name: config_values.get(name) for name in ("host", "user", "password", "database")}
    missing = [name for name, value in required.items() if value in (None, "")]
    if missing:
        raise ValueError(
            "Missing MySQL configuration values: " + ", ".join(missing)
        )

    port_value = int(config_values.get("port") or 3306)
    charset_value = config_values.get("charset")
    use_pure_raw = config_values.get("use_pure")
    if isinstance(use_pure_raw, str):
        use_pure = use_pure_raw.strip().lower() in {"1", "true", "yes"}
    else:
        use_pure = bool(use_pure_raw)

    return MySQLConfig(
        host=str(config_values["host"]),
        user=str(config_values["user"]),
        password=str(config_values["password"]),
        database=str(config_values["database"]),
        port=port_value,
        charset=charset_value,
        use_pure=use_pure,
    )


if __name__ == "__main__":
    sys.exit(main())

