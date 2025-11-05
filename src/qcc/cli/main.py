"""Command-line interface for QCC (Quality Control of Crowd labeling)."""

import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import yaml

from qcc.config.schema import QCCConfig, InputConfig
from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.io.csv_adapter import CSVAdapter
from qcc.io.db_adapter import DBAdapter
from qcc.metrics.speed_strategy import LogTrimTaggingSpeed
from qcc.metrics.pattern_strategy import HorizontalPatternDetection
from qcc.metrics.utils.pattern import PatternCollection


def main() -> int:
    """Main CLI entry point.
    
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    parser = create_argument_parser()
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        config = _apply_run_overrides(config, args)

        # Run the analysis
        result = run_analysis(
            input_path=args.input,
            output_dir=args.output,
            config=config
        )
        
        # Write summary
        write_summary(result, args.output)
        
        print(f"Analysis completed successfully. Results saved to {args.output}")
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
    
    # TODO: Implement actual analysis logic
    # For now, return a simple summary
    summary = _build_summary(domain_objects)

    result = {
        "input_source": input_source,
        "output_directory": str(output_dir),
        "config": config.dict(),
        "summary": summary,
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

    summary = result.get("summary", {})
    _write_summary_csv(summary, output_dir / "summary.csv")


def _build_summary(domain_objects: Dict[str, object]) -> Dict[str, object]:
    """Aggregate a summary report containing tagging speed and pattern metrics."""

    taggers = list(domain_objects.get("taggers", []) or [])

    speed_strategy = LogTrimTaggingSpeed()
    pattern_strategy = HorizontalPatternDetection()
    tracked_patterns = PatternCollection.return_all_patterns()

    per_tagger_speed: List[Dict[str, object]] = []
    seconds_samples = []
    per_tagger_patterns: List[Dict[str, object]] = []
    aggregate_pattern_counts: Dict[str, int] = defaultdict(int)
    taggers_with_patterns = 0

    for tagger in taggers:
        pattern_counts = pattern_strategy.analyze(tagger)
        positive_patterns = {
            pattern: count
            for pattern, count in (pattern_counts or {}).items()
            if count > 0
        }
        if positive_patterns:
            taggers_with_patterns += 1
            per_tagger_patterns.append(
                {
                    "tagger_id": str(tagger.id),
                    "patterns": dict(sorted(positive_patterns.items())),
                }
            )
            for pattern, count in positive_patterns.items():
                aggregate_pattern_counts[pattern] += count

        assignments_with_time = [
            assignment
            for assignment in (tagger.tagassignments or [])
            if getattr(assignment, "timestamp", None) is not None
        ]

        if len(assignments_with_time) < 2:
            continue

        mean_log2 = speed_strategy.speed_log2(tagger)
        if not math.isfinite(mean_log2):
            continue

        seconds_value = speed_strategy.seconds_per_tag(mean_log2)
        if not math.isfinite(seconds_value):
            continue

        per_tagger_speed.append(
            {
                "tagger_id": str(tagger.id),
                "mean_log2": mean_log2,
                "seconds_per_tag": seconds_value,
                "timestamped_assignments": len(assignments_with_time),
            }
        )
        if seconds_value > 0:
            seconds_samples.append(seconds_value)

    if seconds_samples:
        mean_seconds = mean(seconds_samples)
        median_seconds = median(seconds_samples)
        min_seconds = min(seconds_samples)
        max_seconds = max(seconds_samples)
    else:
        mean_seconds = 0.0
        median_seconds = 0.0
        min_seconds = 0.0
        max_seconds = 0.0

    summary_seconds = {
        "mean": mean_seconds,
        "median": median_seconds,
        "min": min_seconds,
        "max": max_seconds,
    }

    pattern_summary = {
        "strategy": "HorizontalPatternDetection",
        "patterns_tracked": tracked_patterns,
        "taggers_with_patterns": taggers_with_patterns,
        "aggregate_counts": dict(sorted(aggregate_pattern_counts.items())),
        "per_tagger": per_tagger_patterns,
    }

    return {
        "tagger_speed": {
            "strategy": "LogTrimTaggingSpeed",
            "taggers_with_speed": len(per_tagger_speed),
            "seconds_per_tag": summary_seconds,
            "per_tagger": per_tagger_speed,
        },
        "pattern_detection": pattern_summary,
    }


def _write_summary_csv(summary: Dict[str, object], csv_path: Path) -> None:
    """Write a CSV representation of the tagging speed summary."""

    rows: List[Dict[str, str]] = []
    tagger_speed = summary.get("tagger_speed", {}) if summary else {}

    if tagger_speed:
        taggers_with_speed = tagger_speed.get("taggers_with_speed", 0)
        rows.append(
            {
                "Strategy": "Tagger Speed",
                "user_id": "aggregate",
                "Metric": "taggers_with_speed",
                "Value": _stringify_csv_value(taggers_with_speed),
            }
        )

        seconds_section = tagger_speed.get("seconds_per_tag", {}) or {}
        for metric_name, metric_value in seconds_section.items():
            rows.append(
                {
                    "Strategy": "Tagger Speed",
                    "user_id": "aggregate",
                    "Metric": f"seconds_per_tag_{metric_name}",
                    "Value": _stringify_csv_value(metric_value),
                }
            )

        for tagger_entry in tagger_speed.get("per_tagger", []) or []:
            tagger_id = str(tagger_entry.get("tagger_id", ""))
            for metric_name in ("mean_log2", "seconds_per_tag", "timestamped_assignments"):
                if metric_name in tagger_entry:
                    rows.append(
                        {
                            "Strategy": "Tagger Speed",
                            "user_id": tagger_id,
                            "Metric": metric_name,
                            "Value": _stringify_csv_value(tagger_entry[metric_name]),
                        }
                    )

    pattern_summary = summary.get("pattern_detection", {}) if summary else {}
    if pattern_summary:
        rows.append(
            {
                "Strategy": "Pattern Detection",
                "user_id": "aggregate",
                "Metric": "taggers_with_patterns",
                "Value": _stringify_csv_value(
                    pattern_summary.get("taggers_with_patterns", 0)
                ),
            }
        )

        aggregate_counts = pattern_summary.get("aggregate_counts", {}) or {}
        for pattern, count in aggregate_counts.items():
            rows.append(
                {
                    "Strategy": "Pattern Detection",
                    "user_id": "aggregate",
                    "Metric": f"pattern_{pattern}",
                    "Value": _stringify_csv_value(count),
                }
            )

        for entry in pattern_summary.get("per_tagger", []) or []:
            tagger_id = str(entry.get("tagger_id", ""))
            for pattern, count in (entry.get("patterns") or {}).items():
                rows.append(
                    {
                        "Strategy": "Pattern Detection",
                        "user_id": tagger_id,
                        "Metric": f"pattern_{pattern}",
                        "Value": _stringify_csv_value(count),
                    }
                )

    if not rows:
        rows.append({"Strategy": "Tagger Speed", "user_id": "aggregate", "Metric": "", "Value": ""})

    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["Strategy", "user_id", "Metric", "Value"])
        writer.writeheader()
        writer.writerows(rows)


def _stringify_csv_value(value: object) -> str:
    """Convert a summary value into a string suitable for CSV output."""

    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".") if not value.is_integer() else str(int(value))
    return str(value)
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
        return adapter.read_domain_objects(), source or "mysql"

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

