"""Command-line interface for QCC (Quality Control of Crowd labeling)."""

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import yaml

from qcc.config.schema import QCCConfig, InputConfig
from qcc.data_ingestion.mysql_config import MySQLConfig
from qcc.io.csv_adapter import CSVAdapter
from qcc.io.db_adapter import DBAdapter


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
        required=True,
        type=Path,
        help="Path to input CSV file"
    )
    run_parser.add_argument(
        "--out",
        dest="output",
        required=True,
        type=Path,
        help="Output directory for reports"
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


def _build_summary(domain_objects: Dict[str, object]) -> Dict[str, object]:
    """Aggregate a summary report from the loaded domain objects."""

    assignments = list(domain_objects.get("assignments", []) or [])
    comments = list(domain_objects.get("comments", []) or [])
    taggers = list(domain_objects.get("taggers", []) or [])
    characteristics = list(domain_objects.get("characteristics", []) or [])
    answers = list(domain_objects.get("answers", []) or [])
    prompt_deployments = list(domain_objects.get("prompt_deployments", []) or [])
    prompts = list(domain_objects.get("prompts", []) or [])
    questions = list(domain_objects.get("questions", []) or [])

    total_assignments = len(assignments)
    total_comments = len(comments)
    total_answers = len(answers) if answers else total_comments
    total_taggers = len(taggers)
    total_characteristics = len(characteristics)
    total_prompt_deployments = len(prompt_deployments)
    total_prompts = len(prompts)
    total_questions = len(questions)

    tag_value_counts = Counter(str(assignment.value) for assignment in assignments)
    characteristic_counts = Counter(assignment.characteristic_id for assignment in assignments)
    assignments_per_answer = {comment.id: len(comment.tagassignments) for comment in comments}
    tagger_activity = {tagger.id: len(tagger.tagassignments) for tagger in taggers}
    characteristic_labels = {characteristic.id: characteristic.name for characteristic in characteristics}
    prompt_control_types = Counter(
        str(prompt.get("control_type"))
        for prompt in prompts
        if prompt.get("control_type") not in (None, "")
    )

    unanswered_comments = [comment_id for comment_id, count in assignments_per_answer.items() if count == 0]
    question_ids = {answer.get("question_id") for answer in answers if answer.get("question_id")}
    response_ids = {answer.get("response_id") for answer in answers if answer.get("response_id")}

    average_tags_per_answer = (
        float(total_assignments) / float(total_answers)
        if total_answers
        else 0.0
    )

    table_row_counts = {
        "answer_tags": total_assignments,
        "answers": len(answers),
    }
    if prompts:
        table_row_counts["tag_prompts"] = len(prompts)
    if prompt_deployments:
        table_row_counts["tag_prompt_deployments"] = len(prompt_deployments)
    if questions:
        table_row_counts["questions"] = len(questions)

    return {
        "total_assignments": total_assignments,
        "total_answers": total_answers,
        "total_comments": total_comments,
        "total_taggers": total_taggers,
        "total_characteristics": total_characteristics,
        "total_prompt_deployments": total_prompt_deployments,
        "total_prompts": total_prompts,
        "total_questions": total_questions,
        "unique_questions": len(question_ids),
        "unique_responses": len(response_ids),
        "assignments_by_value": dict(tag_value_counts),
        "assignments_by_characteristic": dict(characteristic_counts),
        "assignments_per_answer": assignments_per_answer,
        "tagger_activity": tagger_activity,
        "answers_without_tags": unanswered_comments,
        "average_tags_per_answer": average_tags_per_answer,
        "table_row_counts": table_row_counts,
        "characteristic_labels": characteristic_labels,
        "prompt_control_types": dict(prompt_control_types),
    }
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

