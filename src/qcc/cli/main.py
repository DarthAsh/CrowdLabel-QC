"""Command-line interface for QCC (Quality Control of Crowd labeling)."""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import yaml

from qcc.config.schema import QCCConfig
from qcc.io.csv_adapter import CSVAdapter


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
    input_path: Path,
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
    csv_adapter = CSVAdapter()
    domain_objects = csv_adapter.read_domain_objects(input_path)
    
    # TODO: Implement actual analysis logic
    # For now, return a simple summary
    result = {
        "input_file": str(input_path),
        "output_directory": str(output_dir),
        "config": config.dict(),
        "summary": {
            "total_assignments": len(domain_objects.get("assignments", [])),
            "total_comments": len(domain_objects.get("comments", [])),
            "total_taggers": len(domain_objects.get("taggers", [])),
            "total_prompts": len(domain_objects.get("prompts", [])),
            "total_characteristics": len(domain_objects.get("characteristics", []))
        }
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


if __name__ == "__main__":
    sys.exit(main())

