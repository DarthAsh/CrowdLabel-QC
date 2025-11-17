"""Smoke tests for CLI functionality."""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest


class TestCLISmoke:
    """Test CLI basic functionality."""
    
    def test_cli_help(self):
        """Test that CLI shows help without errors."""
        result = subprocess.run(
            [sys.executable, "-m", "qcc.cli.main", "--help"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
        assert "Quality Control of Crowd labeling" in result.stdout
    
    def test_cli_run_help(self):
        """Test that CLI run command shows help without errors."""
        result = subprocess.run(
            [sys.executable, "-m", "qcc.cli.main", "run", "--help"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
        assert "--in" in result.stdout
        assert "--out" in result.stdout
        assert "--config" in result.stdout
        assert "--mysql-dsn" in result.stdout
    
    def test_cli_run_missing_args(self):
        """Test that CLI run fails gracefully with missing required args."""
        result = subprocess.run(
            [sys.executable, "-m", "qcc.cli.main", "run"],
            capture_output=True,
            text=True
        )
        assert result.returncode != 0
        assert "required" in result.stderr.lower() or "error" in result.stderr.lower()
    
    def test_cli_run_with_nonexistent_input(self):
        """Test that CLI run fails gracefully with nonexistent input file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "output"
            result = subprocess.run(
                [
                    sys.executable, "-m", "qcc.cli.main", "run",
                    "--format", "csv",
                    "--in", "nonexistent.csv",
                    "--out", str(output_dir)
                ],
                capture_output=True,
                text=True
            )
            assert result.returncode != 0
            assert "error" in result.stderr.lower() or "not found" in result.stderr.lower()
    
    def test_cli_run_with_minimal_csv(self):
        """Test CLI run with minimal CSV data."""
        # Create minimal test CSV
        with tempfile.TemporaryDirectory() as temp_dir:
            input_file = Path(temp_dir) / "min.csv"
            output_dir = Path(temp_dir) / "output"
            
            # Create minimal CSV with required columns
            csv_content = """assignment_id,team_id,tagger_id,comment_id,prompt_id,characteristic,value,tagged_at,comment_text,prompt_text
1,team1,tagger1,comment1,prompt1,sentiment,YES,2024-01-01T00:00:00,This is a test comment,Please label sentiment
"""
            input_file.write_text(csv_content)
            
            # Run CLI
            result = subprocess.run(
                [
                    sys.executable, "-m", "qcc.cli.main", "run",
                    "--format", "csv",
                    "--in", str(input_file),
                    "--out", str(output_dir)
                ],
                capture_output=True,
                text=True
            )

            # CLI should complete successfully and produce the reports
            assert result.returncode == 0
            assert (output_dir / "summary.json").exists()
            assert list(output_dir.glob("tagging-report-*.csv"))
            assert (output_dir / "qcc.log").exists()
    
    def test_cli_config_loading(self):
        """Test that CLI can load default configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "output"
            config_file = Path("src/qcc/config/default.yml")
            
            if config_file.exists():
                result = subprocess.run(
                    [
                        sys.executable, "-m", "qcc.cli.main", "run",
                        "--format", "csv",
                        "--in", "nonexistent.csv",
                        "--out", str(output_dir),
                        "--config", str(config_file)
                    ],
                    capture_output=True,
                    text=True
                )
                # Should fail on input file, not config loading
                assert result.returncode != 0
                assert "not found" in result.stderr.lower() or "error" in result.stderr.lower()
    
    def test_cli_creates_output_directory(self):
        """Test that CLI creates output directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_file = Path(temp_dir) / "min.csv"
            output_dir = Path(temp_dir) / "new_output_dir"

            # Create minimal CSV
            csv_content = """assignment_id,team_id,tagger_id,comment_id,prompt_id,characteristic,value,tagged_at,comment_text,prompt_text
1,team1,tagger1,comment1,prompt1,sentiment,YES,2024-01-01T00:00:00,Test comment,Test prompt
"""
            input_file.write_text(csv_content)

            # Ensure output directory doesn't exist
            assert not output_dir.exists()

            # Run CLI and ensure it succeeds while creating the output directory
            result = subprocess.run(
                [
                    sys.executable, "-m", "qcc.cli.main", "run",
                    "--format", "csv",
                    "--in", str(input_file),
                    "--out", str(output_dir)
                ],
                capture_output=True,
                text=True
            )

            # Output directory should be created and the command should succeed
            assert result.returncode == 0
            assert (output_dir / "summary.json").exists()
            assert list(output_dir.glob("tagging-report-*.csv"))
            assert (output_dir / "qcc.log").exists()

