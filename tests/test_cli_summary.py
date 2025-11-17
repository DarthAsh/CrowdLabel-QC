"""Tests for CLI summary output helpers."""

import csv
import logging

from pathlib import Path

from qcc.cli.main import setup_logging, write_summary
from qcc.config.schema import LoggingConfig


def _read_csv_rows(csv_path: Path):
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def _find_summary_csv(output_dir: Path) -> Path:
    csv_paths = sorted(output_dir.glob("tagging-report-*.csv"))
    assert len(csv_paths) == 1
    return csv_paths[0]


def test_write_summary_creates_csv(tmp_path):
    output_dir = Path(tmp_path)
    result = {
        "summary": {
            "tagger_speed": {
                "strategy": "LogTrimTaggingSpeed",
                "per_tagger": [
                    {
                        "tagger_id": "worker-1",
                        "mean_log2": 3.0,
                        "seconds_per_tag": 8.0,
                        "timestamped_assignments": 5,
                    },
                    {
                        "tagger_id": "worker-2",
                        "mean_log2": 2.0,
                        "seconds_per_tag": 4.0,
                        "timestamped_assignments": 4,
                    },
                ],
            },
            "pattern_detection": {
                "strategy": "HorizontalPatternDetection",
                "per_tagger": [
                    {
                        "tagger_id": "worker-1",
                        "patterns": {"YN": 3, "YY": 1},
                    },
                    {
                        "tagger_id": "worker-2",
                        "patterns": {"NN": 2},
                    },
                ],
            },
        }
    }

    write_summary(result, output_dir)

    csv_path = _find_summary_csv(output_dir)

    rows = _read_csv_rows(csv_path)
    assert {row["user_id"] for row in rows} == {"worker-1", "worker-2"}

    headers = rows[0].keys()
    assert set(headers) == {
        "user_id",
        "speed_strategy",
        "speed_mean_log2",
        "speed_seconds_per_tag",
        "speed_timestamped_assignments",
        "pattern_strategy",
        "pattern_count_YY",
        "pattern_count_YN",
        "pattern_count_NN",
    }

    worker_row = next(row for row in rows if row["user_id"] == "worker-1")
    assert worker_row["speed_strategy"] == "LogTrimTaggingSpeed"
    assert worker_row["speed_mean_log2"] == "3"
    assert worker_row["speed_seconds_per_tag"] == "8"
    assert worker_row["speed_timestamped_assignments"] == "5"
    assert worker_row["pattern_strategy"] == "HorizontalPatternDetection"
    assert worker_row["pattern_count_YN"] == "3"
    assert worker_row["pattern_count_YY"] == "1"
    assert worker_row["pattern_count_NN"] == ""


def test_write_summary_handles_missing_speed_data(tmp_path):
    output_dir = Path(tmp_path)
    result = {"summary": {"tagger_speed": {"strategy": "LogTrimTaggingSpeed", "per_tagger": []}}}

    write_summary(result, output_dir)

    csv_path = _find_summary_csv(output_dir)

    rows = _read_csv_rows(csv_path)

    assert rows == []


def test_setup_logging_writes_to_configured_file(tmp_path):
    output_dir = Path(tmp_path)
    log_config = LoggingConfig(level="INFO", file="custom.log")

    log_path = setup_logging(log_config, output_dir)

    logging.getLogger("qcc.tests").info("sample message")

    for handler in logging.getLogger().handlers:
        flush = getattr(handler, "flush", None)
        if callable(flush):
            flush()

    assert log_path == output_dir / "custom.log"
    assert log_path.exists()

    contents = log_path.read_text(encoding="utf-8")
    assert "sample message" in contents
