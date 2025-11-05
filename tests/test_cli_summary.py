"""Tests for CLI summary output helpers."""

import csv

from pathlib import Path

from qcc.cli.main import write_summary


def _read_csv_rows(csv_path: Path):
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


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

    csv_path = output_dir / "summary.csv"
    assert csv_path.exists()

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

    csv_path = output_dir / "summary.csv"
    assert csv_path.exists()

    rows = _read_csv_rows(csv_path)

    assert rows == []
