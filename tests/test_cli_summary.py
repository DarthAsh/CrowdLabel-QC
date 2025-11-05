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
                "taggers_with_speed": 2,
                "seconds_per_tag": {"mean": 7.5, "median": 7.5, "min": 5.0, "max": 10.0},
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
            }
        }
    }

    write_summary(result, output_dir)

    csv_path = output_dir / "summary.csv"
    assert csv_path.exists()

    rows = _read_csv_rows(csv_path)
    headers = rows[0].keys()
    assert set(headers) == {
        "user_id",
        "speed_strategy",
        "speed_taggers_with_speed",
        "speed_seconds_per_tag_mean",
        "speed_seconds_per_tag_median",
        "speed_seconds_per_tag_min",
        "speed_seconds_per_tag_max",
        "speed_mean_log2",
        "speed_seconds_per_tag",
        "speed_timestamped_assignments",
    }

    aggregate_row = next(row for row in rows if row["user_id"] == "aggregate")
    assert aggregate_row["speed_strategy"] == "LogTrimTaggingSpeed"
    assert aggregate_row["speed_taggers_with_speed"] == "2"
    assert aggregate_row["speed_seconds_per_tag_mean"] == "7.5"
    assert aggregate_row["speed_seconds_per_tag_median"] == "7.5"
    assert aggregate_row["speed_seconds_per_tag_min"] == "5"
    assert aggregate_row["speed_seconds_per_tag_max"] == "10"

    worker_row = next(row for row in rows if row["user_id"] == "worker-1")
    assert worker_row["speed_strategy"] == "LogTrimTaggingSpeed"
    assert worker_row["speed_mean_log2"] == "3"
    assert worker_row["speed_seconds_per_tag"] == "8"
    assert worker_row["speed_timestamped_assignments"] == "5"


def test_write_summary_handles_missing_speed_data(tmp_path):
    output_dir = Path(tmp_path)
    result = {"summary": {"tagger_speed": {"strategy": "LogTrimTaggingSpeed", "per_tagger": []}}}

    write_summary(result, output_dir)

    csv_path = output_dir / "summary.csv"
    assert csv_path.exists()

    rows = _read_csv_rows(csv_path)

    assert rows[0]["user_id"] == "aggregate"
    assert rows[0]["speed_strategy"] == "LogTrimTaggingSpeed"
