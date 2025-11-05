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
        "Strategy",
        "user_id",
        "Metric",
        "Value",
        "pattern_detected",
        "pattern_value",
    }

    aggregate_rows = [
        row for row in rows if row["user_id"] == "aggregate" and row["Metric"].startswith("seconds_per_tag_")
    ]
    assert {row["Metric"] for row in aggregate_rows} == {
        "seconds_per_tag_mean",
        "seconds_per_tag_median",
        "seconds_per_tag_min",
        "seconds_per_tag_max",
    }

    per_tagger_rows = [row for row in rows if row["user_id"] == "worker-1"]
    per_tagger_metrics = {row["Metric"]: row["Value"] for row in per_tagger_rows}
    assert per_tagger_metrics == {
        "mean_log2": "3",
        "seconds_per_tag": "8",
        "timestamped_assignments": "5",
    }

    assert all(row["pattern_detected"] == "" for row in rows)
    assert all(row["pattern_value"] == "" for row in rows)


def test_write_summary_handles_missing_speed_data(tmp_path):
    output_dir = Path(tmp_path)
    result = {"summary": {"tagger_speed": {"strategy": "LogTrimTaggingSpeed", "per_tagger": []}}}

    write_summary(result, output_dir)

    csv_path = output_dir / "summary.csv"
    assert csv_path.exists()

    rows = _read_csv_rows(csv_path)

    assert rows[0]["Strategy"] == "Tagger Speed"
    assert rows[0]["user_id"] == "aggregate"
    assert rows[0]["pattern_detected"] == ""
    assert rows[0]["pattern_value"] == ""
