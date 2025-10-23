"""Tests for CLI summary output helpers."""

import csv

from pathlib import Path

from qcc.cli.main import write_summary


def test_write_summary_creates_csv(tmp_path):
    output_dir = Path(tmp_path)
    result = {
        "summary": {
            "total_assignments": 3,
            "assignments_by_value": {"YES": 2, "NO": 1},
            "answers_without_tags": ["101"],
            "average_tags_per_answer": 1.5,
        }
    }

    write_summary(result, output_dir)

    csv_path = output_dir / "summary.csv"
    assert csv_path.exists()

    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert {row["section"] for row in rows} >= {"summary", "assignments_by_value", "answers_without_tags"}

    summary_rows = {row["metric"]: row["value"] for row in rows if row["section"] == "summary"}
    assert summary_rows["total_assignments"] == "3"

    assert summary_rows["average_tags_per_answer"] == "1.5"

    assignments_by_value_rows = {
        row["metric"]: row["value"]
        for row in rows
        if row["section"] == "assignments_by_value"
    }
    assert assignments_by_value_rows == {"YES": "2", "NO": "1"}

    answers_without_tags = [
        row["value"] for row in rows if row["section"] == "answers_without_tags"
    ][0]
    assert answers_without_tags == "101"
