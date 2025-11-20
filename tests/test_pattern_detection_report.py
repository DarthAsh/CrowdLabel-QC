from datetime import datetime, timedelta
import csv
import logging

import pytest

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.reports.pattern_detection_report import PatternDetectionReport


def _build_uniform_yes_assignments(count: int = 12, assignment_id: str = "1205") -> list[TagAssignment]:
    start = datetime(2024, 1, 1, 0, 0, 0)
    assignments = []
    for i in range(count):
        assignments.append(
            TagAssignment(
                tagger_id="worker-1",
                comment_id=f"comment-{i}",
                characteristic_id="char-1",
                value=TagValue.YES,
                timestamp=start + timedelta(seconds=i),
                assignment_id=assignment_id,
            )
        )

    return assignments


def test_horizontal_assignments_capture_pattern_window():
    assignments = _build_uniform_yes_assignments()
    tagger = Tagger(id="worker-1", tagassignments=assignments)
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], [])
    horizontal = data["horizontal"]["assignments"]

    assert len(horizontal) == 1
    assert horizontal[0]["patterns"] == ["YYYY"]
    assert horizontal[0]["pattern_detected"] is True
    assert horizontal[0]["assignment_id"] == "1205"
    assert horizontal[0]["pattern_coverage"] == 100.0
    assert horizontal[0]["tag_count"] == 12
    assert horizontal[0]["pattern_tag_count"] == 12
    assert horizontal[0]["answer_count"] == 12
    assert horizontal[0]["speed_mean_log2"] == 0.0
    assert horizontal[0]["speed_seconds_per_tag"] == 1.0


def test_vertical_assignments_filtered_by_characteristic():
    assignments = _build_uniform_yes_assignments()
    tagger = Tagger(id="worker-1", tagassignments=assignments)
    characteristic = Characteristic(id="char-1", name="Characteristic One")
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], [characteristic])
    vertical = data["vertical"]["per_characteristic"]

    assert vertical == []


def test_csv_export_writes_all_assignment_rows(tmp_path):
    assignments = _build_uniform_yes_assignments()
    tagger = Tagger(id="worker-1", tagassignments=assignments)
    characteristic = Characteristic(id="char-1", name="Characteristic One")
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], [characteristic])
    csv_path = tmp_path / "patterns.csv"
    report.export_to_csv(data, csv_path)

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = list(csv.DictReader(csv_file))

    # Only the horizontal row is included
    assert len(reader) == 1
    assert all(row["patterns"] == "YYYY" for row in reader)
    assert all(row["pattern_detected"] == "true" for row in reader)
    assert set(row["perspective"] for row in reader) == {"horizontal"}
    assert set(reader[0].keys()) == {
        "user_id",
        "assignment_id",
        "comment_id",
        "prompt_id",
        "timestamp",
        "perspective",
        "tag_count",
        "pattern_tag_count",
        "answer_count",
        "patterns",
        "pattern_detected",
        "pattern_coverage",
        "speed_mean_log2",
        "speed_seconds_per_tag",
    }


def test_csv_export_deduplicates_vertical_rows(tmp_path):
    assignments: list[TagAssignment] = []
    start = datetime(2024, 1, 1, 0, 0, 0)
    for i in range(12):
        assignments.append(
            TagAssignment(
                tagger_id="worker-1",
                comment_id=f"comment-{i}",
                characteristic_id="char-1",
                value=TagValue.YES,
                timestamp=start + timedelta(seconds=i),
                assignment_id="1205",
            )
        )
        assignments.append(
            TagAssignment(
                tagger_id="worker-1",
                comment_id=f"comment-{i+12}",
                characteristic_id="char-2",
                value=TagValue.YES,
                timestamp=start + timedelta(seconds=i + 12),
                assignment_id="1205",
            )
        )

    tagger = Tagger(id="worker-1", tagassignments=assignments)
    characteristics = [
        Characteristic(id="char-1", name="C1"),
        Characteristic(id="char-2", name="C2"),
    ]
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], characteristics)
    csv_path = tmp_path / "deduped.csv"
    report.export_to_csv(data, csv_path)

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))

    # Only the horizontal row is exported, even though two characteristics were present
    assert len(rows) == 1
    perspectives = {row["perspective"] for row in rows}
    assert perspectives == {"horizontal"}


def test_pattern_coverage_partial_window():
    base_assignments = _build_uniform_yes_assignments(count=18)
    tagger = Tagger(id="worker-1", tagassignments=base_assignments)
    report = PatternDetectionReport(base_assignments)

    data = report.generate_assignment_report([tagger], [])
    coverage = data["horizontal"]["assignments"][0]["pattern_coverage"]
    pattern_tag_count = data["horizontal"]["assignments"][0]["pattern_tag_count"]
    tag_count = data["horizontal"]["assignments"][0]["tag_count"]

    assert coverage == 66.67
    assert pattern_tag_count == 12
    assert tag_count == 18


def test_csv_rows_sorted_by_user_id(tmp_path):
    assignments_a = _build_uniform_yes_assignments(assignment_id="1205")
    tagger_a = Tagger(id="user-1", tagassignments=assignments_a)

    assignments_b = _build_uniform_yes_assignments(assignment_id="1205")
    tagger_b = Tagger(id="user-2", tagassignments=assignments_b)

    report = PatternDetectionReport(assignments_a + assignments_b)

    data = report.generate_assignment_report([tagger_b, tagger_a], [])
    csv_path = tmp_path / "sorted.csv"
    report.export_to_csv(data, csv_path)

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))

    user_ids = [row["user_id"] for row in rows]
    assert user_ids == sorted(user_ids)


def test_only_target_assignment_rows_emitted():
    included = _build_uniform_yes_assignments(assignment_id="1205")
    excluded = _build_uniform_yes_assignments(assignment_id="9999")

    tagger = Tagger(id="user-1", tagassignments=included + excluded)
    report = PatternDetectionReport(included + excluded)

    data = report.generate_assignment_report([tagger], [])
    horizontal = data["horizontal"]["assignments"]

    assert len(horizontal) == 1
    assert horizontal[0]["assignment_id"] == "1205"


def test_ineligible_assignments_skipped_with_logging(caplog):
    allowed = TagAssignment(
        tagger_id="worker-1",
        comment_id="comment-eligible",
        characteristic_id="char-1",
        value=TagValue.YES,
        timestamp=datetime(2024, 1, 1, 0, 0, 0),
        assignment_id="1205",
    )
    rejected = TagAssignment(
        tagger_id="worker-2",
        comment_id="comment-ineligible",
        characteristic_id="char-1",
        value=TagValue.NA,
        timestamp=datetime(2024, 1, 1, 0, 0, 1),
        assignment_id="1205",
    )

    report = PatternDetectionReport([allowed, rejected])

    with caplog.at_level(logging.DEBUG):
        eligible = report._eligible_assignments([rejected, allowed])

    assert eligible == [allowed]
    assert any(
        "Skipping ineligible assignment" in record.message for record in caplog.records
    )
