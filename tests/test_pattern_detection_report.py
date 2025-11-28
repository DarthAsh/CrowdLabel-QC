from datetime import datetime, timedelta
import csv
import logging

import pytest

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.reports.pattern_detection_report import PatternDetectionReport


def _build_uniform_yes_assignments(
    count: int = 12, assignment_id: str = "1205", questionnaire_id: str | None = None
) -> list[TagAssignment]:
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
                questionnaire_id=questionnaire_id,
            )
        )

    return assignments


def test_horizontal_assignments_capture_pattern_window():
    assignments = _build_uniform_yes_assignments(questionnaire_id="753")
    tagger = Tagger(id="worker-1", tagassignments=assignments)
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], [])
    horizontal = data["horizontal"]["assignments"]

    assert len(horizontal) == 1
    assert horizontal[0]["detected_patterns"] == ["YYYY"]
    assert horizontal[0]["has_repeating_pattern"] is True
    assert horizontal[0]["assignment_id"] == "1205"
    assert horizontal[0]["pattern_coverage_pct"] == 100.0
    assert horizontal[0]["# Tags Available"] == 24
    assert horizontal[0]["# Tags Set"] == 12
    assert horizontal[0]["# Tags Set in a pattern"] == 12
    assert horizontal[0]["# Comments available to tag"] == 12
    assert horizontal[0]["trimmed_seconds_per_tag"] == 1.0


def test_vertical_assignments_filtered_by_characteristic():
    assignments = _build_uniform_yes_assignments()
    tagger = Tagger(id="worker-1", tagassignments=assignments)
    characteristic = Characteristic(id="char-1", name="Characteristic One")
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], [characteristic])
    vertical = data["vertical"]["per_characteristic"]

    assert vertical == []


def test_csv_export_writes_all_assignment_rows(tmp_path):
    assignments = _build_uniform_yes_assignments(questionnaire_id="753")
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
    assert all(row["detected_patterns"] == "YYYY" for row in reader)
    assert all(row["has_repeating_pattern"] == "true" for row in reader)
    assert set(reader[0].keys()) == {
        "tagger_id",
        "team_id",
        "assignment_id",
        "# Tags Available",
        "# Tags Set",
        "# Tags Set in a pattern",
        "# Comments available to tag",
        "detected_patterns",
        "has_repeating_pattern",
        "pattern_coverage_pct",
        "trimmed_seconds_per_tag",
    }
    assert "pattern_id" not in reader[0]


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
                questionnaire_id="753",
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
                questionnaire_id="753",
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


def test_csv_export_defaults_tag_availability(tmp_path):
    assignments = _build_uniform_yes_assignments(questionnaire_id="999")
    tagger = Tagger(id="worker-0", tagassignments=assignments)
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], [])
    csv_path = tmp_path / "zero-availability.csv"
    report.export_to_csv(data, csv_path)

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert len(rows) == 1
    assert rows[0]["# Tags Available"] == "12"


def test_pattern_coverage_partial_window():
    base_assignments = _build_uniform_yes_assignments(count=18, questionnaire_id="753")
    tagger = Tagger(id="worker-1", tagassignments=base_assignments)
    report = PatternDetectionReport(base_assignments)

    data = report.generate_assignment_report([tagger], [])
    coverage = data["horizontal"]["assignments"][0]["pattern_coverage_pct"]
    pattern_tag_count = data["horizontal"]["assignments"][0]["# Tags Set in a pattern"]
    tag_count = data["horizontal"]["assignments"][0]["# Tags Set"]

    assert coverage == 66.67
    assert pattern_tag_count == 12
    assert tag_count == 18


def test_available_tags_include_skips():
    start = datetime(2024, 1, 1, 0, 0, 0)
    assignments = [
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-yes",
            characteristic_id="char-1",
            value=TagValue.YES,
            timestamp=start,
            assignment_id="1205",
            questionnaire_id="754",
        ),
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-skip",
            characteristic_id="char-1",
            value=TagValue.SKIP,
            timestamp=start + timedelta(seconds=1),
            assignment_id="1205",
            questionnaire_id="754",
        ),
    ]

    tagger = Tagger(id="worker-1", tagassignments=assignments)
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], [])
    horizontal = data["horizontal"]["assignments"][0]

    assert horizontal["# Tags Available"] == 2
    # Only the YES/NO tag is eligible for pattern detection
    assert horizontal["# Tags Set"] == 1


def test_tags_available_uses_questionnaire_capacity():
    start = datetime(2024, 1, 1, 0, 0, 0)
    assignments = [
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-753-1",
            characteristic_id="char-1",
            value=TagValue.YES,
            timestamp=start,
            assignment_id="1205",
            questionnaire_id="753",
            question_id="question-753-a",
        ),
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-753-2",
            characteristic_id="char-1",
            value=TagValue.SKIP,
            timestamp=start + timedelta(seconds=1),
            assignment_id="1205",
            questionnaire_id="753",
            question_id="question-753-b",
        ),
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-754",
            characteristic_id="char-1",
            value=TagValue.NO,
            timestamp=start + timedelta(seconds=2),
            assignment_id="1205",
            questionnaire_id="754",
            question_id="question-754",
        ),
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-other",
            characteristic_id="char-1",
            value=TagValue.SKIP,
            timestamp=start + timedelta(seconds=3),
            assignment_id="1205",
            questionnaire_id="9999",
            question_id="question-other",
        ),
    ]

    report = PatternDetectionReport(assignments)
    data = report.generate_assignment_report([Tagger(id="worker-1", tagassignments=assignments)], [])
    horizontal = data["horizontal"]["assignments"][0]

    # questionnaire_id 753 allows 2 tags per answer, 754 allows 1, and unknown
    # questionnaires default to the minimal capacity
    assert horizontal["# Tags Available"] == 6
    assert horizontal["# Tags Set"] == 2
    assert horizontal["# Comments available to tag"] == 4


def test_tags_available_counts_unique_comments():
    start = datetime(2024, 1, 1, 0, 0, 0)
    assignments = [
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-shared",
            characteristic_id="char-1",
            value=TagValue.YES,
            timestamp=start,
            assignment_id="1205",
            questionnaire_id="753",
            question_id="question-753",
        ),
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-shared",
            characteristic_id="char-2",
            value=TagValue.NO,
            timestamp=start + timedelta(seconds=1),
            assignment_id="1205",
            questionnaire_id="753",
            question_id="question-753",
        ),
    ]

    report = PatternDetectionReport(assignments)
    data = report.generate_assignment_report(
        [Tagger(id="worker-1", tagassignments=assignments)], []
    )
    horizontal = data["horizontal"]["assignments"][0]

    # Only one comment exists, so availability is counted once despite two tags
    assert horizontal["# Tags Available"] == 2
    assert horizontal["# Tags Set"] == 2
    assert horizontal["# Comments available to tag"] == 1


def test_tags_available_defaults_capacity_for_missing_questionnaire(caplog):
    start = datetime(2024, 1, 1, 0, 0, 0)
    assignments = [
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-without-questionnaire",
            characteristic_id="char-1",
            value=TagValue.YES,
            timestamp=start,
            assignment_id="1205",
            questionnaire_id=None,
            question_id="question-without-questionnaire",
        )
    ]

    report = PatternDetectionReport(assignments)
    with caplog.at_level(logging.WARNING):
        data = report.generate_assignment_report(
            [Tagger(id="worker-1", tagassignments=assignments)], []
        )

    horizontal = data["horizontal"]["assignments"][0]

    assert horizontal["# Tags Available"] == 1
    assert horizontal["# Comments available to tag"] == 1
    assert any("defaulting" in record.message for record in caplog.records)


def test_tags_available_backfills_questionnaire_from_question_lookup():
    start = datetime(2024, 1, 1, 0, 0, 0)
    assignments = [
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-with-questionnaire",
            characteristic_id="char-1",
            value=TagValue.YES,
            timestamp=start,
            assignment_id="1205",
            questionnaire_id="753",
            question_id="question-shared",
        ),
        TagAssignment(
            tagger_id="worker-1",
            comment_id="comment-without-questionnaire",
            characteristic_id="char-1",
            value=TagValue.NO,
            timestamp=start + timedelta(seconds=1),
            assignment_id="1205",
            questionnaire_id=None,
            question_id="question-shared",
        ),
    ]

    report = PatternDetectionReport(assignments)
    data = report.generate_assignment_report(
        [Tagger(id="worker-1", tagassignments=assignments)], []
    )
    horizontal = data["horizontal"]["assignments"][0]

    assert horizontal["# Tags Available"] == 4
    assert horizontal["# Tags Set"] == 2
    assert horizontal["# Comments available to tag"] == 2


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

    user_ids = [row["tagger_id"] for row in rows]
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
