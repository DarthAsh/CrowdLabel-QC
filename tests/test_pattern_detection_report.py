from datetime import datetime, timedelta
import csv

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.reports.pattern_detection_report import PatternDetectionReport


def _build_uniform_yes_assignments(count: int = 12, assignment_id: str = "assign-1") -> list[TagAssignment]:
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

    assert len(horizontal) == len(assignments)
    assert all("YYYY" in entry["patterns"] for entry in horizontal)
    assert all(entry["pattern_detected"] is True for entry in horizontal)
    assert set(entry["assignment_id"] for entry in horizontal) == {"assign-1"}


def test_vertical_assignments_filtered_by_characteristic():
    assignments = _build_uniform_yes_assignments()
    tagger = Tagger(id="worker-1", tagassignments=assignments)
    characteristic = Characteristic(id="char-1", name="Characteristic One")
    report = PatternDetectionReport(assignments)

    data = report.generate_assignment_report([tagger], [characteristic])
    vertical = data["vertical"]["per_characteristic"][0]

    assert vertical["characteristic_id"] == "char-1"
    assert len(vertical["assignments"]) == len(assignments)
    assert all("YYYY" in entry["patterns"] for entry in vertical["assignments"])
    assert all(entry["pattern_detected"] is True for entry in vertical["assignments"])


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

    # 12 horizontal rows + 12 vertical rows
    assert len(reader) == 24
    assert all(row["patterns"] == "YYYY" for row in reader)
    assert all(row["pattern_detected"] == "true" for row in reader)
    assert set(row["perspective"] for row in reader) == {"horizontal", "vertical"}
