"""Per-assignment pattern detection reporting."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.metrics.pattern_strategy import (
    HorizontalPatternDetection,
    VerticalPatternDetection,
)
from qcc.metrics.interfaces import PatternSignalsStrategy


class PatternDetectionReport:
    """Generate per-assignment pattern detection results."""

    def __init__(self, assignments: Sequence[TagAssignment]) -> None:
        self.assignments: List[TagAssignment] = list(assignments or [])

    def generate_assignment_report(
        self,
        taggers: Sequence[Tagger],
        characteristics: Sequence[Characteristic],
    ) -> Dict[str, object]:
        """Return pattern detection results for every assignment a user tagged."""

        horizontal_strategy = HorizontalPatternDetection()
        vertical_strategy = VerticalPatternDetection()

        horizontal_assignments = self._build_horizontal_results(
            taggers, horizontal_strategy
        )
        vertical_assignments = self._build_vertical_results(
            taggers, characteristics, vertical_strategy
        )

        return {
            "strategy": "PatternDetectionReport",
            "horizontal": {
                "strategy": horizontal_strategy.__class__.__name__,
                "assignments": horizontal_assignments,
            },
            "vertical": {
                "strategy": vertical_strategy.__class__.__name__,
                "per_characteristic": vertical_assignments,
            },
        }

    def export_to_csv(self, report_data: Mapping[str, object], output_path: Path) -> None:
        """Export the per-assignment pattern results to CSV."""

        csv_path = Path(output_path)
        rows = self._build_csv_rows(report_data)
        fieldnames = [
            "user_id",
            "assignment_id",
            "comment_id",
            "characteristic_id",
            "prompt_id",
            "team_id",
            "timestamp",
            "perspective",
            "patterns",
            "pattern_detected",
        ]

        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _build_horizontal_results(
        self, taggers: Sequence[Tagger], strategy: PatternSignalsStrategy
    ) -> List[Dict[str, object]]:
        per_assignment: List[Dict[str, object]] = []

        for tagger in taggers:
            grouped = self._group_assignments_by_id(tagger.tagassignments or [])
            for assignment_id, assignments in grouped.items():
                eligible_assignments = self._eligible_assignments(assignments)
                windows = self._pattern_windows(eligible_assignments, strategy)
                per_assignment.extend(
                    self._assignment_entries(
                        eligible_assignments, windows, assignment_id=assignment_id
                    )
                )

        return per_assignment

    def _build_vertical_results(
        self,
        taggers: Sequence[Tagger],
        characteristics: Sequence[Characteristic],
        strategy: PatternSignalsStrategy,
    ) -> List[Dict[str, object]]:
        per_characteristic: List[Dict[str, object]] = []

        for characteristic in characteristics:
            characteristic_id = getattr(characteristic, "id", None)
            if characteristic_id is None:
                continue

            characteristic_entries: List[Dict[str, object]] = []
            for tagger in taggers:
                assignments = [
                    assignment
                    for assignment in (tagger.tagassignments or [])
                    if getattr(assignment, "characteristic_id", None) == characteristic_id
                ]
                grouped = self._group_assignments_by_id(assignments)
                for assignment_id, characteristic_assignments in grouped.items():
                    eligible_assignments = self._eligible_assignments(
                        characteristic_assignments
                    )
                    if not eligible_assignments:
                        continue

                    windows = self._pattern_windows(eligible_assignments, strategy)
                    characteristic_entries.extend(
                        self._assignment_entries(
                            eligible_assignments,
                            windows,
                            assignment_id=assignment_id,
                        )
                    )

            if characteristic_entries:
                per_characteristic.append(
                    {
                        "characteristic_id": str(characteristic_id),
                        "assignments": characteristic_entries,
                    }
                )

        return per_characteristic

    def _pattern_windows(
        self,
        assignments: Sequence[TagAssignment],
        strategy: PatternSignalsStrategy,
        substring_length: int = 12,
    ) -> List[tuple[int, str]]:
        if not assignments:
            return []

        assignment_sequence = list(strategy.build_sequence_str(assignments))
        sub_start = 0
        track_4: List[tuple[int, str]] = []

        while sub_start < len(assignment_sequence) - (substring_length - 1):
            cur_sub = "".join(
                assignment_sequence[sub_start : sub_start + substring_length]
            )

            first_pattern = cur_sub[0:4]
            expected = first_pattern * (substring_length // len(first_pattern))
            if cur_sub == expected:
                track_4.append((sub_start, first_pattern))
                sub_start += substring_length
            else:
                sub_start += 1

        for start_pos, _ in track_4:
            assignment_sequence[start_pos : start_pos + substring_length] = "#"

        sub_start = 0
        track_3: List[tuple[int, str]] = []

        while sub_start < len(assignment_sequence) - (substring_length - 1):
            cur_sub = "".join(
                assignment_sequence[sub_start : sub_start + substring_length]
            )
            if "#" in cur_sub:
                sub_start += substring_length
                continue

            first_pattern = cur_sub[0:3]
            expected = first_pattern * (substring_length // len(first_pattern))
            if cur_sub == expected:
                track_3.append((sub_start, first_pattern))
                sub_start += substring_length
            else:
                sub_start += 1

        return [*track_4, *track_3]

    def _assignment_entries(
        self,
        assignments: Sequence[TagAssignment],
        windows: Sequence[tuple[int, str]],
        *,
        assignment_id: Optional[str],
    ) -> List[Dict[str, object]]:
        entries: List[Dict[str, object]] = []

        for assignment in assignments:
            entries.append(
                {
                    "tagger_id": str(assignment.tagger_id),
                    "assignment_id": assignment_id,
                    "comment_id": getattr(assignment, "comment_id", None),
                    "characteristic_id": getattr(assignment, "characteristic_id", None),
                    "prompt_id": getattr(assignment, "prompt_id", None),
                    "team_id": getattr(assignment, "team_id", None),
                    "timestamp": self._timestamp_str(getattr(assignment, "timestamp", None)),
                    "patterns": [],
                    "pattern_detected": False,
                }
            )

        for start_pos, pattern in windows:
            for offset in range(12):
                idx = start_pos + offset
                if idx >= len(entries):
                    break
                entries[idx]["patterns"].append(pattern)
                entries[idx]["pattern_detected"] = True

        for entry in entries:
            patterns = entry.get("patterns", []) or []
            if patterns:
                unique_patterns = sorted({str(pattern) for pattern in patterns})
                entry["patterns"] = unique_patterns
            else:
                entry["patterns"] = []

        return entries

    @staticmethod
    def _group_assignments_by_id(
        assignments: Iterable[TagAssignment],
    ) -> Dict[str, List[TagAssignment]]:
        grouped: Dict[str, List[TagAssignment]] = {}
        for assignment in assignments:
            assignment_id = getattr(assignment, "assignment_id", None)
            if assignment_id is None:
                continue

            grouped.setdefault(str(assignment_id), []).append(assignment)

        return grouped

    def _build_csv_rows(self, report_data: Mapping[str, object]) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []

        horizontal = report_data.get("horizontal") if isinstance(report_data, Mapping) else None
        if isinstance(horizontal, Mapping):
            assignments = horizontal.get("assignments", []) or []
            rows.extend(self._rows_from_assignments(assignments, perspective="horizontal"))

        vertical = report_data.get("vertical") if isinstance(report_data, Mapping) else None
        if isinstance(vertical, Mapping):
            per_characteristic = vertical.get("per_characteristic", []) or []
            for characteristic_entry in per_characteristic:
                if not isinstance(characteristic_entry, Mapping):
                    continue
                assignments = characteristic_entry.get("assignments", []) or []
                rows.extend(
                    self._rows_from_assignments(
                        assignments,
                        perspective="vertical",
                        characteristic_id=str(
                            characteristic_entry.get("characteristic_id", "")
                        ),
                    )
                )

        return rows

    def _rows_from_assignments(
        self,
        assignments: Iterable[Mapping[str, object]],
        *,
        perspective: str,
        characteristic_id: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        for assignment in assignments:
            if not isinstance(assignment, Mapping):
                continue
            patterns = assignment.get("patterns", []) or []
            pattern_str = ";".join(patterns) if patterns else ""
            row: MutableMapping[str, str] = {
                "user_id": str(assignment.get("tagger_id", "")),
                "assignment_id": str(assignment.get("assignment_id", "") or ""),
                "comment_id": str(assignment.get("comment_id", "") or ""),
                "characteristic_id": str(
                    characteristic_id
                    if characteristic_id is not None
                    else assignment.get("characteristic_id", "")
                ),
                "prompt_id": str(assignment.get("prompt_id", "") or ""),
                "team_id": str(assignment.get("team_id", "") or ""),
                "timestamp": str(assignment.get("timestamp", "") or ""),
                "perspective": perspective,
                "patterns": pattern_str,
                "pattern_detected": str(bool(patterns)).lower(),
            }

            rows.append(dict(row))

        return rows

    @staticmethod
    def _eligible_assignments(
        assignments: Iterable[TagAssignment],
    ) -> List[TagAssignment]:
        eligible: List[TagAssignment] = []
        for assignment in assignments:
            timestamp = getattr(assignment, "timestamp", None)
            value = getattr(assignment, "value", None)
            if timestamp is None or value not in (TagValue.YES, TagValue.NO):
                continue
            eligible.append(assignment)

        return sorted(eligible, key=lambda assignment: assignment.timestamp)

    @staticmethod
    def _timestamp_str(timestamp: Optional[datetime]) -> str:
        return timestamp.isoformat() if isinstance(timestamp, datetime) else ""

