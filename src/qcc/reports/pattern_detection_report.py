"""Per-assignment pattern detection reporting."""

from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.metrics.speed_strategy import LogTrimTaggingSpeed
from qcc.metrics.pattern_strategy import (
    HorizontalPatternDetection,
    VerticalPatternDetection,
)
from qcc.metrics.interfaces import PatternSignalsStrategy

logger = logging.getLogger(__name__)


class PatternDetectionReport:
    """Generate per-assignment pattern detection results."""

    TARGET_ASSIGNMENT_ID = "1205"

    def __init__(self, assignments: Sequence[TagAssignment]) -> None:
        self.assignments: List[TagAssignment] = list(assignments or [])

    def generate_assignment_report(
        self,
        taggers: Sequence[Tagger],
        characteristics: Sequence[Characteristic],
    ) -> Dict[str, object]:
        """Return pattern detection results for every assignment a user tagged."""

        logger.info(
            "Starting pattern detection report generation for %s taggers and %s characteristics",
            len(taggers),
            len(characteristics),
        )

        horizontal_strategy = HorizontalPatternDetection()

        horizontal_assignments = self._build_horizontal_results(
            taggers, horizontal_strategy
        )
        vertical_assignments: List[Dict[str, object]] = []

        logger.info(
            "Finished pattern detection aggregation: %s horizontal rows, %s vertical characteristic groups",
            len(horizontal_assignments),
            len(vertical_assignments),
        )

        return {
            "strategy": "PatternDetectionReport",
            "horizontal": {
                "strategy": horizontal_strategy.__class__.__name__,
                "assignments": horizontal_assignments,
            },
            "vertical": {
                "strategy": VerticalPatternDetection.__name__,
                "per_characteristic": vertical_assignments,
            },
        }

    def export_to_csv(self, report_data: Mapping[str, object], output_path: Path) -> None:
        """Export the per-assignment pattern results to CSV."""

        csv_path = Path(output_path)
        rows = self._build_csv_rows(report_data)
        fieldnames = [
            "tagger_id",
            "assignment_id",
            "first_comment_id",
            "first_prompt_id",
            "first_tag_timestamp",
            "eligible_tag_count",
            "tags_in_pattern_count",
            "distinct_answer_count",
            "detected_patterns",
            "has_repeating_pattern",
            "pattern_coverage_pct",
            "trimmed_seconds_per_tag",
        ]

        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        logger.info(
            "Pattern detection CSV written to %s with %s rows", csv_path, len(rows)
        )

    def _build_horizontal_results(
        self, taggers: Sequence[Tagger], strategy: PatternSignalsStrategy
    ) -> List[Dict[str, object]]:
        per_assignment: List[Dict[str, object]] = []

        for tagger in sorted(taggers, key=lambda t: str(getattr(t, "id", ""))):
            logger.debug(
                "Processing horizontal assignments for user %s (%s assignments)",
                getattr(tagger, "id", ""),
                len(tagger.tagassignments or []),
            )
            grouped = self._group_assignments_by_id(tagger.tagassignments or [])
            for assignment_id, assignments in grouped.items():
                eligible_assignments = self._eligible_assignments(assignments)
                windows = self._pattern_windows(eligible_assignments, strategy)
                per_assignment.extend(
                    self._assignment_entries(
                        assignments,
                        eligible_assignments,
                        windows,
                        assignment_id=assignment_id,
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
            for tagger in sorted(taggers, key=lambda t: str(getattr(t, "id", ""))):
                assignments = [
                    assignment
                    for assignment in (tagger.tagassignments or [])
                    if getattr(assignment, "characteristic_id", None) == characteristic_id
                ]
                if assignments:
                    logger.debug(
                        "Processing vertical assignments for user %s characteristic %s (%s assignments)",
                        getattr(tagger, "id", ""),
                        characteristic_id,
                        len(assignments),
                    )
                grouped = self._group_assignments_by_id(assignments)
                for assignment_id, characteristic_assignments in grouped.items():
                    eligible_assignments = self._eligible_assignments(
                        characteristic_assignments
                    )
                    windows = self._pattern_windows(eligible_assignments, strategy)
                    characteristic_entries.extend(
                        self._assignment_entries(
                            assignments=characteristic_assignments,
                            eligible_assignments=eligible_assignments,
                            windows=windows,
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
        eligible_assignments: Sequence[TagAssignment],
        windows: Sequence[tuple[int, str]],
        *,
        assignment_id: Optional[str],
    ) -> List[Dict[str, object]]:
        if not assignments:
            return []

        first = assignments[0]
        timestamp = getattr(first, "timestamp", None)
        patterns = sorted({pattern for _, pattern in windows})
        coverage, pattern_tag_count = self._pattern_coverage_stats(
            eligible_assignments, windows
        )
        _, seconds_per_tag = self._speed_metrics(eligible_assignments)
        tag_count = len(eligible_assignments)
        answer_count = len(
            {
                getattr(assignment, "comment_id", None)
                for assignment in assignments
                if getattr(assignment, "comment_id", None) is not None
            }
        )

        return [
            {
                "tagger_id": str(first.tagger_id),
                "assignment_id": assignment_id,
                "first_comment_id": getattr(first, "comment_id", None),
                "first_prompt_id": getattr(first, "prompt_id", None),
                "first_tag_timestamp": self._timestamp_str(timestamp),
                "eligible_tag_count": tag_count,
                "tags_in_pattern_count": pattern_tag_count,
                "distinct_answer_count": answer_count,
                "detected_patterns": patterns,
                "has_repeating_pattern": bool(patterns),
                "pattern_coverage_pct": coverage,
                "trimmed_seconds_per_tag": seconds_per_tag,
            }
        ]

    def _group_assignments_by_id(
        self, assignments: Iterable[TagAssignment]
    ) -> Dict[str, List[TagAssignment]]:
        grouped: Dict[str, List[TagAssignment]] = {}
        for assignment in assignments:
            assignment_id = getattr(assignment, "assignment_id", None)
            if assignment_id is None:
                logger.warning(
                    "Skipping assignment without assignment_id: %s",
                    self._assignment_context(assignment),
                )
                continue

            if str(assignment_id) != self.TARGET_ASSIGNMENT_ID:
                logger.debug(
                    "Ignoring assignment outside target %s: %s",
                    self.TARGET_ASSIGNMENT_ID,
                    self._assignment_context(assignment, assignment_id),
                )
                continue

            grouped.setdefault(str(assignment_id), []).append(assignment)

        return grouped

    def _build_csv_rows(self, report_data: Mapping[str, object]) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        seen_keys: set[tuple[str, str]] = set()

        horizontal = report_data.get("horizontal") if isinstance(report_data, Mapping) else None
        if isinstance(horizontal, Mapping):
            assignments = horizontal.get("assignments", []) or []
            for row in self._rows_from_assignments(assignments):
                key = (row.get("tagger_id", ""), row.get("assignment_id", ""))
                if key in seen_keys:
                    logger.debug(
                        "Skipping duplicate horizontal row for %s", key
                    )
                    continue
                seen_keys.add(key)
                rows.append(row)

        return sorted(rows, key=lambda row: row.get("tagger_id", ""))

    def _rows_from_assignments(
        self,
        assignments: Iterable[Mapping[str, object]],
    ) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        for assignment in assignments:
            if not isinstance(assignment, Mapping):
                logger.warning(
                    "Skipping non-mapping assignment entry: %s", assignment
                )
                continue
            patterns = assignment.get("detected_patterns", []) or []
            pattern_str = ";".join(patterns) if patterns else ""
            row: MutableMapping[str, str] = {
                "tagger_id": str(assignment.get("tagger_id", "")),
                "assignment_id": str(assignment.get("assignment_id", "") or ""),
                "first_comment_id": str(
                    assignment.get("first_comment_id", "") or ""
                ),
                "first_prompt_id": str(
                    assignment.get("first_prompt_id", "") or ""
                ),
                "first_tag_timestamp": str(
                    assignment.get("first_tag_timestamp", "") or ""
                ),
                "eligible_tag_count": str(
                    assignment.get("eligible_tag_count", "") or ""
                ),
                "tags_in_pattern_count": str(
                    assignment.get("tags_in_pattern_count", "") or ""
                ),
                "distinct_answer_count": str(
                    assignment.get("distinct_answer_count", "") or ""
                ),
                "detected_patterns": pattern_str,
                "has_repeating_pattern": str(bool(patterns)).lower(),
                "pattern_coverage_pct": str(
                    assignment.get("pattern_coverage_pct", "") or ""
                ),
                "trimmed_seconds_per_tag": str(
                    assignment.get("trimmed_seconds_per_tag", "") or ""
                ),
            }

            if not row["tagger_id"] or not row["assignment_id"]:
                logger.warning(
                    "Skipping row missing required identifiers tagger_id/assignment_id: %s",
                    row,
                )
                continue

            rows.append(dict(row))

        return rows

    @classmethod
    def _eligible_assignments(
        cls, assignments: Iterable[TagAssignment],
    ) -> List[TagAssignment]:
        eligible: List[TagAssignment] = []
        for assignment in assignments:
            timestamp = getattr(assignment, "timestamp", None)
            value = getattr(assignment, "value", None)
            if timestamp is None or value not in (TagValue.YES, TagValue.NO):
                logger.debug(
                    "Skipping ineligible assignment for pattern detection: %s",
                    cls._assignment_context(assignment),
                )
                continue
            eligible.append(assignment)

        return sorted(eligible, key=lambda assignment: assignment.timestamp)

    @staticmethod
    def _timestamp_str(timestamp: Optional[datetime]) -> str:
        return timestamp.isoformat() if isinstance(timestamp, datetime) else ""

    @staticmethod
    def _pattern_coverage_stats(
        assignments: Sequence[TagAssignment],
        windows: Sequence[tuple[int, str]],
        substring_length: int = 12,
    ) -> tuple[float, int]:
        assignment_count = len(assignments)
        if assignment_count == 0 or not windows:
            return 0.0, 0

        covered_positions = set()
        for start, _ in windows:
            covered_positions.update(range(start, start + substring_length))

        pattern_tag_count = min(len(covered_positions), assignment_count)
        coverage_ratio = pattern_tag_count / assignment_count
        return round(coverage_ratio * 100, 2), pattern_tag_count

    @staticmethod
    def _speed_metrics(assignments: Sequence[TagAssignment]) -> tuple[float, float]:
        if not assignments:
            return 0.0, 0.0

        strategy = LogTrimTaggingSpeed()
        tagger = Tagger(id="assignment-speed", tagassignments=list(assignments))
        mean_log2 = strategy.speed_log2(tagger)
        seconds = strategy.seconds_per_tag(mean_log2)
        return round(mean_log2, 6), round(seconds, 6)

    @staticmethod
    def _assignment_context(
        assignment: TagAssignment, assignment_id: Optional[str] = None
    ) -> Dict[str, object]:
        return {
            "user_id": getattr(assignment, "tagger_id", None),
            "assignment_id": assignment_id
            if assignment_id is not None
            else getattr(assignment, "assignment_id", None),
            "comment_id": getattr(assignment, "comment_id", None),
            "characteristic_id": getattr(assignment, "characteristic_id", None),
        }

