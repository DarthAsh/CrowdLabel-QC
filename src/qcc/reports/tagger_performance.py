"""Tagger performance reporting for crowd labeling quality control."""

from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Tuple

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.metrics.pattern_strategy import HorizontalPatternDetection
from qcc.metrics.speed_strategy import LogTrimTaggingSpeed
from qcc.metrics.utils.pattern import PatternCollection


class TaggerPerformanceReport:
    """Generate performance reports for taggers."""

    def __init__(self, assignments: Sequence[TagAssignment]) -> None:
        """Initialize the report with tag assignments."""

        self.assignments: List[TagAssignment] = list(assignments or [])

    def generate_summary_report(
        self,
        taggers: Sequence[Tagger],
        characteristics: Sequence[Characteristic],
        *,
        include_speed: bool = True,
        include_patterns: bool = True,
        include_agreement: bool = False,
    ) -> Dict[str, object]:
        """Generate a summary performance report for taggers.

        Currently speed and pattern metrics are supported. Agreement metrics are
        not yet implemented and attempting to include them raises an error so we
        do not silently omit requested data.
        """

        if include_agreement:
            raise NotImplementedError("Agreement metrics have not been implemented")

        summary: Dict[str, object] = {}

        if include_speed:
            summary["tagger_speed"] = self._generate_speed_summary(taggers)

        if include_patterns:
            summary["pattern_detection"] = self._generate_pattern_summary(taggers)

        return summary

    def export_to_csv(self, report_data: Mapping[str, object], output_path: Path) -> None:
        """Export summary report data to CSV format."""

        csv_path = Path(output_path)
        rows, fieldnames = self._build_csv_rows(report_data)

        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in fieldnames})

    def _generate_speed_summary(self, taggers: Sequence[Tagger]) -> Dict[str, object]:
        speed_strategy = LogTrimTaggingSpeed()
        per_tagger_speed: List[Dict[str, object]] = []

        for tagger in taggers:
            assignments_with_time = [
                assignment
                for assignment in (tagger.tagassignments or [])
                if getattr(assignment, "timestamp", None) is not None
            ]

            if len(assignments_with_time) < 2:
                continue

            mean_log2 = speed_strategy.speed_log2(tagger)
            if not math.isfinite(mean_log2):
                continue

            seconds_value = speed_strategy.seconds_per_tag(mean_log2)
            if not math.isfinite(seconds_value):
                continue

            per_tagger_speed.append(
                {
                    "tagger_id": str(tagger.id),
                    "mean_log2": mean_log2,
                    "seconds_per_tag": seconds_value,
                    "timestamped_assignments": len(assignments_with_time),
                }
            )

        return {
            "strategy": "LogTrimTaggingSpeed",
            "per_tagger": per_tagger_speed,
        }

    def _generate_pattern_summary(self, taggers: Sequence[Tagger]) -> Dict[str, object]:
        pattern_strategy = HorizontalPatternDetection()
        tracked_patterns = PatternCollection.return_all_patterns()
        per_tagger_patterns: List[Dict[str, object]] = []

        for tagger in taggers:
            pattern_counts = pattern_strategy.analyze(tagger)
            positive_patterns = {
                pattern: count
                for pattern, count in (pattern_counts or {}).items()
                if count > 1 and len(pattern) > 1
            }

            if not positive_patterns:
                continue

            per_tagger_patterns.append(
                {
                    "tagger_id": str(tagger.id),
                    "patterns": dict(sorted(positive_patterns.items())),
                }
            )

        return {
            "strategy": "HorizontalPatternDetection",
            "patterns_tracked": tracked_patterns,
            "per_tagger": per_tagger_patterns,
        }

    def _build_csv_rows(
        self, summary: Mapping[str, object]
    ) -> Tuple[List[Dict[str, str]], List[str]]:
        rows: Dict[str, Dict[str, str]] = {}
        all_columns: set[str] = {"user_id"}

        def _row_for(user_id: str) -> Dict[str, str]:
            row = rows.setdefault(user_id, {"user_id": user_id})
            return row

        if not summary:
            return [], ["user_id"]

        tagger_speed = summary.get("tagger_speed", {}) if summary else {}
        if isinstance(tagger_speed, Mapping) and tagger_speed:
            strategy = tagger_speed.get("strategy")
            per_tagger = tagger_speed.get("per_tagger", []) or []
            for tagger_entry in per_tagger:
                if not isinstance(tagger_entry, Mapping):
                    continue
                tagger_id = str(tagger_entry.get("tagger_id", "")).strip()
                if not tagger_id:
                    continue
                tagger_row = _row_for(tagger_id)
                if strategy:
                    tagger_row["speed_strategy"] = str(strategy)
                    all_columns.add("speed_strategy")
                for metric_name in (
                    "mean_log2",
                    "seconds_per_tag",
                    "timestamped_assignments",
                ):
                    if metric_name in tagger_entry:
                        column = f"speed_{metric_name}"
                        tagger_row[column] = self._stringify_csv_value(
                            tagger_entry[metric_name]
                        )
                        all_columns.add(column)

        pattern_summary = summary.get("pattern_detection", {}) if summary else {}
        if isinstance(pattern_summary, Mapping) and pattern_summary:
            strategy = pattern_summary.get("strategy")
            per_tagger = pattern_summary.get("per_tagger", []) or []
            for entry in per_tagger:
                if not isinstance(entry, Mapping):
                    continue
                tagger_id = str(entry.get("tagger_id", "")).strip()
                if not tagger_id:
                    continue
                tagger_row = _row_for(tagger_id)
                if strategy:
                    tagger_row["pattern_strategy"] = str(strategy)
                    all_columns.add("pattern_strategy")
                patterns = entry.get("patterns") or {}
                if isinstance(patterns, Mapping):
                    for pattern, count in patterns.items():
                        column = f"pattern_count_{pattern}"
                        tagger_row[column] = self._stringify_csv_value(count)
                        all_columns.add(column)

        ordered_rows: List[Dict[str, str]] = []
        for user_id in sorted(rows):
            ordered_rows.append(rows[user_id])

        fieldnames: List[str] = ["user_id"]

        def _add_field(name: str) -> None:
            if name in all_columns and name not in fieldnames:
                fieldnames.append(name)

        for name in (
            "speed_strategy",
            "speed_mean_log2",
            "speed_seconds_per_tag",
            "speed_timestamped_assignments",
        ):
            _add_field(name)

        for name in (
            "pattern_strategy",
        ):
            _add_field(name)

        pattern_columns = sorted(
            column for column in all_columns if column.startswith("pattern_count_")
        )
        for column in pattern_columns:
            _add_field(column)

        remaining = sorted(all_columns - set(fieldnames))
        for column in remaining:
            _add_field(column)

        return ordered_rows, fieldnames

    @staticmethod
    def _stringify_csv_value(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return (
                f"{value:.6f}".rstrip("0").rstrip(".")
                if not value.is_integer()
                else str(int(value))
            )
        return str(value)

