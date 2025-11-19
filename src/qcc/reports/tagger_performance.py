"""Tagger performance reporting for crowd labeling quality control."""

from __future__ import annotations

import csv
import math
from pathlib import Path
from collections import Counter
from typing import Dict, List, Mapping, Sequence, Tuple, Optional

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.domain import enums
from qcc.metrics.pattern_strategy import (
    HorizontalPatternDetection,
    VerticalPatternDetection,
)
from qcc.metrics.speed_strategy import LogTrimTaggingSpeed
from qcc.metrics.utils.pattern import PatternCollection
from qcc.metrics.agreement import AgreementMetrics


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
        agreement_methods: Optional[Sequence[str]] = None,
    ) -> Dict[str, object]:
        """Generate a summary performance report for taggers.

        Speed, pattern, and agreement metrics are supported. Agreement analysis
        delegates to :class:`qcc.metrics.agreement.AgreementMetrics` and
        inherits its latest-label semantics.

        Args:
            taggers: Taggers to summarize.
            characteristics: Characteristics to analyze for agreement.
            include_speed: Include tagging speed metrics in the response.
            include_patterns: Include pattern detection metrics in the response.
            include_agreement: Include agreement analysis in the response.
            agreement_methods: Optional ordered list of method identifiers to
                compute when agreement metrics are requested. Unrecognized
                methods are ignored.
        """

        summary: Dict[str, object] = {}

        if include_agreement:
            summary["agreement"] = self._generate_agreement_summary(
                characteristics,
                agreement_methods
                or ("percent_agreement", "cohens_kappa", "krippendorffs_alpha"),
            )

        if include_speed:
            summary["tagger_speed"] = self._generate_speed_summary(taggers)

        if include_patterns:
            summary["pattern_detection"] = self._generate_pattern_summary(
                taggers, characteristics
            )

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

    def _generate_pattern_summary(
        self,
        taggers: Sequence[Tagger],
        characteristics: Sequence[Characteristic],
    ) -> Dict[str, object]:
        horizontal_strategy = HorizontalPatternDetection()
        vertical_strategy = VerticalPatternDetection()
        tracked_patterns = PatternCollection.return_all_patterns()
        horizontal_patterns: List[Dict[str, object]] = []
        vertical_patterns: List[Dict[str, object]] = []

        for tagger in taggers:
            assignments = self._eligible_yes_no_assignments(tagger.tagassignments or [])
            pattern_counts = horizontal_strategy.analyze(tagger)
            positive_patterns = {
                pattern: count
                for pattern, count in (pattern_counts or {}).items()
                if count > 1 and len(pattern) > 1
            }
            if not positive_patterns and assignments:
                positive_patterns = self._short_sequence_pattern_counts(
                    horizontal_strategy, assignments, tracked_patterns
                )

            if positive_patterns:
                horizontal_entry = {
                    "tagger_id": str(tagger.id),
                    "patterns": dict(sorted(positive_patterns.items())),
                }
                horizontal_patterns.append(horizontal_entry)

            if characteristics:
                aggregate_counts: Counter[str] = Counter()
                for characteristic in characteristics:
                    char_assignments = self._assignments_for_characteristic(
                        assignments, characteristic.id
                    )
                    char_counts = vertical_strategy.analyze(tagger, characteristic)
                    if not char_counts and char_assignments:
                        char_counts = self._short_sequence_pattern_counts(
                            vertical_strategy, char_assignments, tracked_patterns
                        )
                    if char_counts:
                        aggregate_counts.update(char_counts)
                vertical_positive = {
                    pattern: count
                    for pattern, count in aggregate_counts.items()
                    if count > 1 and len(pattern) > 1
                }
                if vertical_positive:
                    vertical_patterns.append(
                        {
                            "tagger_id": str(tagger.id),
                            "patterns": dict(sorted(vertical_positive.items())),
                        }
                    )

        return {
            "strategy": "HorizontalPatternDetection",
            "patterns_tracked": tracked_patterns,
            "per_tagger": horizontal_patterns,
            "horizontal": {
                "strategy": "HorizontalPatternDetection",
                "per_tagger": horizontal_patterns,
            },
            "vertical": {
                "strategy": "VerticalPatternDetection",
                "per_tagger": vertical_patterns,
            },
        }

    def _generate_agreement_summary(
        self,
        characteristics: Sequence[Characteristic],
        methods: Sequence[str],
    ) -> Dict[str, object]:
        metrics = AgreementMetrics()
        per_characteristic: List[Dict[str, object]] = []

        for characteristic in characteristics:
            relevant_assignments = [
                assignment
                for assignment in self.assignments
                if assignment.characteristic_id == characteristic.id
            ]

            if not relevant_assignments:
                continue

            char_entry: Dict[str, object] = {
                "characteristic_id": str(characteristic.id),
                "characteristic_name": getattr(characteristic, "name", str(characteristic.id)),
            }

            for method in methods:
                if method == "percent_agreement":
                    char_entry[method] = metrics.percent_agreement(
                        relevant_assignments, characteristic
                    )
                elif method == "cohens_kappa":
                    char_entry[method] = metrics.cohens_kappa(
                        relevant_assignments, characteristic
                    )
                elif method == "krippendorffs_alpha":
                    char_entry[method] = metrics.krippendorffs_alpha(
                        relevant_assignments, characteristic
                    )
                elif method == "agreement_matrix":
                    char_entry[method] = metrics.agreement_matrix(
                        relevant_assignments, characteristic
                    )

            per_tagger_metrics = metrics.per_tagger_metrics(
                relevant_assignments, characteristic, methods
            )
            if per_tagger_metrics:
                char_entry["per_tagger"] = [
                    {"tagger_id": tagger_id, **per_tagger_metrics[tagger_id]}
                    for tagger_id in sorted(per_tagger_metrics)
                ]

            per_characteristic.append(char_entry)

        return {
            "strategy": metrics.strategy.__class__.__name__,
            "methods": list(methods),
            "per_characteristic": per_characteristic,
        }

    @staticmethod
    def _eligible_yes_no_assignments(
        assignments: Sequence[TagAssignment],
    ) -> List[TagAssignment]:
        filtered = [
            assignment
            for assignment in assignments
            if getattr(assignment, "timestamp", None) is not None
            and getattr(assignment, "value", None) in (enums.TagValue.YES, enums.TagValue.NO)
        ]

        return sorted(filtered, key=lambda assignment: assignment.timestamp)

    @staticmethod
    def _assignments_for_characteristic(
        assignments: Sequence[TagAssignment], characteristic_id: object
    ) -> List[TagAssignment]:
        return [
            assignment
            for assignment in assignments
            if getattr(assignment, "characteristic_id", None) == characteristic_id
        ]

    @staticmethod
    def _short_sequence_pattern_counts(
        strategy: HorizontalPatternDetection,
        assignments: Sequence[TagAssignment],
        tracked_patterns: Sequence[str],
    ) -> Dict[str, int]:
        sequence = strategy.build_sequence_str(assignments)
        if not sequence:
            return {}

        return {
            pattern: strategy.count_pattern_repetition(pattern, sequence)
            for pattern in tracked_patterns
            if len(pattern) > 1 and strategy.count_pattern_repetition(pattern, sequence) > 1
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
            pattern_entries: List[Tuple[str, Mapping[str, object]]] = []

            for label in ("horizontal", "vertical"):
                entry = pattern_summary.get(label)
                if isinstance(entry, Mapping):
                    pattern_entries.append((label, entry))

            if not pattern_entries and (
                "strategy" in pattern_summary or "per_tagger" in pattern_summary
            ):
                pattern_entries.append(("horizontal", pattern_summary))

            for label, entry in pattern_entries:
                strategy = entry.get("strategy")
                per_tagger = entry.get("per_tagger", []) or []
                for tagger_entry in per_tagger:
                    if not isinstance(tagger_entry, Mapping):
                        continue
                    tagger_id = str(tagger_entry.get("tagger_id", "")).strip()
                    if not tagger_id:
                        continue
                    tagger_row = _row_for(tagger_id)
                    column_prefix = "pattern"
                    if label not in ("", "horizontal"):
                        column_prefix = f"pattern_{label}"
                    strategy_column = (
                        "pattern_strategy"
                        if column_prefix == "pattern"
                        else f"{column_prefix}_strategy"
                    )
                    if strategy:
                        tagger_row[strategy_column] = str(strategy)
                        all_columns.add(strategy_column)
                    patterns = tagger_entry.get("patterns") or {}
                    if isinstance(patterns, Mapping):
                        for pattern, count in patterns.items():
                            count_column = (
                                f"pattern_count_{pattern}"
                                if column_prefix == "pattern"
                                else f"{column_prefix}_count_{pattern}"
                            )
                            tagger_row[count_column] = self._stringify_csv_value(count)
                            all_columns.add(count_column)

        agreement_summary = summary.get("agreement", {}) if summary else {}
        if isinstance(agreement_summary, Mapping) and agreement_summary:
            per_characteristic = agreement_summary.get("per_characteristic", []) or []
            per_tagger_values: Dict[str, Dict[str, List[float]]] = {}

            for characteristic_entry in per_characteristic:
                if not isinstance(characteristic_entry, Mapping):
                    continue
                per_tagger = characteristic_entry.get("per_tagger", []) or []
                for tagger_entry in per_tagger:
                    if not isinstance(tagger_entry, Mapping):
                        continue
                    tagger_id = str(tagger_entry.get("tagger_id", "")).strip()
                    if not tagger_id:
                        continue
                    for metric_name, metric_value in tagger_entry.items():
                        if metric_name == "tagger_id":
                            continue
                        if isinstance(metric_value, (int, float)) and math.isfinite(float(metric_value)):
                            per_tagger_values.setdefault(tagger_id, {}).setdefault(metric_name, []).append(float(metric_value))

            for tagger_id, metrics in per_tagger_values.items():
                tagger_row = _row_for(tagger_id)
                for metric_name, values in metrics.items():
                    if not values:
                        continue
                    average_value = sum(values) / len(values)
                    column = f"agreement_{metric_name}"
                    tagger_row[column] = self._stringify_csv_value(average_value)
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

        pattern_strategy_columns = sorted(
            column
            for column in all_columns
            if column.startswith("pattern") and column.endswith("strategy")
        )
        for column in pattern_strategy_columns:
            _add_field(column)

        pattern_columns = sorted(
            column
            for column in all_columns
            if column.startswith("pattern") and "_count_" in column
        )
        for column in pattern_columns:
            _add_field(column)

        agreement_columns = sorted(
            column for column in all_columns if column.startswith("agreement_")
        )
        for column in agreement_columns:
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

