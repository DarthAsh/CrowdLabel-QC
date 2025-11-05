"""Tagger performance reporting for crowd labeling quality control."""

from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

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
        rows = self._build_csv_rows(report_data)

        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[
                    "Strategy",
                    "user_id",
                    "Metric",
                    "Value",
                    "pattern_detected",
                    "pattern_value",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)

    def _generate_speed_summary(self, taggers: Sequence[Tagger]) -> Dict[str, object]:
        speed_strategy = LogTrimTaggingSpeed()
        per_tagger_speed: List[Dict[str, object]] = []
        seconds_samples: List[float] = []

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

            if seconds_value > 0:
                seconds_samples.append(seconds_value)

        if seconds_samples:
            mean_seconds = mean(seconds_samples)
            median_seconds = median(seconds_samples)
            min_seconds = min(seconds_samples)
            max_seconds = max(seconds_samples)
        else:
            mean_seconds = 0.0
            median_seconds = 0.0
            min_seconds = 0.0
            max_seconds = 0.0

        return {
            "strategy": "LogTrimTaggingSpeed",
            "taggers_with_speed": len(per_tagger_speed),
            "seconds_per_tag": {
                "mean": mean_seconds,
                "median": median_seconds,
                "min": min_seconds,
                "max": max_seconds,
            },
            "per_tagger": per_tagger_speed,
        }

    def _generate_pattern_summary(self, taggers: Sequence[Tagger]) -> Dict[str, object]:
        pattern_strategy = HorizontalPatternDetection()
        tracked_patterns = PatternCollection.return_all_patterns()
        per_tagger_patterns: List[Dict[str, object]] = []
        aggregate_pattern_counts: MutableMapping[str, int] = defaultdict(int)
        taggers_with_patterns = 0

        for tagger in taggers:
            pattern_counts = pattern_strategy.analyze(tagger)
            positive_patterns = {
                pattern: count
                for pattern, count in (pattern_counts or {}).items()
                if count > 1 and len(pattern) > 1
            }

            if not positive_patterns:
                continue

            taggers_with_patterns += 1
            per_tagger_patterns.append(
                {
                    "tagger_id": str(tagger.id),
                    "patterns": dict(sorted(positive_patterns.items())),
                }
            )

            for pattern, count in positive_patterns.items():
                aggregate_pattern_counts[pattern] += count

        return {
            "strategy": "HorizontalPatternDetection",
            "patterns_tracked": tracked_patterns,
            "taggers_with_patterns": taggers_with_patterns,
            "aggregate_counts": dict(sorted(aggregate_pattern_counts.items())),
            "per_tagger": per_tagger_patterns,
        }

    def _build_csv_rows(self, summary: Mapping[str, object]) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        tagger_speed = summary.get("tagger_speed", {}) if summary else {}

        if tagger_speed:
            taggers_with_speed = tagger_speed.get("taggers_with_speed", 0)
            rows.append(
                {
                    "Strategy": "Tagger Speed",
                    "user_id": "aggregate",
                    "Metric": "taggers_with_speed",
                    "Value": self._stringify_csv_value(taggers_with_speed),
                    "pattern_detected": "",
                    "pattern_value": "",
                }
            )

            seconds_section = tagger_speed.get("seconds_per_tag", {}) or {}
            for metric_name, metric_value in seconds_section.items():
                rows.append(
                    {
                        "Strategy": "Tagger Speed",
                        "user_id": "aggregate",
                        "Metric": f"seconds_per_tag_{metric_name}",
                        "Value": self._stringify_csv_value(metric_value),
                        "pattern_detected": "",
                        "pattern_value": "",
                    }
                )

            for tagger_entry in tagger_speed.get("per_tagger", []) or []:
                tagger_id = str(tagger_entry.get("tagger_id", ""))
                for metric_name in ("mean_log2", "seconds_per_tag", "timestamped_assignments"):
                    if metric_name in tagger_entry:
                        rows.append(
                            {
                                "Strategy": "Tagger Speed",
                                "user_id": tagger_id,
                                "Metric": metric_name,
                                "Value": self._stringify_csv_value(tagger_entry[metric_name]),
                                "pattern_detected": "",
                                "pattern_value": "",
                            }
                        )

        pattern_summary = summary.get("pattern_detection", {}) if summary else {}
        if pattern_summary:
            rows.append(
                {
                    "Strategy": "Pattern Detection",
                    "user_id": "aggregate",
                    "Metric": "taggers_with_patterns",
                    "Value": self._stringify_csv_value(
                        pattern_summary.get("taggers_with_patterns", 0)
                    ),
                    "pattern_detected": "",
                    "pattern_value": "",
                }
            )

            aggregate_counts = pattern_summary.get("aggregate_counts", {}) or {}
            for pattern, count in aggregate_counts.items():
                rows.append(
                    {
                        "Strategy": "Pattern Detection",
                        "user_id": "aggregate",
                        "Metric": "pattern_count",
                        "Value": self._stringify_csv_value(count),
                        "pattern_detected": pattern,
                        "pattern_value": self._stringify_csv_value(count),
                    }
                )

            for entry in pattern_summary.get("per_tagger", []) or []:
                tagger_id = str(entry.get("tagger_id", ""))
                for pattern, count in (entry.get("patterns") or {}).items():
                    rows.append(
                        {
                            "Strategy": "Pattern Detection",
                            "user_id": tagger_id,
                            "Metric": "pattern_count",
                            "Value": self._stringify_csv_value(count),
                            "pattern_detected": pattern,
                            "pattern_value": self._stringify_csv_value(count),
                        }
                    )

        if not rows:
            rows.append(
                {
                    "Strategy": "Tagger Speed",
                    "user_id": "aggregate",
                    "Metric": "",
                    "Value": "",
                    "pattern_detected": "",
                    "pattern_value": "",
                }
            )

        return rows

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

