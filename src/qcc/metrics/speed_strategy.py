from __future__ import annotations

"""Skeleton tagging-speed strategy implementation.

This module provides a named strategy class for the log2+trim tagging-speed
algorithm. It is a skeleton only: methods raise NotImplementedError and
contain TODO notes pointing to the original implementation in
`qcc.domain.tagger` which should be ported in a follow-up PR.

Constraints:
- Pure, deterministic, no I/O
- Use forward refs for domain types to avoid circular imports
"""

from typing import Any, List
import math
import statistics

from .interfaces import TaggingSpeedStrategy

TRIM_FRACTION = 0.1

class LogTrimTaggingSpeed(TaggingSpeedStrategy):

    
    def speed_log2(self, tagger: "Tagger") -> float:
        """Return mean log2(seconds) between tags.

        This ports the original algorithm from `Tagger.tagging_speed` but
        operates on the provided `tagger` argument. It is pure and
        deterministic and performs an upper-tail trim of the longest
        intervals controlled by TRIM_FRACTION.
        """
        # Compute log2 intervals using the same approach as the original
        # Tagger implementation. We operate on tagger.tagassignments instead
        # of `self.tagassignments` because this is a strategy method.
        # Filter assignments with usable timestamps
        valid = [ta for ta in (tagger.tagassignments or []) if getattr(ta, "timestamp", None) is not None]
        if len(valid) < 2:
            return 0.0

        sorted_assignments = sorted(valid, key=lambda ta: ta.timestamp)
        log_intervals: List[float] = []
        for i in range(1, len(sorted_assignments)):
            t0 = sorted_assignments[i - 1].timestamp
            t1 = sorted_assignments[i].timestamp
            try:
                delta_seconds = (t1 - t0).total_seconds()
            except Exception:
                continue
            if delta_seconds > 0:
                log_intervals.append(math.log2(delta_seconds))

        if not log_intervals:
            return 0.0

        n = len(log_intervals)
        trim_count = int(math.floor(n * TRIM_FRACTION))
        if trim_count > 0:
            log_intervals_sorted = sorted(log_intervals)
            trimmed = log_intervals_sorted[: max(1, len(log_intervals_sorted) - trim_count)]
        else:
            trimmed = log_intervals
        if not trimmed:
            trimmed = log_intervals
        try:
            return float(statistics.mean(trimmed))
        except statistics.StatisticsError:
            return 0.0



    @staticmethod
    def seconds_per_tag(mean_log2: float) -> float:
        """Convert mean_log2 back to seconds-per-tag (2 ** mean_log2).

        Skeleton only: keeps signature for callers to rely on during migration.
        """
        # The conversion is 2 ** mean_log2
        try:
            return float(2 ** mean_log2)
        except Exception:
            raise
