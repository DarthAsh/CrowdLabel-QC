from __future__ import annotations

"""
tagging-speed strategy implementation.

Constraints:
- Pure, deterministic, no I/O
- Use forward refs for domain types to avoid circular imports
"""

from typing import List
import math
import statistics

from .interfaces import TaggingSpeedStrategy


TRIM_FRACTION = 0.1


class TrimmedMeanTaggingSpeed(TaggingSpeedStrategy):
    """Tagging speed: trimmed mean of inter-tag intervals in seconds.

    For a given tagger, compute the time in seconds between consecutive
    tag assignments, sort those intervals, drop the slowest
    TRIM_FRACTION fraction, and return the mean of the remaining
    intervals. The result is "seconds per tag" ignoring long idle gaps.
    """

    def speed_seconds(self, tagger: "Tagger") -> float:
        valid = [
            ta for ta in (tagger.tagassignments or [])
            if getattr(ta, "timestamp", None) is not None
        ]
        if len(valid) < 2:
            return 0.0

        sorted_assignments = sorted(valid, key=lambda ta: ta.timestamp)
        intervals: List[float] = []
        for i in range(1, len(sorted_assignments)):
            t0 = sorted_assignments[i - 1].timestamp
            t1 = sorted_assignments[i].timestamp
            try:
                delta_seconds = (t1 - t0).total_seconds()
            except Exception:
                continue
            if delta_seconds > 0:
                intervals.append(delta_seconds)

        if not intervals:
            return 0.0

        intervals_sorted = sorted(intervals)
        n = len(intervals_sorted)
        trim_count = int(math.floor(n * TRIM_FRACTION))  # 10%

        if trim_count > 0:
            trimmed = intervals_sorted[: max(1, n - trim_count)]
        else:
            trimmed = intervals_sorted

        try:
            return float(statistics.mean(trimmed))
        except statistics.StatisticsError:
            return 0.0
