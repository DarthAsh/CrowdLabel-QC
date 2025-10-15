"""Tagger domain model for crowd labeling quality control."""

from dataclasses import dataclass
import math
import statistics
from typing import Any, Dict, List, Optional

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.enums import TagValue

# Fraction of the longest intervals (by log2 seconds) to trim before taking the mean.
# For example, 0.1 means trim the top 10% longest intervals. This helps ignore very
# long breaks (e.g., overnight) when estimating a tagger's typical speed.
TRIM_FRACTION = 0.1

# Pattern detection thresholds (tunable heuristics)
LONG_RUN_THRESHOLD = 10
ALTERNATION_RATIO_THRESHOLD = 0.9
MIN_ALTERNATION_SEQUENCE = 10
NGRAM_REPEAT_THRESHOLD = 5


@dataclass(frozen=True)
class Tagger:
    """A tagger (crowd worker) who makes label assignments.
    
    A tagger represents an individual who participates in the
    crowd labeling process by making assignments for various
    characteristics across different comments.
    
    Attributes:
        id: Unique identifier for the tagger
        meta: Optional metadata about the tagger (e.g., demographics, experience)
        tagassignments: List of all tag assignments made by this tagger

    Notes:
        - This class expects TagAssignment objects that use *_id fields
          (e.g., `comment_id`, `characteristic_id`) rather than object
          references. Timestamps should be timezone-aware and normalized to
          UTC by the IO layer. Assignments missing timestamps are ignored.
    """

    id: str # Not sure if id is a string or int
    meta: Optional[Dict[str, Any]] = None
    tagassignments: List[TagAssignment] = None
    
    def __post_init__(self) -> None:
        """Initialize empty tagassignments list if not provided."""
        if self.tagassignments is None:
            object.__setattr__(self, 'tagassignments', [])
    
    def tagging_speed(self) -> float:
        """Calculate a log2-based average tagging speed.

        Procedure (robust to missing/None timestamps):
        - If there are fewer than 2 valid timestamps, return 0.0.
        - Sort assignments by timestamp and compute successive differences in seconds.
        - For each positive interval, compute log2(interval_seconds) and keep it.
                - Trim the TOP `TRIM_FRACTION` fraction of longest log2 intervals.
                    (upper-tail trimming only; this ignores long breaks such as
                    overnight gaps).
        - Return the mean of the remaining log2 intervals. A lower score implies a
          faster tagger (smaller time between tags).

        Returns:
            Mean of the log2(seconds) of typical intervals (float). Returns 0.0
            when not enough data is available.
        """

        """Deprecated: shim delegating to a tagging speed strategy.

        This method is preserved for backward compatibility. Migration plan:
        - Port `_compute_log_intervals` + trimming logic into
          `metrics.default_strategies.DefaultTaggingSpeedStrategy.speed_log2`.
        - Replace this shim to call the configured strategy.

        Current behavior: call the default strategy (skeleton) and return its
        result. For now, raise NotImplementedError to indicate migration is
        pending.

        # TODO: deprecate after callers migrate to strategies.
        """
        # The heavy logic originally lived here; keep it commented for porting:
        # ------------------------------------------------------------------
        # log_intervals = self._compute_log_intervals()
        # if not log_intervals:
        #     return 0.0
        # n = len(log_intervals)
        # trim_count = int(math.floor(n * TRIM_FRACTION))
        # if trim_count > 0:
        #     log_intervals_sorted = sorted(log_intervals)
        #     trimmed = log_intervals_sorted[: max(1, len(log_intervals_sorted) - trim_count)]
        # else:
        #     trimmed = log_intervals
        # if not trimmed:
        #     trimmed = log_intervals
        # try:
        #     return float(statistics.mean(trimmed))
        # except statistics.StatisticsError:
        #     return 0.0
        # ------------------------------------------------------------------
        # Delegate to the default strategy (not implemented yet)
        from qcc.metrics.default_strategies import DefaultTaggingSpeedStrategy

        strategy = DefaultTaggingSpeedStrategy()
        return strategy.speed_log2(self)

    def _compute_log_intervals(self) -> List[float]:
        """Compute log2 of positive successive intervals (seconds).

        Returns a list of log2(interval_seconds). This helper centralizes the
        interval extraction and can be used by other methods to decide
        "insufficient data" based on the number of intervals rather than the
        numeric value of the mean (which may be 0.0 for a true 1s interval).

        Notes:
        - We use the TagAssignment fields (comment_id, characteristic_id, timestamp)
          rather than object references; this is the project convention.
        - Assignments without a timestamp are ignored.
        - Timestamps should be timezone-aware and normalized to UTC upstream; non-conforming
          timestamps are skipped here (TODO: normalize in IO layer).
        """
        # Filter assignments with usable timestamps
        valid = [ta for ta in (self.tagassignments or []) if getattr(ta, 'timestamp', None) is not None]
        if len(valid) < 2:
            return []

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
        return log_intervals

    def seconds_per_tag(self) -> float:
        """Return an interpretable seconds-per-tag derived from the log2 mean.

        This companion method converts the log2-mean returned by `tagging_speed`
        back into seconds (by performing 2 ** mean). The internal trimming
        logic is kept consistent by passing the same fraction. This value is
        easier to interpret: lower seconds-per-tag = faster tagging.

        Returns 0.0 when insufficient data is available.
        """
        """Deprecated: shim that will delegate to tagging speed strategy helper.

        Returns seconds-per-tag derived from the strategy's log2 mean. For now
        delegate to DefaultTaggingSpeedStrategy.seconds_per_tag once the
        strategy returns a mean; currently raises NotImplementedError.

        # TODO: deprecate after callers migrate to strategies.
        """
        # Original logic commented for later porting:
        # log_intervals = self._compute_log_intervals()
        # if not log_intervals:
        #     return 0.0
        # mean_log2 = self.tagging_speed()
        # try:
        #     return float(2 ** mean_log2)
        # except Exception:
        #     return 0.0
        # ------------------------------------------------------------------
        # Delegate to strategy helper (not implemented yet)
        from qcc.metrics.default_strategies import DefaultTaggingSpeedStrategy

        raise NotImplementedError("seconds_per_tag is deprecated; use strategy helper")
    
    def agreement_with(self, other: "Tagger", characteristic: Characteristic) -> float:
        """Calculate simple percent agreement with another tagger for a characteristic.

        For each comment that both taggers have tagged for the given
        characteristic, we use only the latest tag assignment (by timestamp)
        from each tagger for that (comment, characteristic) pair. Equality is
        determined by comparing the `value` field for the two latest
        assignments. Missing assignments or comments without overlap are
        ignored. The metric returned is:

            matching / total_overlapping_comments

        where matching is the count of overlapping comments where the latest
        values are equal. Returns 0.0 when there is no overlap.

        This is intentionally simple and O(n) in the number of assignments.
        """
        """Deprecated: agreement shim.

        This should be migrated to an AgreementStrategy implementation. For now
        raise NotImplementedError to signal callers to use the strategy module.
        # TODO: deprecate after callers migrate to strategies.
        """
        # Keep the original logic commented for porting:
        # ------------------------------------------------------------------
        # def latest_by_comment(assignments: List[TagAssignment]) -> Dict[str, TagAssignment]:
        #     latest: Dict[str, TagAssignment] = {}
        #     for ta in assignments:
        #         if ta.characteristic_id != characteristic.id:
        #             continue
        #         if getattr(ta, 'timestamp', None) is None:
        #             continue
        #         cur = latest.get(ta.comment_id)
        #         if cur is None:
        #             latest[ta.comment_id] = ta
        #             continue
        #         if ta.timestamp > cur.timestamp:
        #             latest[ta.comment_id] = ta
        #         elif ta.timestamp == cur.timestamp:
        #             if str(ta.value) > str(cur.value):
        #                 latest[ta.comment_id] = ta
        #     return latest
        # self_latest = latest_by_comment(self.tagassignments or [])
        # other_latest = latest_by_comment(other.tagassignments or [])
        # ... (rest of logic)
        # ------------------------------------------------------------------
        raise NotImplementedError("agreement_with is deprecated; use AgreementStrategy")
    
    def pattern_signals(self, characteristic: Characteristic) -> Dict[str, Any]:
        """Detect pattern signals for a specific characteristic.
        
        Identifies potential systematic patterns in the tagger's
        assignments that might indicate bias or other issues.
        
        Args:
            characteristic: The characteristic to analyze patterns for
            
        Returns:
            Dictionary containing pattern analysis results
            
        Complexity: O(n) where n is the number of assignments for this characteristic
        """

        """Deprecated: pattern signals shim.

        Should be migrated to a PatternSignalsStrategy implementation. Keep the
        original algorithm commented here for future porting. For now raise
        NotImplementedError to force callers to move to strategy-based APIs.

        # TODO: deprecate after callers migrate to strategies.
        """
        # Original implementation (commented for porting):
        # ------------------------------------------------------------------
        # assignments_for_char = [
        #     ta for ta in (self.tagassignments or [])
        #     if ta.characteristic_id == characteristic.id and getattr(ta, 'timestamp', None) is not None
        # ]
        # if not assignments_for_char:
        #     return {"patterns_found": False, "details": []}
        # sorted_assignments = sorted(assignments_for_char, key=lambda ta: ta.timestamp)
        # tag_sequence = [ta.value for ta in sorted_assignments]
        # ... (rest of algorithm)
        # ------------------------------------------------------------------
        raise NotImplementedError("pattern_signals is deprecated; use PatternSignalsStrategy")