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


@dataclass(frozen=False)
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
    # Optional because a Tagger may be constructed without assignments for
    # testing or incremental assembly. Use Optional[List[TagAssignment]] to
    # satisfy static type checkers.
    tagassignments: Optional[List[TagAssignment]] = None
    
    def __post_init__(self) -> None:
        """Initialize empty tagassignments list if not provided."""
        if self.tagassignments is None:
            object.__setattr__(self, 'tagassignments', [])
    
    def tagging_speed(self) -> float:
        """Calculate a log2-based average tagging speed.

        NOTE: This method is now a thin shim that delegates the actual
        computation to the configured tagging-speed strategy
        (`qcc.metrics.speed_strategy.LogTrimTaggingSpeed`). The original
        algorithm (log2 of positive successive intervals with an upper-tail
        trim) is preserved in this module as commented code to serve as the
        source of truth for future refactors.

        Returns:
            Mean of the log2(seconds) of typical intervals (float). Returns
            0.0 when not enough data is available.
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
        # Delegate to the named strategy skeleton. The actual algorithm will be
        # ported into `qcc.metrics.speed_strategy.LogTrimTaggingSpeed` in a
        # follow-up PR. Keep the original implementation above (commented)
        # as the source of truth for the port.
        from qcc.metrics.speed_strategy import LogTrimTaggingSpeed

        strategy = LogTrimTaggingSpeed()
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
        # Delegate to the strategy helper for consistent behavior.
        from qcc.metrics.speed_strategy import LogTrimTaggingSpeed

        strategy = LogTrimTaggingSpeed()
        mean_log2 = strategy.speed_log2(self)
        return strategy.seconds_per_tag(mean_log2)
        
    
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
        def latest_by_comment(assignments: List[TagAssignment]) -> Dict[str, TagAssignment]:
            latest: Dict[str, TagAssignment] = {}
            for ta in assignments:
                if ta.characteristic_id != characteristic.id:
                    continue
                if getattr(ta, "timestamp", None) is None:
                    continue
                if getattr(ta, "value", None) == TagValue.NA:
                    continue
                cur = latest.get(ta.comment_id)
                if cur is None:
                    latest[ta.comment_id] = ta
                    continue
                if ta.timestamp > cur.timestamp:
                    latest[ta.comment_id] = ta
                elif ta.timestamp == cur.timestamp and str(ta.value) > str(cur.value):
                    latest[ta.comment_id] = ta
            return latest

        if not (self.tagassignments and other.tagassignments):
            raise NotImplementedError("agreement_with requires assignments; use AgreementStrategy")

        self_latest = latest_by_comment(self.tagassignments or [])
        other_latest = latest_by_comment(other.tagassignments or [])
        overlap = set(self_latest) & set(other_latest)
        if not overlap:
            return 0.0

        matching = sum(
            1
            for comment_id in overlap
            if self_latest[comment_id].value == other_latest[comment_id].value
        )
        return matching / len(overlap)
    
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
        assignments_for_char = [
            ta
            for ta in (self.tagassignments or [])
            if ta.characteristic_id == characteristic.id
            and getattr(ta, "timestamp", None) is not None
            and getattr(ta, "value", None) in (TagValue.YES, TagValue.NO)
        ]
        if not assignments_for_char:
            raise NotImplementedError("pattern_signals requires assignments; use PatternSignalsStrategy")

        sorted_assignments = sorted(assignments_for_char, key=lambda ta: ta.timestamp)
        sequence = [ta.value for ta in sorted_assignments]

        longest_run_len = 0
        longest_run_value: Optional[TagValue] = None
        current_len = 0
        current_value: Optional[TagValue] = None
        for value in sequence:
            if value == current_value:
                current_len += 1
            else:
                current_value = value
                current_len = 1
            if current_len > longest_run_len:
                longest_run_len = current_len
                longest_run_value = current_value

        alternations = 0
        for i in range(1, len(sequence)):
            if sequence[i] != sequence[i - 1]:
                alternations += 1
        alternation_ratio = alternations / (len(sequence) - 1) if len(sequence) > 1 else 0.0

        tokens = ["Y" if value == TagValue.YES else "N" for value in sequence]
        ngram_counts: Dict[str, int] = {}
        for i in range(len(tokens) - 2):
            ngram = "".join(tokens[i : i + 3])
            ngram_counts[ngram] = ngram_counts.get(ngram, 0) + 1

        top_repeats = [
            {"ngram": ngram, "count": count}
            for ngram, count in sorted(ngram_counts.items(), key=lambda item: item[1], reverse=True)
            if count > 1
        ]

        runs_summary: List[Dict[str, object]] = []
        if longest_run_len >= LONG_RUN_THRESHOLD:
            runs_summary.append(
                {
                    "type": "long_run",
                    "length": longest_run_len,
                    "value": longest_run_value,
                }
            )
        if len(sequence) >= MIN_ALTERNATION_SEQUENCE and alternation_ratio >= ALTERNATION_RATIO_THRESHOLD:
            runs_summary.append(
                {
                    "type": "alternation",
                    "ratio": alternation_ratio,
                    "transitions": alternations,
                }
            )
        repeat_trigger = next(
            (entry for entry in top_repeats if entry["count"] >= NGRAM_REPEAT_THRESHOLD),
            None,
        )
        if repeat_trigger:
            runs_summary.append(
                {
                    "type": "repeated_ngrams",
                    "ngram": repeat_trigger["ngram"],
                    "count": repeat_trigger["count"],
                }
            )

        patterns_found = bool(runs_summary)

        return {
            "patterns_found": patterns_found,
            "longest_run": {"length": longest_run_len, "value": longest_run_value},
            "alternations": {"ratio": alternation_ratio, "count": alternations},
            "runs_summary": runs_summary,
            "top_repeats": top_repeats,
        }
