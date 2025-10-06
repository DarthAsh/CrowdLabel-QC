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

        # Use helper to compute log intervals; tagging_speed reports the mean
        log_intervals = self._compute_log_intervals()
        if not log_intervals:
            return 0.0

    # Trim the top TRIM_FRACTION longest log intervals
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
        # Check there are intervals; use the same trimming semantics as
        # `tagging_speed` when converting back to seconds so the two metrics
        # align (seconds_per_tag mirrors the trimmed view).
        log_intervals = self._compute_log_intervals()
        if not log_intervals:
            return 0.0
        mean_log2 = self.tagging_speed()
        if mean_log2 == 0.0 and log_intervals:
            # If mean is numerically zero but intervals exist, that's a valid
            # 1-second typical interval; we still convert.
            try:
                return float(2 ** mean_log2)
            except Exception:
                return 0.0
        try:
            return float(2 ** mean_log2)
        except Exception:
            return 0.0
    
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
        # Helper: build map comment_id -> latest TagAssignment for this characteristic
        def latest_by_comment(assignments: List[TagAssignment]) -> Dict[str, TagAssignment]:
            """Return latest TagAssignment per comment for this characteristic.

            Tie-break rule: when two assignments have the same timestamp, pick the
            one with the lexicographically greater value name (deterministic).
            Assignments missing timestamps are ignored.
            """
            latest: Dict[str, TagAssignment] = {}
            for ta in assignments:
                if ta.characteristic_id != characteristic.id:
                    continue
                if getattr(ta, 'timestamp', None) is None:
                    continue
                cur = latest.get(ta.comment_id)
                if cur is None:
                    latest[ta.comment_id] = ta
                    continue
                if ta.timestamp > cur.timestamp:
                    latest[ta.comment_id] = ta
                elif ta.timestamp == cur.timestamp:
                    # deterministic tie-break: higher enum name wins
                    if str(ta.value) > str(cur.value):
                        latest[ta.comment_id] = ta
            return latest

        self_latest = latest_by_comment(self.tagassignments or [])
        other_latest = latest_by_comment(other.tagassignments or [])

        overlap = set(self_latest.keys()).intersection(other_latest.keys())
        if not overlap:
            return 0.0

        matches = 0
        for cid in overlap:
            a = self_latest[cid].value
            b = other_latest[cid].value
            # Exclude NA values from agreement calculation (do not count as match or mismatch)
            if a == TagValue.NA or b == TagValue.NA:
                # treat as non-overlapping for agreement purposes
                continue
            if a == b:
                matches += 1
        # Compute denominator as the number of overlapping comments considered
        # (exclude pairs with NA)
        considered = 0
        for cid in overlap:
            a = self_latest[cid].value
            b = other_latest[cid].value
            if a == TagValue.NA or b == TagValue.NA:
                continue
            considered += 1
        if considered == 0:
            return 0.0
        return float(matches) / float(considered)
    
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

        # Collect assignments for this characteristic and ensure they have timestamps
        assignments_for_char = [
            ta for ta in (self.tagassignments or [])
            if ta.characteristic_id == characteristic.id and getattr(ta, 'timestamp', None) is not None
        ]
        # If this tagger has no tags for this characteristic...
        if not assignments_for_char:
            # then there's nothing to check, so we return a default value.
            return {"patterns_found": False, "details": []}

        # Sort chronologically and build sequence
        sorted_assignments = sorted(assignments_for_char, key=lambda ta: ta.timestamp)
        tag_sequence = [ta.value for ta in sorted_assignments]
        n = len(tag_sequence)

        # Longest run
        longest_run_value = None
        longest_run_length = 0
        if n > 0:
            cur_val = tag_sequence[0]
            cur_len = 1
            longest_run_value = cur_val
            longest_run_length = 1
            for v in tag_sequence[1:]:
                if v == cur_val:
                    cur_len += 1
                else:
                    if cur_len > longest_run_length:
                        longest_run_length = cur_len
                        longest_run_value = cur_val
                    cur_val = v
                    cur_len = 1
            if cur_len > longest_run_length:
                longest_run_length = cur_len
                longest_run_value = cur_val

        # Alternations
        switches = 0
        if n > 1:
            for i in range(1, n):
                if tag_sequence[i] != tag_sequence[i - 1]:
                    switches += 1
            alternation_ratio = switches / float(n - 1)
        else:
            alternation_ratio = 0.0

        # n-gram repeats (3..5)
        from collections import Counter

        top_repeats: Dict[str, int] = {}
        for L in range(3, 6):
            if n < L:
                continue
            c = Counter()
            for i in range(0, n - L + 1):
                gram = tuple(tag_sequence[i:i + L])
                c[gram] += 1
            for gram, cnt in c.items():
                if cnt > 1:
                    top_repeats["-".join(map(str, gram))] = max(top_repeats.get("-".join(map(str, gram)), 0), cnt)

        # Pattern thresholds
        patterns_found = False
        runs_summary = []
        if longest_run_length >= 10:
            patterns_found = True
            runs_summary.append({"type": "long_run", "value": str(longest_run_value), "length": longest_run_length})
        if alternation_ratio >= 0.9 and n >= 10:
            patterns_found = True
            runs_summary.append({"type": "alternation", "switches": switches, "ratio": alternation_ratio})
        repeated_ngrams = {g: c for g, c in top_repeats.items() if c >= 5}
        if repeated_ngrams:
            patterns_found = True
            runs_summary.append({"type": "repeated_ngrams", "ngrams": repeated_ngrams})

        details = {
            "patterns_found": patterns_found,
            "sequence_length": n,
            "longest_run": {"value": str(longest_run_value), "length": longest_run_length},
            "alternations": {"switches": switches, "ratio": alternation_ratio},
            "top_repeats": top_repeats,
            "runs_summary": runs_summary,
        }

        return details
