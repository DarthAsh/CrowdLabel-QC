"""Tagger domain model for crowd labeling quality control."""

from dataclasses import dataclass
import math
import statistics
from typing import Any, Dict, List, Optional

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment

# Fraction of the longest intervals (by log2 seconds) to trim before taking the mean.
# For example, 0.1 means trim the top 10% longest intervals. This helps ignore very
# long breaks (e.g., overnight) when estimating a tagger's typical speed.
TRIM_FRACTION = 0.1


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
        - Return the mean of the remaining log2 intervals. A lower score implies a
          faster tagger (smaller time between tags).

        Returns:
            Mean of the log2(seconds) of typical intervals (float). Returns 0.0
            when not enough data is available.
        """

        # Filter out assignments that lack a usable timestamp
        valid_assignments = [ta for ta in (self.tagassignments or []) if getattr(ta, 'timestamp', None) is not None]

        # Need at least two timestamps to compute an interval
        if len(valid_assignments) < 2:
            return 0.0

        # Sort by timestamp
        sorted_assignments = sorted(valid_assignments, key=lambda ta: ta.timestamp)

        # Compute successive differences in seconds and take log2 of positive intervals
        log_intervals = []
        for i in range(1, len(sorted_assignments)):
            t0 = sorted_assignments[i-1].timestamp
            t1 = sorted_assignments[i].timestamp
            # Defensive: ensure attributes are datetimes with total_seconds
            try:
                delta_seconds = (t1 - t0).total_seconds()
            except Exception:
                # If subtraction or total_seconds fails, skip this pair
                continue
            if delta_seconds > 0:
                # Use log2 to compress long tails; skip non-positive deltas
                log_intervals.append(math.log2(delta_seconds))

        # If no positive intervals found, cannot compute speed
        if not log_intervals:
            return 0.0

        # Trim the top TRIM_FRACTION longest log intervals
        n = len(log_intervals)
        trim_count = int(math.floor(n * TRIM_FRACTION))
        if trim_count > 0:
            # Remove the largest `trim_count` values
            log_intervals_sorted = sorted(log_intervals)  # ascending
            trimmed = log_intervals_sorted[: max(1, len(log_intervals_sorted) - trim_count)]
        else:
            trimmed = log_intervals

        # Defensive: if trimming removed everything (shouldn't happen), fall back
        if not trimmed:
            trimmed = log_intervals

        # Return mean of the remaining log2 intervals
        try:
            return float(statistics.mean(trimmed))
        except statistics.StatisticsError:
            return 0.0
    
    def agreement_with(self, other: "Tagger", characteristic: Characteristic) -> float:
        """Calculate agreement with another tagger for a specific characteristic.
        
        Args:
            other: The other tagger to compare against
            characteristic: The characteristic to calculate agreement for
            
        Returns:
            Agreement score between 0.0 and 1.0 with the other tagger
            
        Complexity: O(n) where n is the number of common assignments
        """
        # Get a unique set of all comment IDs this tagger ('self') has worked on.
        self_comments = {ta.comment.id for ta in self.tagassignments}
        # Get a unique set of all comment IDs the other tagger has worked on.
        other_comments = {ta.comment.id for ta in other.tagassignments}

        # Find the comment IDs that appear in BOTH sets.
        common_comment_ids = self_comments.intersection(other_comments)

        # If the set of common comments is empty (they have no work in common)
        if not common_comment_ids:
            return 0.0 # No common comments to compare

        '''
        TODO: We use the common comment IDs to calculate a real agreement score.
            1) Krippendorff's Alpha ?
            2) Agreement with the Majority (Mode) ?
            3) Agreement Fraction ?
        '''
        # Placeholder Return Value
        return 0.0 
    
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

        # Get the tags that match this characteristic.
        assignments_for_char = [
            ta for ta in self.tagassignments
            if ta.characteristic.id == characteristic.id
        ]
        # If this tagger has no tags for this characteristic...
        if not assignments_for_char:
            # then there's nothing to check, so we return a default value.
            return {"patterns_found": False, "details": []}
        
        # Sort the tags by time to put them in the right order.
        sorted_assignments = sorted(assignments_for_char, key=lambda ta: ta.timestamp)
        
        # Create a simple list of the answers (like 'Y' or 'N').
        tag_sequence = [ta.value for ta in sorted_assignments]

        '''
        TODO: The 'tag_sequence' will be passed to a pattern-detection algorithm.
            The algorithm would ideally look for:
            - Longest run of 'yes' or 'no' answers.
            - Short, repeated patterns (e.g., 'YNY', 'YYN').
            - Alternating patterns (e.g., 'YNYN...').
        '''
        # For now, return a default dictionary.
        return {
            "patterns_found": False,
            "details": [],
            "sequence_length": len(tag_sequence)
        }
