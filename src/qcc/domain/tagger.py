"""Tagger domain model for crowd labeling quality control."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment


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
        """Calculate the average tagging speed for this tagger.
        
        Returns:
            Average time between tag assignments in seconds
            
        Complexity: O(n) where n is the number of tag assignments
        """
         # If there are less than 2 tags, we can't calculate an interval.
        if len(self.tagassignments) < 2:
            return 0.0
        
        # Sort the tags by time to put them in the right order.
        sorted_assignments = sorted(self.tagassignments, key=lambda ta: ta.timestamp)

        # Create a list of the time differences in seconds.
        intervals_in_seconds = []
        for i in range(1, len(sorted_assignments)):
            time_delta = sorted_assignments[i].timestamp - sorted_assignments[i-1].timestamp
            intervals_in_seconds.append(time_delta.total_seconds())

        '''
        TODO: Previously the final calculation was to take the log base 2 of each interval
            and then find the average. This method was chosen to
            correctly handle long breaks.
        '''
        # For now, just return a default placeholder value.
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
