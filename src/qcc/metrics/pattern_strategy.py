from __future__ import annotations
from typing import Dict, Optional
from .interfaces import PatternSignalsStrategy
from ..domain import enums
class VerticalPatternDetection(PatternSignalsStrategy):
    """
    Class to detect if there are patterns for tag assignments for specific characteristics
    This pattern detection strategy assumes that students are marking tags vertically, instead of across the review question.
    """


    # TODO - why is characteristic a parameter? Try to understand where pattern detection is happening again.
    # tag assignments should be in a chronological order
    # there are probably tag assignments that should not be part of a pattern after certain amount of time has elapsed
    
    # it would make more sense to analyze patterns for a Tagger, NOT based on the characteristic, but just all their tags
    # arranged in chronological order for a specific assignment

    # it seems pattern evaluation for characteristic would be assuming that the students are tagging in a vertical order
    # but students could also be tagging horizontally (in order to complete tagging for a review question before moving on to the next)


    def analyze(
        self,
        tagger: "Tagger",
        char: Optional["Characteristic"] = None,
    ) -> Dict[str, int]:
        """
        Analyze the tagging sequence of a single Tagger for a single Characteristic
        to detect simple repetitive or suspicious patterns.

        Args:
            tagger (Tagger): The tagger whose assignments are being analyzed.
            char (Characteristic): The characteristic under evaluation.

        Returns:
            dict: A dictionary such that the key represents a pattern, and the value represents the frequency of its occurrences.
        """
        # TODO: Implement pattern detection logic (e.g., runs, alternations, N-grams).


        # Plan
        # Get list of TagAssignments for Tagger
        if char is None:
            return {}

        assignments = list(tagger.tagassignments or [])

        # SORT assignments in ascending order (since I assume that that is how the student has assigned the tags)

        # only use assignments that have a timestamp, and a Yes/No value
        marked_assignments = (
            ta for ta in assignments
            if getattr(ta, "timestamp", None) is not None
            and (ta.value in (enums.TagValue.YES, enums.TagValue.NO))
        )

        sorted_assignments = sorted(
            marked_assignments, key=lambda ta: ta.timestamp,
        )

        # Filter list to only contain TagAssignments for given Characteristic
        search_char_id = getattr(char, "id", None)
        char_assignments = []
        for assignment in sorted_assignments:
            characteristic_id = getattr(assignment, "characteristic_id", None)
            if characteristic_id is not None and search_char_id == characteristic_id:
                char_assignments.append(assignment)

        return self.generate_pattern_frequency(char_assignments)
    

class HorizontalPatternDetection(PatternSignalsStrategy):
    """
    Class to detect if there are patterns for tag assignments for specific characteristics
    This pattern detection strategy assumes that students are marking tags horizontally, such that all tags for a review question are marked
    before moving on to the next review question
    """

    def analyze(self, tagger: "Tagger") -> Dict[str, int]:
        """
        Analyze the tagging sequence of a single Tagger to detect simple repetitive or suspicious patterns.

        Args:
            tagger (Tagger): The tagger whose assignments are being analyzed.

        Returns:
            dict: A dictionary such that the key represents a pattern, and the value represents the frequency of its occurrences.
        """
        
        # Plan
        # Get list of TagAssignments for Tagger
        assignments = list(tagger.tagassignments or [])

        # SORT assignments in ascending order (since I assume that that is how the student has assigned the tags)
        # only use assignments that have a timestamp, and a Yes/No value
        marked_assignments = (
            ta for ta in assignments
            if getattr(ta, "timestamp", None) is not None
            and (ta.value in (enums.TagValue.YES, enums.TagValue.NO))
        )

        sorted_assignments = sorted(
            marked_assignments, key=lambda ta: ta.timestamp,
        )        


        return self.generate_pattern_frequency(sorted_assignments)
