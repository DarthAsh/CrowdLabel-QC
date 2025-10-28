from ..domain.enums import TagValue

from __future__ import annotations

"""Strategy interfaces for Tagger metrics.

Protocols describe pure, deterministic, no-I/O strategy contracts. Use forward
references for domain types to avoid circular imports.
"""

from typing import Protocol, Any, Dict
import re


class TaggingSpeedStrategy(Protocol):
    """Strategy interface for tagging speed estimation.

    Inputs:
        tagger: "Tagger" (forward ref) containing tagassignments
    Outputs:
        float: estimated mean of log2(seconds) between tags

    Invariants:
        - Pure: no mutation, no I/O
        - Deterministic for same input
    """

    def speed_log2(self, tagger: "Tagger") -> float:  # pragma: no cover - interface
        ...


class AgreementStrategy(Protocol):
    """Strategy interface for pairwise agreement between taggers.

    Inputs:
        a, b: "Tagger"
        characteristic: "Characteristic"
    Outputs:
        float: agreement in [0.0, 1.0]

    Invariants:
        - Pure and deterministic
    """

    def pairwise(self, a: "Tagger", b: "Tagger", characteristic: "Characteristic") -> float:  # pragma: no cover - interface
        ...


class PatternSignalsStrategy(Protocol):
    """Strategy interface for pattern signal detection.

    Inputs:
        tagger: "Tagger"
    Outputs:
        dict: structured analysis results (pure JSON-serializable dict)

    Invariants:
        - Pure and deterministic
    """

    def analyze(self, tagger: "Tagger") -> Dict[str, int]:  # pragma: no cover - interface
        ...

    def build_sequence_str(self,  assignments: "list[TagAssignment]"):
        """Function takes in a list of tag assignments, and then returns a sequence string consisting of the first character of the tag value
        for each tag assignment

        The sequence string created allows for simpler pattern detection, since then the re module can be utilized.

        Args:
            assignments (List[TagAssignment]): _description_

        Returns:
            str: sequence_str in the format "YNYN" etc.
        """
        assignment_str = ""
        for assignment in assignments:
            if assignment.value == TagValue.YES:
                assignment_str += "Y"
            elif assignment.value == TagValue.NO:
                assignment_str += "N"
        
        return assignment_str
    
    
    def detect_pattern(self, pattern: str, assignment_sequence: str):
        """Function counts number of occurences of input pattern in the input assignment_sequence string

        Args:
            pattern (str): ordering of characters to check repetition for
            assignment_sequence (str): string representing the ordering of the tag values for a tag assignment

        Returns:
            int: number of times input pattern was repeated in the input assignment_sequence string
        """
        pattern_str = pattern.str

        # when pattern detected, jump to char that exists right after last letter in pattern
        list_patterns_found = re.findall(pattern=pattern_str, string=assignment_sequence)
        num_repeats = len(list_patterns_found)

        return num_repeats

