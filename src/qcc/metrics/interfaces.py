"""Strategy interfaces for Tagger metrics.

Protocols describe pure, deterministic, no-I/O strategy contracts. Use forward
references for domain types to avoid circular imports.
"""
from itertools import product
from __future__ import annotations
from collections import defaultdict
from typing import Dict, Iterable, Protocol
import re

from ..domain.enums import TagValue
from .utils.pattern import PatternCollection


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

    def build_sequence_str(self, assignments: "list[TagAssignment]") -> str:
        """Function takes in a list of tag assignments, and then returns a sequence string consisting of the first character of the tag value
        for each tag assignment

        The sequence string created allows for simpler pattern detection, since then the re module can be utilized.

        Args:
            assignments (List[TagAssignment]): _description_

        Returns:
            str: sequence_str in the format "YNYN" etc.
        """
        tokens: list[str] = []
        for assignment in assignments:
            value = getattr(assignment, "value", None)
            if value == TagValue.YES:
                tokens.append("Y")
            elif value == TagValue.NO:
                tokens.append("N")

        return "".join(tokens)
    
    
    def count_pattern_repetition(self, pattern: str, assignment_sequence: str) -> int:
        """Function counts number of occurences of input pattern in the input assignment_sequence string

        Args:
            pattern (str): ordering of characters to check repetition for
            assignment_sequence (str): string representing the ordering of the tag values for a tag assignment

        Returns:
            int: number of times input pattern was repeated in the input assignment_sequence string
        """
        pattern_str = str(pattern)
        if not pattern_str:
            return 0

        # when pattern detected, jump to char that exists right after last letter in pattern
        list_patterns_found = re.findall(pattern=re.escape(pattern_str), string=assignment_sequence)
        num_repeats = len(list_patterns_found)

        return num_repeats
    
    def canonical_rotation(self, pattern):
        rotations = [pattern[i:] + pattern[:i] for i in range(len(pattern))]
        return min(rotations)
    
    def generate_pattern_frequency(
        self, tag_assignments: Iterable["TagAssignment"]
    ) -> Dict[str, int]:
        """Function that creates a dictionary representing the number of times each pattern in PatternCollection is repeated
        in tag_assignments

        Args:
            tag_assignments (list(TagAssignment)): list of TagAssignment objects to determine various pattern occurrences for

        Returns:
            Dict[str, int]: A dictionary such that the key represents a pattern, and the value represents the frequency of its occurrences.
        """
        assignment_sequence = self.build_sequence_str(tag_assignments)

        atomic_patterns = ["Y", "N"]
        

        patterns = {1: atomic_patterns}

        # repeated cartesian product to create exhaustive list of patterns
        for pattern_length in range(2, 5):
            cur_patterns = []
            
            for bits in product('YN', repeat = pattern_length):
                cur_pattern_str = ''.join(bits)
                cur_patterns.append(cur_pattern_str)

            patterns[pattern_length] = cur_patterns

        patterns_to_remove = {}
        # Discard larger patterns (of length 4 and 3), that have at least one smaller pattern (length 3 or length 2) as a building block. Patterns of length 1 will be ignored, since these are atomic, so they will always be building blocks.
        for key in sorted(patterns.keys(), reverse=True):
            if key > 2:
                cur_patterns = patterns[key]
                smaller_patterns = []
                patterns_to_discard = []

                for i in range(2, key):
                    smaller_patterns.extend(patterns[i])
                
                for pattern in cur_patterns:
                    if any(small_pattern in pattern for small_pattern in smaller_patterns):
                        patterns_to_discard.append(pattern)
                patterns_to_remove[key] = patterns_to_discard
            else:
                break

        for key, to_delete in patterns_to_remove.items():
            patterns[key] = [p for p in patterns[key] if p not in to_delete]

        
        pattern_frequency = {}

        # For each remaining pattern, count repetition within tag assignment sequence.
        for key, pattern_list in patterns.items():
            patterns_count = {}
            for pattern in pattern_list:
                patterns_count[pattern] = self.count_pattern_repetition(pattern, assignment_sequence)
            pattern_frequency[key] = patterns_count

        rot_groups = defaultdict(list)
        # Next, identify patterns that are rotations of each other, and group those together - we will call these rotation groups. 
        for patterns_count in pattern_frequency.values():
            for pattern in patterns_count:
                canon = self.canonical_rotation(pattern)
                rot_groups[canon].append(pattern)

        max_pattern_counts = {}
        all_pattern_counts = {}
        for counts in pattern_frequency.values():
            all_pattern_counts.update(counts)

        for canon, patterns in rot_groups.items():        
            max_pattern = max(patterns, key=lambda p: all_pattern_counts[p])
            max_count = all_pattern_counts[max_pattern]
            max_pattern_counts[max_pattern] = max_count

        # For each rotation group, find pattern with max  occurrences, and create a key-value pair => {"patternWithMaxOccurrence": # of occurrences}. Append this key-value pair to pattern_frequency map.
        return max_pattern_counts
        # Output pattern_frequency map
        
