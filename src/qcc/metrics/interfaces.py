"""Strategy interfaces for Tagger metrics.

Protocols describe pure, deterministic, no-I/O strategy contracts. Use forward
references for domain types to avoid circular imports.
"""
from __future__ import annotations
from itertools import product
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
    
    # Non-overlapping patterns are found
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
        self, tag_assignments: Iterable["TagAssignment"], substring_length = 12
    ) -> Dict[str, int]:
        """Function that creates a dictionary representing the number of times each pattern in PatternCollection is repeated
        in tag_assignments

        Args:
            tag_assignments (list(TagAssignment)): list of TagAssignment objects to determine various pattern occurrences for

        Returns:
            Dict[str, int]: A dictionary such that the key represents a pattern, and the value represents the frequency of its occurrences.
        """

        # New algo
        # For each position in sequence, take that tag assignemnt and the next 11
        # Within this substring, determine whether there are length-4 patterns that are continously repeated (3x)
        # determine whether there are length-3 patterns that are continously repeated (4x)
        # For a suffix tree, the number of leaves for a given node are the number of repeats.
        # Given a suffix tree for length 12 substring, we look at the leaves of nodes with height 4 or more(top to bottom),
        # and if there are 3 leaves of the height 4 node, contiguous repeats of length-4 pattern have been found
        # add length-4 pattern to track, and "1", where 1 refers to 1 contigious 12-length repeat.

        # For length-3, we look at the leaves of nodes with height 3 or more (top to bottom),
        # and if there are 4 leaves of the height 3 node, contiguous repeats of length-3 pattern have been found
        # add length-3 pattern to track, and "1", where 1 refers to 1 contiguous 12-length repeat

        # Repeat for each 12-length substring

        # before final output, add up all occurrences of cyclic equivalence of each pattern?
        # slightly confused about 1-off overlaps...maybe cyclic equivalence takes care of that? In that case, occurrences shouldn't be added up.

        # while building suffix tree, each node could have
        #  - a height field, which would get updated every time a new suffix longer than existing suffixes starting with that node is found
        #  - an n_leaves field, which would get updated every time a new suffix starting with that node is found
        #  then searching for length-4 patterns would essentially be looking at the children of the root node,
        #  check each of the children node's height, if height is >= 4, then check n_leaves of the 4th node = 3. If yes,
        # length-4 pattern has been found, and stop search - since no other length-4 pattern occurrences will be possible
        # 

        # 2nd algo:
        # I don't think the sliding window approach would be accurate - how do you account for the window "sliding" - where patterns are detected in each window?
        # Maybe a second approach could be to build a suffix tree of the entire sequence of tag assignments - if Ukkonen's algo can be used, the time complexity would not be awful
        # although the space complexity could be O(n^2)
        # then after creating the suffix tree, we look at all paths with nodes >= 4 for length-4 patterns, and check what proportion of the indices are contiguous.
        # for length-3 patterns, look at all paths with nodes >= 3, and check what proportion of the indices are contiguous
        # maybe we can define proportion dependent on what the result of N/12 is, where N is the total number of tags in sequence
        # N/12 would tell us how many max blocks of size 12 there would be if starting from index 0 (non-sliding window)
        # if there are 24 tags, then there are 2 blocks each of 12 tags, but what does that mean?
        # 
        
        assignment_sequence = list(self.build_sequence_str(tag_assignments))
        sub_start = 0
        track_4 = defaultdict(list)

        while sub_start < len(assignment_sequence) - (substring_length - 1):
            cur_sub = "".join(assignment_sequence[sub_start : sub_start + substring_length])
        
            first_pattern = cur_sub[0:4]
            # expected = first_pattern * 3
            expected = first_pattern * (substring_length // len(first_pattern))
            if cur_sub == expected:
                track_4[first_pattern].append(sub_start)
                sub_start += substring_length
            else:
                sub_start += 1
        
        for length_4_occurrences in track_4.values():
            for start_pos in length_4_occurrences:
                assignment_sequence[start_pos: start_pos + substring_length] = "#"
        
        sub_start = 0
        track_3 = defaultdict(list)

        while sub_start < len(assignment_sequence) - (substring_length - 1):
            cur_sub = "".join(assignment_sequence[sub_start : sub_start + substring_length])
            if "#" in cur_sub:
                sub_start += substring_length
                continue
            else:
                first_pattern = cur_sub[0:3]
                # expected = first_pattern * 4
                expected = first_pattern * (substring_length // len(first_pattern))
                if cur_sub == expected:
                    track_3[first_pattern].append(sub_start)
                    sub_start += substring_length
                else:
                    sub_start += 1
        
        all_detected_patterns = {}
        for pattern, occurences in track_4.items():
            all_detected_patterns[pattern] = len(occurences)
        for pattern, occurences in track_3.items():
            all_detected_patterns[pattern] = len(occurences)

        return all_detected_patterns
        
        # assignment_sequence = self.build_sequence_str(tag_assignments)

        # atomic_patterns = ["Y", "N"]
        

        # patterns = {1: atomic_patterns}

        # # repeated cartesian product to create exhaustive list of patterns
        # for pattern_length in range(2, 5):
        #     cur_patterns = []
            
        #     for bits in product('YN', repeat = pattern_length):
        #         cur_pattern_str = ''.join(bits)
        #         cur_patterns.append(cur_pattern_str)

        #     patterns[pattern_length] = cur_patterns

        # patterns_to_remove = {}

        # for key, to_delete in patterns_to_remove.items():
        #     patterns[key] = [p for p in patterns[key] if p not in to_delete]

        
        # pattern_frequency = {}

        # # For each remaining pattern, count repetition within tag assignment sequence.
        # for key, pattern_list in patterns.items():
        #     patterns_count = {}
        #     for pattern in pattern_list:
        #         patterns_count[pattern] = self.count_pattern_repetition(pattern, assignment_sequence)
        #     pattern_frequency[key] = patterns_count

        # rot_groups = defaultdict(list)
        # # Next, identify patterns that are rotations of each other, and group those together - we will call these rotation groups. 
        # for patterns_count in pattern_frequency.values():
        #     for pattern in patterns_count:
        #         canon = self.canonical_rotation(pattern)
        #         rot_groups[canon].append(pattern)

        # max_pattern_counts = {}
        # all_pattern_counts = {}

        # for counts in pattern_frequency.values():
        #     all_pattern_counts.update(counts)

        # for canon, patterns in rot_groups.items():      
        #     max_pattern = max(patterns, key=lambda p: all_pattern_counts[p])

        #     # TODO - what if there are multiple max patterns in one cyclic rotation? Currently, the first of the maxes will be printed.
        #     # For each rotation group, find pattern with max  occurrences, and create a key-value pair => {"patternWithMaxOccurrence": # of occurrences}. Append this key-value pair to pattern_frequency map.
        #     max_count = all_pattern_counts[max_pattern]

        #     if max_count > 0:
        #         max_pattern_counts[max_pattern] = max_count

        # # Output pattern_frequency map
        # return max_pattern_counts
        
