from __future__ import annotations
from typing import Any, Optional, Iterable, Dict, Set, Tuple
from collections import defaultdict
import datetime
import math

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger

class LatestLabelPercentAgreement:
    """
    Implements various agreement metrics, including Krippendorff's Alpha (nominal scale),
    by first resolving multiple assignments to use only the **latest, non-NA label**
    for each unique (Unit, Rater) pair.
    """
    @staticmethod
    def _prepare_alpha_matrix(all_assignments: Iterable[TagAssignment], char_id: str) -> Tuple[Set[TagValue], Dict[str, Dict[str, TagValue]]]:
        """
        Processes raw assignments to create the clean Unit x Rater matrix required for Alpha calculation.

        This function handles: filtering by characteristic, discarding NA/None labels, and
        selecting the single latest label when multiple exist for a (Unit, Rater) pair.

        Returns: (set of unique TagValue categories, {comment_id: {tagger_id: TagValue}})
        """
        # Dictionary to temporarily store the latest rating and its timestamp for each (Unit, Rater)
        # Default value is (min_datetime, None) to ensure the first valid assignment is always selected.
        # Structure: {comment_id: {tagger_id: (latest_timestamp, latest_value)}}
        latest_ratings_raw = defaultdict(lambda: defaultdict(lambda: (datetime.datetime.min, None)))
        
        # 1. Determine the latest non-NA rating for each Unit x Rater pair
        for assignment in all_assignments:
            # Filter: Skip assignments not relevant to the target characteristic
            if assignment.characteristic_id != char_id:
                continue
                
            # Filter: Skip Not Applicable (NA) or missing labels
            if assignment.value == TagValue.NA:
                continue
                
            tagger_id = assignment.tagger_id
            comment_id = assignment.comment_id
            
            # Retrieve the currently stored latest timestamp for this (Unit, Rater) pair
            current_time, current_value = latest_ratings_raw[comment_id][tagger_id]
            
            # Select latest: Check if the current assignment is STRICTLY newer, OR
            # if timestamps are equal, use the tagger_id for a deterministic tie-breaker.
            if assignment.timestamp > current_time or \
            (assignment.timestamp == current_time and tagger_id > str(current_value)):
                latest_ratings_raw[comment_id][tagger_id] = (assignment.timestamp, assignment.value)
        
        # 2. Extract final values and collect categories
        unit_rater_matrix = defaultdict(dict)
        categories: Set[TagValue] = set()
        
        # Convert the temporary (timestamp, value) map into the final (tagger_id: value) matrix
        for comment_id, tagger_map in latest_ratings_raw.items():
            for tagger_id, (time, value) in tagger_map.items():
                if value is not None:  # Extra safety check
                    unit_rater_matrix[comment_id][tagger_id] = value
                    categories.add(value)  # Collect the set of all unique categories used
                
        return categories, unit_rater_matrix

    # --- Pairwise Metric (Percent Agreement) ---
    
    def pairwise(self, tagger_a: Tagger, tagger_b: Tagger, char: Characteristic) -> float:
        """
        Calculates simple percent agreement between two taggers for a specific
        characteristic, using their latest assignments on overlapping units.
        """
        # 1. Combine all assignments from both taggers
        # Assumes Tagger object has a 'tagassignments' attribute
        all_assignments = (tagger_a.tagassignments or []) + (tagger_b.tagassignments or [])
        
        # 2. Prepare the matrix to get latest, valid ratings for this characteristic
        # The matrix is filtered to only include assignments by tagger_a and tagger_b
        _, matrix = LatestLabelPercentAgreement._prepare_alpha_matrix(all_assignments, char.id)
        
        total_agreed = 0
        total_compared = 0

        # 3. Iterate over units (comments) and calculate agreement
        for comment_id, ratings in matrix.items():
            # Get the latest rating for the current characteristic from each tagger
            a_value = ratings.get(tagger_a.id)
            b_value = ratings.get(tagger_b.id)
            
            # Check for overlap: both taggers must have a latest, non-NA rating for this unit
            if a_value is not None and b_value is not None:
                total_compared += 1
                if a_value == b_value:
                    total_agreed += 1  # Increment count if labels match

        if total_compared == 0:
            return 0.0

        # Percent agreement = (Agreed Units) / (Compared Units)
        return total_agreed / total_compared
    
    # --- Overall Metric (Krippendorff's Alpha) ---
    
    @classmethod
    def krippendorff_alpha(
        cls,
        all_assignments: Iterable[TagAssignment],
        char: Characteristic
    ) -> Optional[float]:
        """
        Compute Krippendorff's alpha (nominal data) using all provided assignments
        for a characteristic, selecting the latest non-NA label per (comment, tagger) pair.

        Formula: alpha = 1 - (D_o / D_e)
        """

        # 1. Prepare the clean Unit x Rater matrix and the set of categories
        categories, unit_rater_matrix = cls._prepare_alpha_matrix(all_assignments, char.id)

        # 2. Build category index
        unique_categories = sorted(list(categories), key=lambda x: x.value)
        category_to_index = {c: i for i, c in enumerate(unique_categories)}
        C = len(unique_categories)

        if len(unit_rater_matrix) == 0:
            return None  # No valid data

        # 3. Build coincidence matrix (symmetric)
        coincidence_matrix = [[0] * C for _ in range(C)]
        N = 0.0  # Total number of unordered pairs

        for ratings in unit_rater_matrix.values():
            valid_ratings = list(ratings.values())
            m_u = len(valid_ratings)
            if m_u < 2:
                continue

            # FIX: use unordered pairs, not ordered
            n_u = m_u * (m_u - 1) / 2
            N += n_u

            for i in range(m_u):
                for j in range(i + 1, m_u):
                    idx1 = category_to_index[valid_ratings[i]]
                    idx2 = category_to_index[valid_ratings[j]]
                    coincidence_matrix[idx1][idx2] += 1
                    coincidence_matrix[idx2][idx1] += 1

        if N == 0:
            return None

        # 4. Category counts (marginal frequencies)
        category_counts = defaultdict(int)
        for ratings in unit_rater_matrix.values():
            for value in ratings.values():
                category_counts[value] += 1

        n_c_totals = [category_counts.get(cat, 0) for cat in unique_categories]

        # 5. Observed and expected disagreement
        D_o_raw = sum(coincidence_matrix[i][j] for i in range(C) for j in range(C) if i != j)
        D_o_normalized = D_o_raw / N

        sum_of_squares_nc = sum(n_c ** 2 for n_c in n_c_totals)

        D_e_normalized = 0.0
        if N > 1:
            D_e_normalized = (N * N - sum_of_squares_nc) / (N * (N - 1))

        # 6. Compute alpha
        if D_e_normalized == 0.0:
            return 1.0 if D_o_normalized == 0.0 else 0.0

        alpha = 1 - (D_o_normalized / D_e_normalized)

        # Clamp result for stability
        alpha = max(-1.0, min(1.0, alpha))
        alpha = max(0.0, alpha)  # For standard reporting
        return round(alpha, 3)