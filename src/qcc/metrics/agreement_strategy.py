from __future__ import annotations
from typing import Any, Optional, Iterable, Dict, Set, Tuple
from collections import defaultdict
import datetime
import math

from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment

# --- Mocked/Assumed Domain Classes (for context) ---
# Note: These are placeholders; the actual implementation requires these classes to exist.
class Tagger:
    """Represents a human rater or tagging agent."""
    def get_assignments_for_characteristic(self, char: "Characteristic") -> list["Assignment"]: ...
    def __lt__(self, other: "Tagger") -> bool: ...

class Characteristic:
    """Represents the feature being tagged (e.g., 'Sentiment', 'Toxicity')."""
    pass

class Assignment:
    """Represents a single tagging action by a Tagger on a Unit."""
    comment_id: Any  # The Unit ID (e.g., the comment being rated)
    value: Any       # The assigned category (e.g., "Cat A", TagValue.YES)
    timestamp: datetime.datetime
    is_na: bool = False
# ------------------------------------------------------------


class LatestLabelPercentAgreement:
    """
    Implements various agreement metrics, including Krippendorff's Alpha (nominal scale),
    by first resolving multiple assignments to use only the **latest, non-NA label**
    for each unique (Unit, Rater) pair.
    """
    @staticmethod
    def _prepare_alpha_matrix(all_assignments: Iterable[TagAssignment], char_id: str) -> Tuple[Set[Any], Dict[str, Dict[str, Any]]]:
        """
        Processes raw assignments to create the clean Unit x Rater matrix required for Alpha calculation.

        This function handles: filtering by characteristic, discarding NA/None labels, and
        selecting the single latest label when multiple exist for a (Unit, Rater) pair.

        Returns: (set of unique categories, {comment_id: {tagger_id: category_value}})
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
            if assignment.value is None or assignment.value == TagValue.NA:
                continue
                
            tagger_id = assignment.tagger_id
            comment_id = assignment.comment_id
            
            # Retrieve the currently stored latest timestamp for this (Unit, Rater) pair
            current_time, current_value = latest_ratings_raw[comment_id][tagger_id]
            
            # Select latest: Check if the current assignment is STRICTLY newer, OR
            # if timestamps are equal, use the tagger_id for a deterministic tie-breaker.
            # NOTE: The tagger_id tie-breaker is comparing string tagger_id to the string value in the tuple.
            # This is non-standard but ensures a deterministic result if timestamps are identical.
            if assignment.timestamp > current_time or \
            (assignment.timestamp == current_time and tagger_id > current_value):
                latest_ratings_raw[comment_id][tagger_id] = (assignment.timestamp, assignment.value)
        
        # 2. Extract final values and collect categories
        unit_rater_matrix = defaultdict(dict)
        categories = set()
        
        # Convert the temporary (timestamp, value) map into the final (tagger_id: value) matrix
        for comment_id, tagger_map in latest_ratings_raw.items():
            for tagger_id, (time, value) in tagger_map.items():
                unit_rater_matrix[comment_id][tagger_id] = value
                categories.add(value) # Collect the set of all unique categories used
                
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
                    total_agreed += 1 # Increment count if labels match

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

        Formula: alpha = (D_e - D_o) / D_e
        """
        # Prepare the clean Unit x Rater matrix and the set of categories
        categories, unit_rater_matrix = LatestLabelPercentAgreement._prepare_alpha_matrix(all_assignments, char.id)
        
        # Prepare index mapping: essential for building the matrix and calculating D_e
        unique_categories = sorted(list(categories), key=str) # Sort for deterministic indexing
        category_to_index = {c: i for i, c in enumerate(unique_categories)}
        C = len(unique_categories) # Number of unique categories

        if len(unit_rater_matrix) == 0:
            return None # Cannot calculate Alpha if no units have valid ratings

        # Step 1: Build Coincidence Matrix (m_c,k) and calculate total pairs (N)
        # Coincidence Matrix stores the count of pairs (i, j) where rating i and rating j occurred
        # on the same unit.
        coincidence_matrix = [[0] * C for _ in range(C)]
        N = 0  # Total number of observable pairs across all units (sum of n_u)

        for ratings in unit_rater_matrix.values():
            valid_ratings = list(ratings.values())
            m_u = len(valid_ratings) # Number of raters who tagged this unit
            
            if m_u < 2:
                continue # A unit needs at least 2 ratings to form an agreement/disagreement pair
                
            n_u = m_u * (m_u - 1) # Number of unique ordered pairs for this unit
            N += n_u

            # Form all unique unordered pairs for this unit
            for i in range(m_u):
                for j in range(i + 1, m_u): # Starts at i + 1 to count each pair once
                    idx1 = category_to_index[valid_ratings[i]]
                    idx2 = category_to_index[valid_ratings[j]]
                    
                    # Increment coincidence count for both (idx1, idx2) and (idx2, idx1)
                    # This ensures the matrix is symmetrical.
                    coincidence_matrix[idx1][idx2] += 1
                    if idx1 != idx2:
                         coincidence_matrix[idx2][idx1] += 1
        
        if N == 0:
            return None # Cannot calculate if no units had overlapping valid ratings
            
        # Step 2: Calculate Marginal Frequencies (n_c) and Disagreements
        
        # 1. Initialize category counts
        category_counts = defaultdict(int)
        
        # 2. Calculate the true marginal frequency (n_c): total count of ratings for each category.
        # This is the most robust way to calculate marginals (row sums of the raw data matrix).
        for ratings in unit_rater_matrix.values():
            for value in ratings.values():
                category_counts[value] += 1
                
        # 3. Create the n_c array matching the unique_categories order
        n_c_totals = [category_counts.get(cat, 0) for cat in unique_categories]
        
        # Observed Disagreement (D_o_raw): Sum of all off-diagonal cells
        # This is the count of all observed disagreements across all units.
        D_o_raw = sum(coincidence_matrix[i][j] for i in range(C) for j in range(C) if i != j)
        D_o_normalized = D_o_raw / N # P_o (Observed Agreement Proportion) = 1 - D_o_normalized
        
        # Expected Disagreement (D_e_normalized)
        # D_e = (N^2 - sum(n_c^2)) / (N * (N - 1))
        # This represents the disagreement expected by chance.
        sum_of_squares_nc = sum(n_c**2 for n_c in n_c_totals)
        
        D_e_normalized = 0.0
        if N > 1:
            D_e_normalized = (N * N - sum_of_squares_nc) / (N * (N - 1))
        
        # Step 3: Compute Alpha: alpha = (D_e - D_o) / D_e
        if D_e_normalized == 0.0:
            # If expected disagreement is 0, it means all marginals are zero except one (perfect homogeneity)
            # Alpha is 1.0 only if observed disagreement is also 0 (perfect agreement).
            return 1.0 if D_o_normalized == 0.0 else 0.0
            
        alpha = (D_e_normalized - D_o_normalized) / D_e_normalized

        # Step 4: Clamp and format result
        alpha = max(-1.0, alpha) # Keep negative for analytical purposes (worse than chance)
        alpha = max(0.0, alpha)  # Clamp to 0.0 for standard reporting (common practice)
        return round(alpha, 3)