from __future__ import annotations
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Set, Tuple
from collections import Counter, defaultdict
from itertools import combinations
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

    # --- Aggregate metrics -------------------------------------------------

    def percent_agreement(
        self, assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> float:
        """Return overall percent agreement for the latest labels."""

        _, matrix = self._prepare_alpha_matrix(assignments, characteristic.id)
        return self._pairwise_agreement_from_matrix(matrix)

    def cohens_kappa(
        self, assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> float:
        """Average Cohen's kappa across all tagger pairs."""

        _, matrix = self._prepare_alpha_matrix(assignments, characteristic.id)
        tagger_ids = self._ordered_tagger_ids(matrix)
        if len(tagger_ids) < 2:
            return 0.0

        kappas = [
            self._cohens_kappa_for_pair(matrix, a_id, b_id)
            for a_id, b_id in combinations(tagger_ids, 2)
        ]
        kappas = [score for score in kappas if score is not None]
        if not kappas:
            return 0.0
        return float(sum(kappas) / len(kappas))

    def agreement_matrix(
        self, assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> Dict[str, Dict[str, float]]:
        """Return a symmetric matrix of pairwise percent agreements."""

        _, matrix = self._prepare_alpha_matrix(assignments, characteristic.id)
        tagger_ids = self._ordered_tagger_ids(matrix)
        if not tagger_ids:
            return {}

        agreement: Dict[str, Dict[str, float]] = {tid: {} for tid in tagger_ids}
        for tid in tagger_ids:
            agreement[tid][tid] = 1.0

        for a_id, b_id in combinations(tagger_ids, 2):
            score = self._pairwise_agreement_for_ids(matrix, a_id, b_id)
            agreement[a_id][b_id] = score
            agreement[b_id][a_id] = score

        return agreement

    def per_tagger_metrics(
        self,
        assignments: Iterable[TagAssignment],
        characteristic: Characteristic,
        methods: Sequence[str],
    ) -> Dict[str, Dict[str, float]]:
        """Return requested agreement metrics averaged per tagger."""

        _, matrix = self._prepare_alpha_matrix(assignments, characteristic.id)
        tagger_ids = self._ordered_tagger_ids(matrix)
        if not tagger_ids:
            return {}

        requested_methods = set(methods)
        compute_percent = "percent_agreement" in requested_methods
        compute_kappa = "cohens_kappa" in requested_methods

        percent_cache: Dict[Tuple[str, str], float] = {}
        kappa_cache: Dict[Tuple[str, str], Optional[float]] = {}

        if compute_percent or compute_kappa:
            for a_id, b_id in combinations(tagger_ids, 2):
                if compute_percent:
                    score = self._pairwise_agreement_for_ids(matrix, a_id, b_id)
                    percent_cache[(a_id, b_id)] = score
                    percent_cache[(b_id, a_id)] = score
                if compute_kappa:
                    score = self._cohens_kappa_for_pair(matrix, a_id, b_id)
                    kappa_cache[(a_id, b_id)] = score
                    kappa_cache[(b_id, a_id)] = score

        per_tagger: Dict[str, Dict[str, float]] = {}
        for tagger_id in tagger_ids:
            peers = [other for other in tagger_ids if other != tagger_id]
            tagger_entry: Dict[str, float] = {}

            if compute_percent:
                peer_scores = [
                    percent_cache.get((tagger_id, other))
                    for other in peers
                    if (tagger_id, other) in percent_cache
                ]
                if peer_scores:
                    tagger_entry["percent_agreement"] = sum(peer_scores) / len(peer_scores)
                else:
                    tagger_entry["percent_agreement"] = 0.0

            if compute_kappa:
                peer_scores = [
                    kappa_cache.get((tagger_id, other))
                    for other in peers
                    if (tagger_id, other) in kappa_cache
                ]
                cleaned_scores = [score for score in peer_scores if score is not None]
                if cleaned_scores:
                    tagger_entry["cohens_kappa"] = sum(cleaned_scores) / len(cleaned_scores)
                else:
                    tagger_entry["cohens_kappa"] = 0.0

            if tagger_entry:
                per_tagger[tagger_id] = tagger_entry

        return per_tagger

    # --- Pairwise Metric (Percent Agreement) ---

    def pairwise(self, tagger_a: Tagger, tagger_b: Tagger, char: Characteristic) -> float:
        """Calculate percent agreement between two taggers."""

        all_assignments = (tagger_a.tagassignments or []) + (tagger_b.tagassignments or [])
        _, matrix = self._prepare_alpha_matrix(all_assignments, char.id)
        return self._pairwise_agreement_for_ids(matrix, tagger_a.id, tagger_b.id)
    
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

        categories, unit_rater_matrix = cls._prepare_alpha_matrix(all_assignments, char.id)
        return cls._krippendorffs_alpha_from_matrix(categories, unit_rater_matrix)

    # --- Shared helpers ----------------------------------------------------

    @staticmethod
    def _ordered_tagger_ids(
        matrix: Mapping[str, Mapping[str, TagValue]]
    ) -> Sequence[str]:
        tagger_ids = {
            tagger_id
            for ratings in matrix.values()
            for tagger_id in ratings
        }
        return sorted(tagger_ids)

    @staticmethod
    def _pairwise_agreement_from_matrix(
        matrix: Mapping[str, Mapping[str, TagValue]]
    ) -> float:
        total_pairs = 0
        agreeing_pairs = 0

        for ratings in matrix.values():
            values = list(ratings.values())
            if len(values) < 2:
                continue
            for idx in range(len(values)):
                for jdx in range(idx + 1, len(values)):
                    total_pairs += 1
                    if values[idx] == values[jdx]:
                        agreeing_pairs += 1

        if total_pairs == 0:
            return 0.0
        return agreeing_pairs / total_pairs

    @staticmethod
    def _pairwise_agreement_for_ids(
        matrix: Mapping[str, Mapping[str, TagValue]],
        tagger_a: str,
        tagger_b: str,
    ) -> float:
        total = 0
        agree = 0
        for ratings in matrix.values():
            a_value = ratings.get(tagger_a)
            b_value = ratings.get(tagger_b)
            if a_value is None or b_value is None:
                continue
            total += 1
            if a_value == b_value:
                agree += 1
        if total == 0:
            return 0.0
        return agree / total

    @staticmethod
    def _cohens_kappa_for_pair(
        matrix: Mapping[str, Mapping[str, TagValue]],
        tagger_a: str,
        tagger_b: str,
    ) -> Optional[float]:
        pairs = []
        for ratings in matrix.values():
            a_value = ratings.get(tagger_a)
            b_value = ratings.get(tagger_b)
            if a_value is None or b_value is None:
                continue
            pairs.append((a_value, b_value))

        if not pairs:
            return None

        total = len(pairs)
        observed_agreements = sum(1 for a_value, b_value in pairs if a_value == b_value)
        p_o = observed_agreements / total

        counts_a: Counter[TagValue] = Counter()
        counts_b: Counter[TagValue] = Counter()
        for a_value, b_value in pairs:
            counts_a[a_value] += 1
            counts_b[b_value] += 1

        p_e = 0.0
        for value in set(counts_a.keys()) | set(counts_b.keys()):
            p_e += (counts_a[value] / total) * (counts_b[value] / total)

        denominator = 1.0 - p_e
        if denominator == 0.0:
            return 1.0 if p_o == 1.0 else 0.0
        return (p_o - p_e) / denominator

    @classmethod
    def _krippendorffs_alpha_from_matrix(
        cls,
        categories: Set[TagValue],
        unit_rater_matrix: Mapping[str, Mapping[str, TagValue]],
    ) -> Optional[float]:
        if not unit_rater_matrix:
            return None

        ordered_categories = sorted(categories, key=lambda value: value.value)
        if not ordered_categories:
            return None

        category_to_index = {value: idx for idx, value in enumerate(ordered_categories)}
        category_count = len(ordered_categories)

        coincidence_matrix = [[0] * category_count for _ in range(category_count)]
        total_pairs = 0.0

        for ratings in unit_rater_matrix.values():
            values = list(ratings.values())
            if len(values) < 2:
                continue
            total_pairs += len(values) * (len(values) - 1) / 2
            for idx in range(len(values)):
                for jdx in range(idx + 1, len(values)):
                    a_idx = category_to_index[values[idx]]
                    b_idx = category_to_index[values[jdx]]
                    coincidence_matrix[a_idx][b_idx] += 1
                    coincidence_matrix[b_idx][a_idx] += 1

        if total_pairs == 0:
            return None

        category_totals: Counter[TagValue] = Counter()
        for ratings in unit_rater_matrix.values():
            for value in ratings.values():
                category_totals[value] += 1

        observed_disagreement = sum(
            coincidence_matrix[i][j]
            for i in range(category_count)
            for j in range(category_count)
            if i != j
        )
        observed_disagreement /= total_pairs

        sum_of_squares = sum(count ** 2 for count in category_totals.values())
        expected_disagreement = 0.0
        if total_pairs > 1:
            expected_disagreement = (total_pairs * total_pairs - sum_of_squares) / (
                total_pairs * (total_pairs - 1)
            )

        if expected_disagreement == 0.0:
            return 1.0 if observed_disagreement == 0.0 else 0.0

        alpha = 1 - (observed_disagreement / expected_disagreement)
        alpha = max(-1.0, min(1.0, alpha))
        alpha = max(0.0, alpha)
        return round(alpha, 3)
