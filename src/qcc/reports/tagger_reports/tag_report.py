"""Core utilities for tag-level reports."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple, Set, Optional

from qcc.domain.tagassignment import TagAssignment
from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.metrics.agreement import AgreementMetrics

import statistics


@dataclass
class TagReportRow:
    comment_id: str
    characteristic_id: str
    num_taggers_could_set: int
    num_yes: int
    num_no: int
    num_skipped: int
    cohens_kappa: Optional[float]
    krippendorffs_alpha: Optional[float]
    aggregate_tagger_reliability: Optional[float]
    tag_reliability_yes : Optional[float] 
    tag_reliability_no : Optional[float]

    def to_csv_row(self):
        return [
            self.comment_id,
            self.characteristic_id,
            self.num_taggers_could_set,
            self.num_yes,
            self.num_no,
            self.num_skipped,
            self.cohens_kappa,
            self.krippendorffs_alpha,
            self.aggregate_tagger_reliability,
            self.tag_reliability_yes,
            self.tag_reliability_no,
        ]


def group_by_comment(assignments: List[TagAssignment]) -> Dict[str, List[TagAssignment]]:
    groups: Dict[str, List[TagAssignment]] = defaultdict(list)

    for assignment in assignments or []:
        cid = getattr(assignment, "comment_id", None)
        if cid is None:
            comment_obj = getattr(assignment, "comment", None)
            cid = getattr(comment_obj, "id", None) if comment_obj is not None else None
        if cid is None:
            continue
        groups[str(cid)].append(assignment)

    return dict(groups)


def group_by_characteristic(assignments: List[TagAssignment]) -> Dict[str, List[TagAssignment]]:
    groups: Dict[str, List[TagAssignment]] = defaultdict(list)

    for assignment in assignments or []:
        char_id = getattr(assignment, "characteristic_id", None)
        if char_id is None:
            char_obj = getattr(assignment, "characteristic", None)
            char_id = getattr(char_obj, "id", None) if char_obj is not None else None

        if char_id is None:
            continue

        groups[str(char_id)].append(assignment)

    return dict(groups)


def group_by_comment_and_characteristic(
    assignments: List[TagAssignment],
) -> Dict[Tuple[str, str], List[TagAssignment]]:
    groups: Dict[Tuple[str, str], List[TagAssignment]] = defaultdict(list)

    for assignment in assignments or []:
        cid = getattr(assignment, "comment_id", None)
        if cid is None:
            comment_obj = getattr(assignment, "comment", None)
            cid = getattr(comment_obj, "id", None) if comment_obj is not None else None

        char_id = getattr(assignment, "characteristic_id", None)
        if char_id is None:
            char_obj = getattr(assignment, "characteristic", None)
            char_id = getattr(char_obj, "id", None) if char_obj is not None else None

        if cid is None or char_id is None:
            continue

        groups[(str(cid), str(char_id))].append(assignment)

    return dict(groups)


def taggers_who_touched_comment(assignments_for_comment: List[TagAssignment]) -> Set[str]:
    tagger_ids: Set[str] = set()

    for assignment in assignments_for_comment or []:
        tid = getattr(assignment, "tagger_id", None)
        if tid is None:
            tagger_obj = getattr(assignment, "tagger", None)
            tid = getattr(tagger_obj, "id", None) if tagger_obj is not None else None

        if tid is not None:
            tagger_ids.add(str(tid))

    return tagger_ids

def count_yes_no(assignments: List[TagAssignment]) -> Tuple[int, int]:
    yes = 0
    no = 0

    for assignment in assignments or []:
        v = getattr(assignment, "value", None)
        if v == TagValue.YES:
            yes += 1
        elif v == TagValue.NO:
            no += 1

    return yes, no

def alpha_for_item(
    assignments: List[TagAssignment],
    characteristic: Characteristic,
) -> Optional[float]:
    
    if not assignments:
        return None

    taggers = taggers_who_touched_comment(assignments)
    if len(taggers) < 2:
        return None

    metrics = AgreementMetrics()
    return metrics.krippendorffs_alpha(assignments, characteristic)

def kappa_for_item(
    assignments: List[TagAssignment],
    characteristic: Characteristic,
) -> Optional[float]:
    if not assignments:
        return None

    taggers = taggers_who_touched_comment(assignments)
    if len(taggers) < 2:
        return None

    metrics = AgreementMetrics()
    return metrics.cohens_kappa(assignments, characteristic)

def calculate_tag_reliability(
    assignments: List[TagAssignment],
    value: TagValue,
) -> Optional[float]:
    """
    Calculate tag reliability using a three-step approach:
    
    Step 1: Agreement Strength
        agreement = k / n
        where n = total number of taggers, k = number who assigned this tag
    
    Step 2: Worker Reliability Weighting
        For each worker, compute their global reliability as the mean of their
        pairwise agreement scores with all other workers.
        
        weighted_vote = sum(r_w for w in T) / sum(r_w for w in all_taggers)
        where T = workers who assigned the tag, r_w = worker w's reliability
    
    Step 3: Combine
        tag_reliability = agreement * weighted_vote
    
    Args:
        assignments: List of TagAssignment objects for the item
        value: TagValue to calculate reliability for (YES, NO, etc.)
        per_tagger_metric_cache: Optional cache of per-tagger metrics
    
    Returns:
        Reliability score between 0.0 and 1.0, or None if calculation fails
    """
    
    if not assignments:
        return None
    
    # Get all taggers who tagged this item
    all_taggers = taggers_who_touched_comment(assignments)
    n = len(all_taggers)
    
    if n == 0:
        return None
    
    # STEP 1: Calculate agreement strength
    # Identify taggers who assigned this specific tag value
    taggers_with_tag = {
        str(a.tagger_id)
        for a in assignments
        if a.value == value
    }
    k = len(taggers_with_tag)
    
    if k == 0:
        return 0.0
    
    # agreement = n / k
    agreement = k / n
    
    # STEP 2: Calculate worker reliability weights
    # Build reliability scores for each tagger based on pairwise agreement
    reliability: Dict[str, float] = {}
    
    # Get the characteristic ID from the first assignment
    char_id = getattr(assignments[0], "characteristic_id", None)
    if char_id is None:
        # Fallback: use equal weights for all taggers
        default_reliability = 1.0 / n if n > 0 else 0.0
        for tagger in all_taggers:
            reliability[tagger] = default_reliability
    else:
        # Create a Characteristic object for agreement calculations
        characteristic = Characteristic(str(char_id), str(char_id))
        
        # Compute pairwise agreement matrix
        metrics = AgreementMetrics()
        pairwise_matrix = metrics.agreement_matrix(assignments, characteristic)
        
        # For each tagger, compute their mean pairwise agreement with others
        for tagger in all_taggers:
            if tagger in pairwise_matrix:
                agreement_scores = [
                    score for other_tagger, score in pairwise_matrix[tagger].items()
                    if other_tagger != tagger  # Exclude self-agreement (1.0)
                ]
                
                if agreement_scores:
                    reliability[tagger] = statistics.mean(agreement_scores)
                else:
                    # Tagger has no other taggers to compare with
                    reliability[tagger] = 0.5
            else:
                # Tagger not in matrix, use neutral reliability
                reliability[tagger] = 0.5
    
    # STEP 3: Calculate weighted vote
    # Sum reliability of taggers who assigned this tag
    sum_reliability_with_tag = sum(
        reliability.get(tagger, 0.5)
        for tagger in taggers_with_tag
    )
    
    # Sum reliability of all taggers
    sum_reliability_all = sum(
        reliability.get(tagger, 0.5)
        for tagger in all_taggers
    )
    
    if sum_reliability_all == 0:
        return None
    
    # weighted_vote = sum(r_w for w in T) / sum(r_w for w in all_taggers)
    weighted_vote = sum_reliability_with_tag / sum_reliability_all
    
    # Combine: tag_reliability = agreement * weighted_vote
    tag_reliability = agreement * weighted_vote
    
    # Clamp between 0 and 1
    return max(0.0, min(1.0, tag_reliability))