"""Core utilities for tag-level reports.

This module provides lightweight helper functions and a simple
dataclass used by higher-level tag reporting tools.  It intentionally
contains no I/O or CSV/export logic — those live in higher layers
of the reporting system.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple, Set, Optional

from qcc.domain.tagassignment import TagAssignment
from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.metrics.agreement import AgreementMetrics


@dataclass
class TagReportRow:
    comment_id: str
    characteristic_id: str
    num_taggers_could_set: int
    num_yes: int
    num_no: int
    krippendorffs_alpha: Optional[float]
    tag_quality: Optional[float] = None   # filled later by another component


def group_by_comment(assignments: List[TagAssignment]) -> Dict[str, List[TagAssignment]]:
    """Group assignments by comment_id (string key).

    This function focuses on IDs only — it will look for a ``comment_id``
    attribute on the assignment and fall back to an enriched ``comment.id``
    when available. Assignments missing any comment id are skipped.
    """

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


def group_by_comment_and_characteristic(
    assignments: List[TagAssignment],
) -> Dict[Tuple[str, str], List[TagAssignment]]:
    """Group assignments by (comment_id, characteristic_id) tuple of strings.

    This is explicitly ID-focused and does not attempt to create any
    placeholder domain objects. If either id is missing the assignment is
    skipped.
    """

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
    """Return set of tagger_id strings of taggers who tagged this comment.

    This function strictly returns IDs and does not construct or return
    Tagger objects.
    """

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
    """Return (#YES, #NO) based on TagValue.YES and TagValue.NO.

    Non-YES/NO values are ignored.
    """

    yes = 0
    no = 0

    for assignment in assignments or []:
        v = getattr(assignment, "value", None)
        if v == TagValue.YES:
            yes += 1
        elif v == TagValue.NO:
            no += 1

    return yes, no


def alpha_for_item(assignments: List[TagAssignment], characteristic: Characteristic) -> Optional[float]:
    """Return Krippendorff’s alpha for a single (comment, characteristic).

    Uses :class:`qcc.metrics.agreement.AgreementMetrics`. If fewer than two
    distinct taggers participated on this item the function returns ``None``
    to signify alpha is not defined.
    """

    if not assignments:
        return None

    # If fewer than 2 unique taggers, alpha is not meaningful
    taggers = taggers_who_touched_comment(assignments)
    if len(taggers) < 2:
        return None

    metrics = AgreementMetrics()
    return metrics.krippendorffs_alpha(assignments, characteristic)
