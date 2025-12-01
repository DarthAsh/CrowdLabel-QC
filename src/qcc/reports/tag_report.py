"""Core utilities for tag-level reports.

This module provides lightweight helper functions and a simple
dataclass used by higher-level tag reporting tools.  It intentionally
contains no I/O or CSV/export logic — those live in higher layers
of the reporting system.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple, Set, Optional, Sequence

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

class TagMetricsReport:
    """ Class that generates a tag report with the following metrics
        A "tag" is defined as a unique pairing of a comment and a characteristic.

        # of taggers who could have set the tag
        # of taggers who said "Yes" for the tag
        # of taggers who said "No" for the tag
        krippendorff's alpha value for each tag
    """

    def __init__(self, assignments: Sequence[TagAssignment]) -> None:
        """Initialize the report with tag assignments."""

        self.assignments: List[TagAssignment] = list(assignments or [])

    def taggers_who_touched_comment(self, assignments_for_comment: List[TagAssignment]) -> Set[str]:
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
    
    def count_yes_no(self, assignments: List[TagAssignment]) -> Tuple[int, int]:
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


    def alpha_for_item(self, assignments: List[TagAssignment], characteristic: Characteristic) -> Optional[float]:
        """Return Krippendorff’s alpha for a single (comment, characteristic).

        Uses :class:`qcc.metrics.agreement.AgreementMetrics`. If fewer than two
        distinct taggers participated on this item the function returns ``None``
        to signify alpha is not defined.
        """

        if not assignments:
            return None

        # If fewer than 2 unique taggers, alpha is not meaningful
        taggers = self.taggers_who_touched_comment(assignments)
        if len(taggers) < 2:
            return None
        # TODO: this method does not have the functionality to group by comment and characteristic
        # it seems that the assumption is that the input tag assignments will belong to one tag (one comment and characteristic)
        # 

        metrics = AgreementMetrics()
        return metrics.krippendorffs_alpha(assignments, characteristic) 
   
    def generate_summary_report(self):
    # function that will do all the high level report generation tasks
    # collect all unique pairings of comments and characteristics, and the corresponding tag assignments
    # for each pairing:
        # compute number of taggers
        # compute number of taggers who said yes, no
        # compute krippendorff's alpha
        # create a TagReportRow object, append to list of TagReportRow objects

        tag_report_rows = []
        # return list of TagReportRow objects
        tags_and_assignments = self.group_by_comment_and_characteristic(self.assignments)
        for cur_tag, cur_assignments in tags_and_assignments.items():
            cid = cur_tag[0]
            char_id = cur_tag[1]
            n_taggers_all = self.taggers_who_touched_comment(cur_assignments)
            n_taggers_yes, n_taggers_no = self.count_yes_no(cur_assignments)
            krip_alpha = self.alpha_for_item(cur_assignments, char_id)

            report_row_obj = TagReportRow(cid, char_id, n_taggers_all, n_taggers_yes, n_taggers_no, krip_alpha)
            tag_report_rows.append(report_row_obj)

        return tag_report_rows
        # 

    def export_to_csv(self, report_data, output_path):
        # report_data is list of TagReportRow objects
        # build csv report headers
        # write header to file in output_path
        # write each tag report row to file in output_path
        pass

    def group_by_comment_and_characteristic(self, 
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









