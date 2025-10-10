# src/qcc/domain/tag.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Optional, Dict, List, Any

# Forward imports (to avoid circulars in skeletons)
# these live in: src/qcc/domain/...
if False:  # type: ignore
    from .comment import Comment
    from .characteristic import Characteristic
    from .tagassignment import TagAssignment
    from .enums import TagValue


@dataclass(slots=True)
class Tag:
    """
    Aggregate Tag (“Collection”) for one {comment , characteristic}.

    This models the *set* of TagAssignments made by multiple taggers for the same
    Comment and the same Characteristic. It is responsible for reporting per-item
    metrics such as agreement, consensus, prevalence, and value distributions.

    Notes
    -----
    - This is NOT a single tagger’s choice; that is a TagAssignment.
    - No business logic here—methods are placeholders that raise NotImplementedError.
    """

    id: str
    comment_id: str
    characteristic_id: str

    # Optional direct references (resolved elsewhere to avoid cycles in early scaffolding)
    comment: Optional["Comment"] = None
    characteristic: Optional["Characteristic"] = None

    # The raw inputs this aggregate summarizes
    assignments: List["TagAssignment"] = field(default_factory=list)

    # Optional cached/derived metadata (fill in by loaders or report builders)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    # Collection management 

    def add_assignment(self, assignment: "TagAssignment") -> None:
        """Add a TagAssignment to this aggregate."""
        if assignment.comment_id != self.comment_id or assignment.characteristic_id != self.characteristic_id:
            raise ValueError(
                "Assignment does not belong to this Tag (comment_id or characteristic_id mismatch)"
            )
        self.assignments.append(assignment)


    def extend_assignments(self, items: Iterable["TagAssignment"]) -> None:
        """Add multiple TagAssignments to this aggregate (reuses add_assignment validation)."""
        for item in items:
            self.add_assignment(item)


    def remove_assignment(self, assignment_id: str) -> None:
        """Remove a TagAssignment by a best-effort id lookup (supports common id attribute names)."""
        def _get_id(a):
            for attr in ("id", "assignment_id", "assignmentId", "uuid"):
                if hasattr(a, attr):
                    return getattr(a, attr)
            return None

        for i, a in enumerate(self.assignments):
            if _get_id(a) == assignment_id:
                del self.assignments[i]
                return

        raise KeyError(f"No TagAssignment found with id {assignment_id}")


    # Core metrics (per {comment x characteristic}) 

    def num_assignments(self) -> int:
        """Return count of TagAssignments attached."""
        return len(self.assignments)


    def num_unique_taggers(self) -> int:
        """Return number of distinct taggers who contributed assignments."""
        tagger_ids = set()
        for a in self.assignments:
            # common attribute names
            tid = getattr(a, "tagger_id", None) or getattr(a, "taggerId", None)
            # some implementations embed a tagger object
            if tid is None and hasattr(a, "tagger"):
                tid = getattr(a.tagger, "id", None) or getattr(a.tagger, "tagger_id", None)
            if tid is not None:
                tagger_ids.add(tid)
        return len(tagger_ids)

    def yesno_counts(self) -> Dict["TagValue", int]:
        """
        Return counts per TagValue across assignments.
        If the Characteristic provides a domain, include zero-count keys for completeness.
        Example: {TagValue.YES: 7, TagValue.NO: 3, TagValue.NA: 0}
        """
        # local import to avoid circulars at module import time
        from qcc.domain.enums import TagValue

        counts: Dict[TagValue, int] = {}
        # If characteristic object present and has a domain, prefill counts
        domain = None
        if self.characteristic is not None and hasattr(self.characteristic, "domain"):
            domain = getattr(self.characteristic, "domain")
            for v in domain:
                counts[v] = 0

        for a in self.assignments:
            val = getattr(a, "value", None)
            if val is None:
                continue
            if val in counts:
                counts[val] += 1
            else:
                # include unexpected values as well
                counts[val] = counts.get(val, 0) + 1

        return counts


    def yesno_proportions(self) -> Dict["TagValue", float]:
        """Return normalized proportions per TagValue (sum to 1.0 if any assignments exist)."""
        counts = self.value_counts()
        total = sum(counts.values())
        if total == 0:
            # preserve keys, return zeros
            return {k: 0.0 for k in counts}
        return {k: (counts[k] / total) for k in counts}


    def consensus_value(self) -> Optional["TagValue"]:
        """
        Return the majority (mode) TagValue if there is a unique winner, else None for a tie or no data.
        """
        counts = self.value_counts()
        if not counts:
            return None
        max_count = max(counts.values())
        winners = [v for v, c in counts.items() if c == max_count]
        if len(winners) == 1:
            return winners[0]
        return None


    def consensus_ratio(self) -> Optional[float]:
        """
        Return the fraction of assignments that match the consensus value.
        None if there are no assignments or if there is no unique consensus.
        """
        total = self.num_assignments()
        if total == 0:
            return None
        cv = self.consensus_value()
        if cv is None:
            return None
        counts = self.value_counts()
        return counts.get(cv, 0) / total


    def agreement_percent(self) -> Optional[float]:
        """
        this would be a class method, look into the name of this method.
        Pairwise percent agreement: (# agreeing pairs) / (# total pairs).
        Returns:
        - None if no assignments,
        - 1.0 if only one assignment (trivial full agreement).
        """
        n = self.num_assignments()
        if n == 0:
            return None
        if n == 1:
            return 1.0

        agree = 0
        total_pairs = 0
        for i in range(n):
            vi = getattr(self.assignments[i], "value", None)
            for j in range(i + 1, n):
                vj = getattr(self.assignments[j], "value", None)
                total_pairs += 1
                if vi == vj:
                    agree += 1
        return agree / total_pairs if total_pairs else None


    def krippendorff_alpha(self) -> Optional[float]:
        """
        Return Krippendorff’s alpha for this item (nominal domain typical).
        (Delegate to metrics layer; here just a placeholder.)
        """
        raise NotImplementedError

# re-think method
    # def prevalence(self) -> Optional[float]:
    #     """
    #     Return prevalence of the positive (or focal) TagValue for this item.
    #     (Exact mapping to TagValue decided by Characteristic/domain.)
    #     """
    #     raise NotImplementedError

    # Filters / projections 

    def assignments_by_tagger(self, tagger_id: str) -> List["TagAssignment"]:
        """Return assignments in this collection made by a specific tagger."""
        raise NotImplementedError

    # def assignments_by_time(self) -> List["TagAssignment"]:
    #     """Return assignments sorted by timestamp (ascending)."""
    #     raise NotImplementedError

    # Serialization 

    def to_dict(self) -> Dict[str, Any]:
        """Lightweight dictionary for reporting/JSON output."""
        raise NotImplementedError

    @classmethod
    def from_assignments(
        cls,
        id: str,
        comment_id: str,
        characteristic_id: str,
        assignments: Iterable["TagAssignment"],
        *,
        comment: Optional["Comment"] = None,
        characteristic: Optional["Characteristic"] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> "Tag":
        """
        Factory: build an aggregate Tag from a set of TagAssignments that all share the
        same {comment_id, characteristic_id}.
        """
        raise NotImplementedError
