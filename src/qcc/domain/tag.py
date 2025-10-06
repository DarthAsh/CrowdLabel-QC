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
        raise NotImplementedError

    def extend_assignments(self, items: Iterable["TagAssignment"]) -> None:
        """Add multiple TagAssignments to this aggregate."""
        raise NotImplementedError

    def remove_assignment(self, assignment_id: str) -> None:
        """Remove a TagAssignment by its id."""
        raise NotImplementedError

    # Core metrics (per {comment x characteristic}) 

    def num_assignments(self) -> int:
        """Return count of TagAssignments attached."""
        raise NotImplementedError

    def num_unique_taggers(self) -> int:
        """Return number of distinct taggers who contributed assignments."""
        raise NotImplementedError

    def value_counts(self) -> Dict["TagValue", int]:
        """
        Return counts per TagValue across assignments.

        Example: {TagValue.YES: 7, TagValue.NO: 3, TagValue.NA: 0}
        """
        raise NotImplementedError

    def value_distribution(self) -> Dict["TagValue", float]:
        """
        Return normalized proportions per TagValue (sum to 1.0 when any data exists).
        """
        raise NotImplementedError

    def consensus_value(self) -> Optional["TagValue"]:
        """
        Return the majority (mode) TagValue if there is one, else None (e.g., tie).
        """
        raise NotImplementedError

    def consensus_ratio(self) -> Optional[float]:
        """
        Return the fraction of assignments that match the consensus value.
        Example: 0.67 for 2/3 agreement. None if no assignments.
        """
        raise NotImplementedError

    def agreement_percent(self) -> Optional[float]:
        """
        Return simple percent agreement among taggers for this item.
        (Signature only; actual definition chosen by metrics module.)
        """
        raise NotImplementedError

    def krippendorff_alpha(self) -> Optional[float]:
        """
        Return Krippendorff’s alpha for this item (nominal domain typical).
        (Delegate to metrics layer; here just a placeholder.)
        """
        raise NotImplementedError

    def prevalence(self) -> Optional[float]:
        """
        Return prevalence of the positive (or focal) TagValue for this item.
        (Exact mapping to TagValue decided by Characteristic/domain.)
        """
        raise NotImplementedError

    # Filters / projections 

    def assignments_for_tagger(self, tagger_id: str) -> List["TagAssignment"]:
        """Return assignments in this collection made by a specific tagger."""
        raise NotImplementedError

    def assignments_by_time(self) -> List["TagAssignment"]:
        """Return assignments sorted by timestamp (ascending)."""
        raise NotImplementedError

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
