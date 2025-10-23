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
        counts = self.yesno_counts()
        total = sum(counts.values())
        if total == 0:
            # preserve keys, return zeros
            return {k: 0.0 for k in counts}
        return {k: (counts[k] / total) for k in counts}


    def consensus_value(self) -> Optional["TagValue"]:
        """
        Return the majority (mode) TagValue if there is a unique winner, else None for a tie or no data.
        """
        counts = self.yesno_counts()
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
        counts = self.yesno_counts()
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


    def prevalence(self) -> Optional[float]:
        """
        Prevalence of the focal (positive) TagValue for this item.
        Behavior:
        - If `meta['focal_value']` exists, use that TagValue.
        - Else if characteristic defines a focal (not implemented), attempt to use it.
        - Else fall back to consensus_value (if any).
        - Return None if no assignment or no focal can be determined.
        """
        total = self.num_assignments()
        if total == 0:
            return None

        focal = None
        # check meta for explicit focal value
        if isinstance(self.meta, dict) and "focal_value" in self.meta:
            focal = self.meta["focal_value"]

        # fallback to consensus_value if meta not set
        if focal is None:
            focal = self.consensus_value()

        if focal is None:
            return None

        counts = self.yesno_counts()
        return counts.get(focal, 0) / total

    def krippendorff_alpha(self) -> Optional[float]:
        """
        Compute Krippendorff's alpha (nominal data) for this Tag's assignments.
        
        Krippendorff's alpha measures inter-rater agreement for categorical data, 
        accounting for agreement that might occur by chance. Values range from 0 to 1,
        where 1 indicates perfect agreement and 0 indicates no agreement beyond chance.
        
        Returns:
            float: Alpha value in [0, 1], rounded to 3 decimals, or
            None: If fewer than 2 valid assignments.
        """
        # Step 1: Check minimum data requirements
        # Krippendorff's alpha requires at least 2 assignments to measure agreement
        if len(self.assignments) < 2:
            return None

        from qcc.domain.enums import TagValue

        # Step 2: Extract valid values from assignments
        # Filter out any assignments with missing or None values
        values = [a.value for a in self.assignments if getattr(a, "value", None) is not None]
        n = len(values)
        
        # Double-check we still have enough valid data after filtering
        if n < 2:
            return None

        # Step 3: Analyze value distribution
        # Identify all unique tag values and count their occurrences
        unique_values = list(set(values))
        value_counts = {v: values.count(v) for v in unique_values}
        
        # Step 4: Calculate observed agreement (Po)
        # Count how many pairs of assignments have the same value
        # This measures the actual agreement between taggers
        observed_agreement = 0
        for i in range(n):
            for j in range(i + 1, n):
                if values[i] == values[j]:
                    observed_agreement += 1
        
        # Step 5: Calculate total possible pairs
        # For n items, there are n*(n-1)/2 unique pairs
        total_pairs = n * (n - 1) / 2
        if total_pairs == 0:
            return None
        
        # Step 6: Convert observed agreement to proportion
        # This gives us the probability that any two randomly selected assignments agree
        observed_agreement_prop = observed_agreement / total_pairs
        
        # Step 7: Calculate expected agreement (Pe)
        # This estimates the agreement we'd expect by random chance alone
        # For nominal data, it's the sum of squared marginal probabilities
        expected_agreement = 0.0
        for v in unique_values:
            p_v = value_counts[v] / n  # Probability of value v occurring
            expected_agreement += p_v * p_v  # Probability of two independent assignments both having value v
        
        # Step 8: Handle edge case - perfect value homogeneity
        # If all assignments have the same value, expected agreement is 1.0
        # In this case, we return 1.0 only if observed agreement is also perfect
        if abs(expected_agreement - 1.0) < 1e-10:
            return 1.0 if abs(observed_agreement_prop - 1.0) < 1e-10 else 0.0
        
        # Step 9: Compute Krippendorff's alpha
        # Alpha = (Observed Agreement - Expected Agreement) / (1 - Expected Agreement)
        # This formula adjusts observed agreement by removing chance agreement
        alpha = (observed_agreement_prop - expected_agreement) / (1 - expected_agreement)
        
        # Step 10: Clamp and format result
        # Alpha can theoretically be negative (worse than chance agreement) but for 
        # reporting purposes we interpret this as no agreement (0.0)
        # Round to 3 decimal places for consistent reporting
        alpha = max(0.0, min(1.0, alpha))
        return round(alpha, 3)


# re-think method
    # def prevalence(self) -> Optional[float]:
    #     """
    #     Return prevalence of the positive (or focal) TagValue for this item.
    #     (Exact mapping to TagValue decided by Characteristic/domain.)
    #     """
    #     raise NotImplementedError

    # Filters / projections 

    def assignments_for_tagger(self, tagger_id: str) -> List["TagAssignment"]:
        """Return assignments in this collection made by a specific tagger."""
        out: List["TagAssignment"] = []
        for a in self.assignments:
            tid = getattr(a, "tagger_id", None) or getattr(a, "taggerId", None)
            if tid is None and hasattr(a, "tagger"):
                tid = getattr(a.tagger, "id", None)
            if tid == tagger_id:
                out.append(a)
        return out

    def to_dict(self) -> Dict[str, Any]:
        """Lightweight dictionary for reporting/JSON output (TagValue keys become strings)."""
        vd = self.yesno_proportions()
        # Convert TagValue keys to strings for JSON friendliness
        vd_serializable = {str(k): v for k, v in vd.items()}
        cv = self.consensus_value()
        return {
            "id": self.id,
            "comment_id": self.comment_id,
            "characteristic_id": self.characteristic_id,
            "num_assignments": self.num_assignments(),
            "num_unique_taggers": self.num_unique_taggers(),
            "value_distribution": vd_serializable,
            "consensus_value": str(cv) if cv is not None else None,
            "consensus_ratio": self.consensus_ratio(),
            "agreement_percent": self.agreement_percent(),
            "meta": self.meta,
        }


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
        Build a Tag aggregate from assignments that must share {comment_id, characteristic_id}.
        """
        lst = []
        for a in assignments:
            if getattr(a, "comment_id", None) != comment_id or getattr(a, "characteristic_id", None) != characteristic_id:
                raise ValueError("Provided assignments must share the same comment_id and characteristic_id")
            lst.append(a)

        tag = cls(id=id, comment_id=comment_id, characteristic_id=characteristic_id)
        tag.assignments = list(lst)
        tag.comment = comment
        tag.characteristic = characteristic
        tag.meta = dict(meta) if meta else {}
        # set created_at from earliest assignment timestamp if possible
        try:
            times = [getattr(a, "timestamp") for a in lst if getattr(a, "timestamp", None) is not None]
            tag.created_at = min(times) if times else None
        except Exception:
            tag.created_at = None
        return tag

