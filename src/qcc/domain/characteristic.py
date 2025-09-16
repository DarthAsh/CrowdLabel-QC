"""Characteristic domain model for crowd labeling quality control."""

from dataclasses import dataclass
from typing import Iterable, List, Optional

from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment


@dataclass(frozen=True)
class Characteristic:
    """A characteristic that can be labeled by crowd workers.
    
    A characteristic represents a specific aspect or dimension that
    can be evaluated in comments, such as sentiment, topic, or quality.
    
    Attributes:
        id: Unique identifier for the characteristic
        name: Human-readable name of the characteristic
        description: Optional detailed description
        domain: List of valid tag values for this characteristic
    """
    
    id: str
    name: str
    description: Optional[str] = None
    domain: List[TagValue] = None
    
    def __post_init__(self) -> None:
        """Initialize default domain if not provided."""
        if self.domain is None:
            object.__setattr__(
                self,
                "domain",
                [
                    TagValue.YES,
                    TagValue.NO,
                    TagValue.NA,
                    TagValue.UNCERTAIN,
                    TagValue.SKIP,
                ],
            )
    
    def num_unique_taggers(self, tagassignments: Iterable[TagAssignment]) -> int:
        """Count the number of unique taggers for this characteristic."""
        unique_taggers = {
            ta.tagger_id for ta in tagassignments if ta.characteristic_id == self.id
        }
        return len(unique_taggers)
    
    def agreement_overall(self, tagassignments: Iterable[TagAssignment]) -> float:
        """Calculate overall agreement for this characteristic.
        
        Agreement = (# agreeing pairs) / (# total pairs).
        Returns 0.0 if fewer than 2 assignments exist.
        """
        filtered = [ta for ta in tagassignments if ta.characteristic_id == self.id]
        n = len(filtered)
        if n < 2:
            return 0.0
        
        agree_pairs = 0
        total_pairs = 0
        for i in range(n):
            for j in range(i + 1, n):
                total_pairs += 1
                if filtered[i].value == filtered[j].value:
                    agree_pairs += 1
        
        return agree_pairs / total_pairs if total_pairs > 0 else 0.0
    
    def prevalence(self, tagassignments: Iterable[TagAssignment]) -> dict[TagValue, float]:
        """Calculate the prevalence of each tag value for this characteristic."""
        filtered = [ta for ta in tagassignments if ta.characteristic_id == self.id]
        total = len(filtered)
        if total == 0:
            return {value: 0.0 for value in self.domain}
        
        counts: dict[TagValue, int] = {value: 0 for value in self.domain}
        for ta in filtered:
            counts[ta.value] += 1
        
        return {value: counts[value] / total for value in self.domain}
