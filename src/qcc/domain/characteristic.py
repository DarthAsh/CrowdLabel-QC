"""Characteristic domain model for crowd labeling quality control."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Optional

from qcc.domain.enums import TagValue

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
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
            object.__setattr__(self, 'domain', [TagValue.YES, TagValue.NO, TagValue.NA])
    
    def num_unique_taggers(self, tagassignments: Iterable["TagAssignment"]) -> int:
        """Count the number of unique taggers for this characteristic.
        
        Args:
            tagassignments: Iterable of tag assignments to filter
            
        Returns:
            Number of unique taggers who have tagged this characteristic
            
        Complexity: O(n) where n is the number of tag assignments
        """
        # TODO: Implement filtering and counting logic
        raise NotImplementedError("num_unique_taggers not yet implemented")
    
    def agreement_overall(self, tagassignments: Iterable["TagAssignment"]) -> float:
        """Calculate overall agreement for this characteristic.
        
        Args:
            tagassignments: Iterable of tag assignments to analyze
            
        Returns:
            Agreement score between 0.0 and 1.0
            
        Complexity: O(n²) where n is the number of tag assignments
        """
        # TODO: Implement agreement calculation logic
        raise NotImplementedError("agreement_overall not yet implemented")
    
    def prevalence(self, tagassignments: Iterable["TagAssignment"]) -> dict[TagValue, float]:
        """Calculate the prevalence of each tag value for this characteristic.
        
        Args:
            tagassignments: Iterable of tag assignments to analyze
            
        Returns:
            Dictionary mapping tag values to their prevalence (0.0 to 1.0)
            
        Complexity: O(n) where n is the number of tag assignments
        """
        # TODO: Implement prevalence calculation logic
        raise NotImplementedError("prevalence not yet implemented")

