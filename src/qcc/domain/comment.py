"""Comment domain model for crowd labeling quality control."""

from dataclasses import dataclass
from typing import List, Set

# Use forward references for Characteristic and TagAssignment
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from qcc.domain.characteristic import Characteristic
    from qcc.domain.tagassignment import TagAssignment

# We still reference Characteristic in the method signature, 
# so the original import structure (using TYPE_CHECKING) is best practice.


@dataclass(frozen=True)
class Comment:
    """A comment that has been labeled by crowd workers.
    
    A comment represents a single piece of text that has been
    evaluated by multiple taggers across different characteristics.
    
    Attributes:
        id: Unique identifier for the comment
        text: The actual text content of the comment
        prompt_id: Identifier of the prompt this comment belongs to
        tagassignments: List of all tag assignments for this comment
    """
    
    id: str
    text: str
    prompt_id: str
    tagassignments: List['TagAssignment']  # Type hint updated to use forward reference
    
    def __post_init__(self) -> None:
        """Validate the comment."""
        if not self.id:
            raise ValueError("comment id cannot be empty")
        if not self.text:
            raise ValueError("comment text cannot be empty")
        if not self.prompt_id:
            raise ValueError("prompt_id cannot be empty")
    
    
    def unique_taggers_all(self) -> Set[str]:
        """Get the set of unique taggers who have tagged this comment for ANY characteristic.
        
        Returns:
            Set of unique tagger IDs who have made assignments for this comment
            
        Complexity: O(n) where n is the number of tag assignments
        """
        # Logic updated to access tagger.id via the TagAssignment's object reference
        return {assignment.tagger.id for assignment in self.tagassignments}


    def unique_taggers_for(self, characteristic: 'Characteristic') -> Set[str]:
        """Count the number of unique Tagger objects for a specific characteristic.
        
        Args:
            characteristic: The characteristic to filter tag assignments by.
            
        Returns:
            Set of unique tagger IDs who have tagged this specific characteristic.
            
        Complexity: O(n) where n is the number of tag assignments
        """
        tagger_ids = {
            # Access Tagger ID through the object reference in TagAssignment
            assignment.tagger.id 
            for assignment in self.tagassignments
            # Filter by the Characteristic object reference in TagAssignment
            if assignment.characteristic.id == characteristic.id
        }
        return tagger_ids


    def agreement_for(self, characteristic: 'Characteristic') -> float:
        """Calculate inter-rater agreement among the Tags for a specific characteristic on this comment.
        
        Args:
            characteristic: The characteristic to calculate agreement for.
            
        Returns:
            Agreement score between 0.0 and 1.0 for this characteristic.
            
        Complexity: O(n²) where n is the number of taggers for this characteristic
        """
        # TODO: Implement characteristic-specific agreement calculation
        raise NotImplementedError("agreement_for not yet implemented")