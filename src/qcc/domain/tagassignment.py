"""TagAssignment domain model for crowd labeling quality control."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from qcc.domain.tagger import Tagger
from qcc.domain.comment import Comment
from qcc.domain.characteristic import Characteristic

@dataclass(frozen=True)
class TagAssignment:
    """A single tag assignment by a tagger for a characteristic.
    
    Represents the core unit of crowd labeling data - a single
    decision made by a tagger about a specific characteristic
    of a comment.
    
    Attributes:
        tagger: Tagger who made the assignment
        comment: Comment being tagged
        characteristic: Characteristic being evaluated
        value: The tag value assigned (must be from characteristic's domain)
        timestamp: When the assignment was made
    """
    
    tagger_who_assigned: Tagger
    comment_assigned_for: Comment
    assignment_time: datetime
    characteristic_assigned_for: Characteristic
    tag_value: bool
    
    
    def __post_init__(self) -> None:
        """Validate the tag assignment."""
        if not self.tagger:
            raise ValueError("tagger cannot be empty")
        if not self.comment:
            raise ValueError("comment cannot be empty")
        if not self.value:
            raise ValueError("characteristic cannot be empty")
        if not self.timestamp:
            raise ValueError("timestamp cannot be empty")
        
    def count_taggers_that_tagged_this_characteristic_for_this_comment(self, ):
        # 
