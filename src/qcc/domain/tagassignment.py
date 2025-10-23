"""TagAssignment domain model for crowd labeling quality control."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Set

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
        tagger_who_assigned: Tagger who made the assignment
        comment_assigned_for: Comment being tagged
        assignment_time: When the assignment was made
        characteristic_assigned_for: Characteristic being evaluated
        tag_value: The tag value assigned (must be from characteristic's domain)        
    """
    
    tagger_who_assigned: Tagger
    comment_assigned_for: Comment
    assignment_time: datetime
    characteristic_assigned_for: Characteristic
    tag_value: bool
    
    
    def __post_init__(self) -> None:
        """Validate the tag assignment."""
        if not self.tagger_who_assigned:
            raise ValueError("tagger cannot be empty")
        if not self.comment_assigned_for:
            raise ValueError("comment cannot be empty")
        if not self.characteristic_assigned_for:
            raise ValueError("characteristic cannot be empty")
        if not self.assignment_time:
            raise ValueError("timestamp cannot be empty")