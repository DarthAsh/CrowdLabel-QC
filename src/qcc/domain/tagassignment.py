from __future__ import annotations

"""TagAssignment domain model for crowd labeling quality control."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from qcc.domain.enums import TagValue

@dataclass(frozen=True)
class TagAssignment:
    """A single tag assignment by a tagger for a characteristic.
    
    Represents the core unit of crowd labeling data - a single
    decision made by a tagger about a specific characteristic
    of a comment.
    
    Attributes:
        tagger_id: Identifier of the tagger who made the assignment
        comment_id: Identifier of the comment being tagged
        characteristic_id: Identifier of the characteristic being evaluated
        value: The tag value assigned (must be from characteristic's domain)
        timestamp: When the assignment was made
    """
    
    tagger_id: str
    comment_id: str
    characteristic_id: str
    value: TagValue
    timestamp: datetime
    assignment_id: Optional[str] = None
    prompt_id: Optional[str] = None
    team_id: Optional[str] = None
    question_id: Optional[str] = None
    questionnaire_id: Optional[str] = None
    
    def __post_init__(self) -> None:
        """Validate the tag assignment."""
        if not self.tagger_id:
            raise ValueError("tagger_id cannot be empty")
        if not self.comment_id:
            raise ValueError("comment_id cannot be empty")
        if not self.characteristic_id:
            raise ValueError("characteristic_id cannot be empty")

