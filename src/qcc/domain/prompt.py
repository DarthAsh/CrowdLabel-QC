"""Prompt domain model for crowd labeling quality control."""

from dataclasses import dataclass
from typing import List

from qcc.domain.comment import Comment


@dataclass(frozen=True)
class Prompt:
    """A prompt that contains multiple comments for labeling.
    
    A prompt represents a specific task or question that taggers
    are asked to evaluate, and contains multiple comments that
    need to be labeled according to the prompt's instructions.
    
    Attributes:
        id: Unique identifier for the prompt
        text: The instruction text for the prompt
        comments: List of comments associated with this prompt
    """
    
    id: str
    text: str
    comments: List[Comment]
    
    def __post_init__(self) -> None:
        """Validate the prompt."""
        if not self.id:
            raise ValueError("prompt id cannot be empty")
        if not self.text:
            raise ValueError("prompt text cannot be empty")
