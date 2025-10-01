"""Prompt domain model for crowd labeling quality control."""

from dataclasses import dataclass
from typing import List

# Removed: from qcc.domain.comment import Comment (no longer needed)

@dataclass(frozen=True)
class Prompt:
    """A prompt that defines a task for labeling.
    
    A prompt represents a specific task or question that taggers
    are asked to evaluate. It is a definition, not a container of data.
    
    Attributes:
        id: Unique identifier for the prompt
        text: The instruction text for the prompt
    """
    
    id: str
    text: str
    # Removed: comments: List[Comment]
    
    def __post_init__(self) -> None:
        """Validate the prompt."""
        if not self.id:
            raise ValueError("prompt id cannot be empty")
        if not self.text:
            raise ValueError("prompt text cannot be empty")