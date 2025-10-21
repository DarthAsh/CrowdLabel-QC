"""Prompt domain model for crowd labeling quality control."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from qcc.domain.comment import Comment


@dataclass(frozen=True)
class Prompt:
    """A prompt that groups a collection of comments for tagging."""

    id: str
    text: str
    comments: Optional[List[Comment]] = None

    def __post_init__(self) -> None:
        """Validate prompt data and normalise optional fields."""

        if not self.id:
            raise ValueError("prompt id cannot be empty")
        if not self.text:
            raise ValueError("prompt text cannot be empty")
        if self.comments is None:
            object.__setattr__(self, "comments", [])
