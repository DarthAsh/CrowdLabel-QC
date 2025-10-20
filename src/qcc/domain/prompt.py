"""Prompt domain model for crowd labeling quality control."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Prompt:
    """A prompt groups related comments within a deployment."""

    id: str
    text: str
    description: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("prompt id cannot be empty")
        if not self.text:
            raise ValueError("prompt text cannot be empty")
