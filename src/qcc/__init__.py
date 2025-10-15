"""QCC - Quality Control of Crowd labeling.

A Python library for analyzing and reporting on crowd labeling quality metrics.
"""

__version__ = "0.1.0"
__author__ = "QCC Contributors"

from qcc.domain.enums import TagValue
from qcc.domain.characteristic import Characteristic
from qcc.domain.comment import Comment
from qcc.domain.tagger import Tagger
from qcc.domain.tagassignment import TagAssignment

__all__ = [
    "TagValue",
    "Characteristic",
    "Comment",
    "Tagger",
    "TagAssignment",
]
