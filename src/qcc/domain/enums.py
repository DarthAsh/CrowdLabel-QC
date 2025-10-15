"""Enums for the QCC domain model."""

from enum import Enum


class TagValue(str, Enum):
    """Possible values for tag assignments.
    
    This enum represents the canonical set of values that can be assigned
    to characteristics during crowd labeling.
    """
    
    YES = "YES"
    NO = "NO"
    NA = "NA"  # Not Applicable
    UNCERTAIN = "UNCERTAIN"
    SKIP = "SKIP"
    
    def __str__(self) -> str:
        """Return the string value of the enum."""
        return self.value

