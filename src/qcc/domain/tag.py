"""Tag domain model for crowd labeling quality control."""

from dataclasses import dataclass

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue


@dataclass(frozen=True)
class Tag:
    """A specific value assigned for a given Characteristic.
    
    A Tag represents the core labeling decision - a specific value
    that has been assigned to a particular characteristic. This is
    the atomic unit of crowd labeling data that combines a
    characteristic with its assigned value.
    
    Attributes:
        characteristic: The characteristic being labeled
        value: The specific tag value assigned to the characteristic
    """
    
    characteristic: Characteristic
    value: TagValue
    
    def __post_init__(self) -> None:
        """Validate the tag assignment."""
        if not self.characteristic:
            raise ValueError("characteristic cannot be None")
        if not self.value:
            raise ValueError("value cannot be None")
        
        # Validate that the value is in the characteristic's domain
        if self.value not in self.characteristic.domain:
            raise ValueError(
                f"Value '{self.value}' is not in characteristic '{self.characteristic.name}' domain: {self.characteristic.domain}"
            )
