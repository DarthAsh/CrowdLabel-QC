from __future__ import annotations

"""Public exports for the qcc.metrics package.

Exports the strategy Protocols and the named speed strategy skeleton.
"""

from .interfaces import TaggingSpeedStrategy, AgreementStrategy, PatternSignalsStrategy
from .speed_strategy import LogTrimTaggingSpeed

__all__ = [
    "TaggingSpeedStrategy",
    "AgreementStrategy",
    "PatternSignalsStrategy",
    "LogTrimTaggingSpeed",
]
from .interfaces import TaggingSpeedStrategy, AgreementStrategy, PatternSignalsStrategy
from .default_strategies import DefaultTaggingSpeedStrategy

__all__ = [
    "TaggingSpeedStrategy",
    "AgreementStrategy",
    "PatternSignalsStrategy",
    "DefaultTaggingSpeedStrategy",
]
