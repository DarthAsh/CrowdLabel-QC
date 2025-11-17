"""Pattern metric façade with unimplemented strategy hooks.

The :class:`PatternMetrics` class exposes the intended interface for pattern
analysis but defers implementation to future work by raising
``NotImplementedError``. This preserves call sites and provides clear guidance
for forthcoming strategy integrations.
"""

from __future__ import annotations

from typing import Iterable

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger


class PatternMetrics:
    """Expose pattern-detection metric helpers."""

    def detect_repetitive_patterns(
        self,
        assignments: Iterable[TagAssignment],
        characteristic: Characteristic,
        pattern_length: int = 3,
        window_size: int = 12,
    ) -> dict[str, int]:
        """Detect repetitive patterns for a characteristic."""

        raise NotImplementedError

    def detect_sequential_patterns(
        self,
        assignments: Iterable[TagAssignment],
        tagger: Tagger,
        characteristic: Characteristic,
        window_size: int = 10,
    ) -> list[str]:
        """Detect sequential patterns produced by a tagger."""

        raise NotImplementedError

    def detect_bias_patterns(
        self,
        assignments: Iterable[TagAssignment],
        tagger: Tagger,
        characteristic: Characteristic,
    ) -> dict[str, float]:
        """Detect bias patterns for a tagger and characteristic."""

        raise NotImplementedError

    def detect_temporal_patterns(
        self,
        assignments: Iterable[TagAssignment],
        tagger: Tagger,
        characteristic: Characteristic,
        period_hours: int = 24,
    ) -> list[float]:
        """Detect temporal patterns in tagging behavior."""

        raise NotImplementedError

    def calculate_pattern_entropy(
        self,
        assignments: Iterable[TagAssignment],
        characteristic: Characteristic,
        max_pattern_length: int = 5,
    ) -> float:
        """Calculate pattern entropy for a characteristic."""

        raise NotImplementedError
