from __future__ import annotations

"""Default strategy skeletons for tagging metrics.

These are lightweight, pure stubs where the heavy logic will be ported later.
"""

from typing import Any

from .interfaces import TaggingSpeedStrategy


class DefaultTaggingSpeedStrategy(TaggingSpeedStrategy):
    """Default tagging speed strategy (skeleton).

    Methods are pure and deterministic. No I/O. Concrete logic will be
    ported from the Tagger implementation into these methods later.
    """

    def speed_log2(self, tagger: "Tagger") -> float:
        """Return log2(seconds-per-tag) estimate for a tagger.

        Args:
            tagger: forward-ref to Tagger domain model
        Returns:
            float: mean log2(seconds) estimate
        Raises:
            NotImplementedError: implementation pending
        """
        # TODO: port existing log2 + trimming logic here (uses TRIM_FRACTION)
        raise NotImplementedError("DefaultTaggingSpeedStrategy.speed_log2 not implemented")

    @staticmethod
    def seconds_per_tag(mean_log2: float) -> float:
        """Helper: convert mean log2 back to seconds-per-tag.

        Pure conversion: 2 ** mean_log2. Skeleton only.
        """
        # TODO: consider numeric edge-cases and return types
        raise NotImplementedError("DefaultTaggingSpeedStrategy.seconds_per_tag not implemented")
