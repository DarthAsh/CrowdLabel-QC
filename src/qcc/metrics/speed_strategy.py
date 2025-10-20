from __future__ import annotations

"""Skeleton tagging-speed strategy implementation.

This module provides a named strategy class for the log2+trim tagging-speed
algorithm. It is a skeleton only: methods raise NotImplementedError and
contain TODO notes pointing to the original implementation in
`qcc.domain.tagger` which should be ported in a follow-up PR.

Constraints:
- Pure, deterministic, no I/O
- Use forward refs for domain types to avoid circular imports
"""

from typing import Any

from .interfaces import TaggingSpeedStrategy


class LogTrimTaggingSpeed(TaggingSpeedStrategy):
    """Skeleton for tagging speed: log2 of inter-tag intervals with top-tail trim.

    This class intentionally does not implement the algorithm yet. The
    implementation should port the commented original code from
    `qcc.domain.tagger.Tagger.tagging_speed`.
    """

    def speed_log2(self, tagger: "Tagger") -> float:
        """Return mean log2(seconds) between tags.

        Skeleton only: raise NotImplementedError until porting is complete.
        """
        # TODO: Port logic from Tagger.tagging_speed (log2 intervals + trim top 10% by length)
        #       Use the commented code in Tagger as the source of truth.
        raise NotImplementedError("LogTrimTaggingSpeed.speed_log2 not implemented")

    @staticmethod
    def seconds_per_tag(mean_log2: float) -> float:
        """Convert mean_log2 back to seconds-per-tag (2 ** mean_log2).

        Skeleton only: keeps signature for callers to rely on during migration.
        """
        # TODO: Implement in the logic-port PR; keep pure and deterministic.
        raise NotImplementedError("LogTrimTaggingSpeed.seconds_per_tag not implemented")
