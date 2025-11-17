"""Speed metric façade with unimplemented strategy hooks.

This module provides the :class:`SpeedMetrics` class as an interface surface for
future implementations. Each method currently raises ``NotImplementedError``
to indicate the absence of concrete logic while preserving the intended API
shape used across the codebase and tests.
"""

from __future__ import annotations

from typing import Iterable, Optional

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger


class SpeedMetrics:
    """Expose tagging speed-related metric helpers.

    The methods act as placeholders to be filled with strategy-backed logic in
    future work. They currently raise ``NotImplementedError`` to make the
    expected interface explicit.
    """

    def average_tagging_speed(
        self, assignments: Iterable[TagAssignment], tagger: Optional[Tagger] = None
    ) -> float:
        """Return the average tagging speed for all assignments or a tagger.

        Args:
            assignments: Tag assignments to analyze.
            tagger: Optional tagger to scope the calculation.
        """

        raise NotImplementedError

    def tagging_speed_distribution(
        self, assignments: Iterable[TagAssignment], tagger: Optional[Tagger] = None
    ) -> list[float]:
        """Return the distribution of tagging speeds.

        Args:
            assignments: Tag assignments to analyze.
            tagger: Optional tagger to scope the calculation.
        """

        raise NotImplementedError

    def detect_speed_anomalies(
        self,
        assignments: Iterable[TagAssignment],
        tagger: Optional[Tagger] = None,
        threshold: Optional[float] = None,
    ) -> list[TagAssignment]:
        """Identify assignments with anomalous tagging speeds.

        Args:
            assignments: Tag assignments to analyze.
            tagger: Optional tagger to scope the detection.
            threshold: Optional threshold controlling anomaly sensitivity.
        """

        raise NotImplementedError

    def speed_by_characteristic(
        self, assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> dict[str, float]:
        """Return tagging speed grouped by a characteristic."""

        raise NotImplementedError

    def speed_trends(
        self,
        assignments: Iterable[TagAssignment],
        tagger: Optional[Tagger] = None,
        window_size: Optional[int] = None,
    ) -> list[float]:
        """Return time-based tagging speed trends."""

        raise NotImplementedError
