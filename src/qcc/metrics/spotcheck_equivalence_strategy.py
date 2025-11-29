from __future__ import annotations

"""Spot Check Equivalence (SCE) strategy skeleton.

This module provides a strategy class used to assess tagger reliability
via a spot-check equivalence approach.

This version defines clear method signatures and return types and
raises NotImplementedError in metric methods so callers fail loudly
until concrete algorithms are implemented.

Constraints:
- Only uses dataclasses and typing imports.
- Methods perform no heavy computation yet.
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class SpotCheckEquivalenceStrategy:
    """Strategy template for Spot Check Equivalence (SCE) reliability.

    Attributes
    
    tagassignments:
        A list-like container of tag assignment objects the strategy will
        later operate on. TagAssignment is expected to at least expose
        a `tagger` attribute.
    config:
        Optional configuration parameters for tuning the SCE behaviour
        (e.g., weights for sensitivity vs. measurement integrity).
    """

    tagassignments: List[Any]
    config: Optional[Dict[str, Any]] = None

    #  Core metrics 

    def sensitivity(self, tagger: Any) -> float:
        """Compute responsiveness of a tagger's score to changes in effort.

        Parameters
        
        tagger:
            The tagger instance for which sensitivity is computed.

        Returns
        --
        float
            A numeric sensitivity score. Higher should mean that small
            changes in (implicit) effort would noticeably change the
            tagger's score.

        Notes
        
        This is a placeholder. The concrete implementation should be
        pure and deterministic: no I/O, DB access, or global state.
        """
        raise NotImplementedError("SpotCheckEquivalenceStrategy.sensitivity() is not implemented yet.")

    def measurement_integrity(self, tagger: Any) -> float:
        """Measure correlation between tagger scores and inferred quality.

        This is where a real implementation would compute a statistic
        analogous to Measurement Integrity (e.g., correlation between
        the tagger's score and some quality signal such as agreement).

        Parameters
        
        tagger:
            The tagger instance being evaluated.

        Returns
        --
        float
            A numeric integrity measure, typically in [0, 1], where
            higher values indicate stronger alignment between the
            tagger's scores and inferred quality.
        """
        raise NotImplementedError("SpotCheckEquivalenceStrategy.measurement_integrity() is not implemented yet.")

    def spot_check_equivalence(self, tagger: Any) -> float:
        """Combine underlying metrics into a normalized SCE value.

        In a full implementation, this would map the tagger's metrics
        (sensitivity, measurement integrity, etc.) to an equivalent
        spot-checking ratio (e.g., 'this tagger behaves like X% of
        their work was spot-checked against ground truth').

        Parameters
        
        tagger:
            The tagger instance being evaluated.

        Returns
        --
        float
            A normalized SCE score, e.g., a pseudo spot-checking ratio
            in the range [0.0, 1.0].
        """
        raise NotImplementedError("SpotCheckEquivalenceStrategy.spot_check_equivalence() is not implemented yet.")

    #  Aggregation / reporting 

    def summary_report(self) -> Dict[Any, Dict[str, float]]:
        """Summarize SCE-related metrics for all taggers in tagassignments.

        Returns
        --
        dict
            A mapping from tagger identifier (or tagger object) to a
            small dict of metric names and values, e.g.:

            {
                tagger_id_1: {
                    "sensitivity": 0.42,
                    "measurement_integrity": 0.81,
                    "sce": 0.27,
                },
                ...
            }

        Notes
        
        This method is designed for downstream reporting. It should
        remain pure/deterministic once the metric methods are defined.
        """
        raise NotImplementedError("SpotCheckEquivalenceStrategy.summary_report() is not implemented yet.")
