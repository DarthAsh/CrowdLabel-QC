"""Agreement metric façade delegating to the strategy implementation."""

from __future__ import annotations

from typing import Iterable, Optional, Sequence

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger

from .agreement_strategy import LatestLabelPercentAgreement


class AgreementMetrics:
    """Expose agreement helpers backed by :class:`LatestLabelPercentAgreement`."""

    def __init__(self, strategy: Optional[LatestLabelPercentAgreement] = None) -> None:
        self._strategy = strategy or LatestLabelPercentAgreement()

    @property
    def strategy(self) -> LatestLabelPercentAgreement:
        """Return the underlying agreement strategy instance."""

        return self._strategy

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def percent_agreement(
        self, assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> float:
        """Return overall percent agreement across all raters."""

        return self._strategy.percent_agreement(assignments, characteristic)

    def cohens_kappa(
        self, assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> float:
        """Return Cohen's kappa averaged across tagger pairs."""

        return self._strategy.cohens_kappa(assignments, characteristic)

    def krippendorffs_alpha(
        self, assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> Optional[float]:
        """Return Krippendorff's alpha for the characteristic."""

        return self._strategy.krippendorff_alpha(assignments, characteristic)

    def pairwise_agreement(
        self, tagger_a: Tagger, tagger_b: Tagger, characteristic: Characteristic
    ) -> float:
        """Return percent agreement for the two provided taggers."""

        return self._strategy.pairwise(tagger_a, tagger_b, characteristic)

    def agreement_matrix(
        self, assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> dict[str, dict[str, float]]:
        """Return the pairwise agreement matrix for all participating taggers."""

        return self._strategy.agreement_matrix(assignments, characteristic)

    def per_tagger_metrics(
        self,
        assignments: Iterable[TagAssignment],
        characteristic: Characteristic,
        methods: Sequence[str],
    ) -> dict[str, dict[str, float]]:
        """Return agreement metrics averaged per tagger for the requested methods."""

        return self._strategy.per_tagger_metrics(assignments, characteristic, methods)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    @staticmethod
    def _filter_assignments_by_characteristic(
        assignments: Iterable[TagAssignment], characteristic: Characteristic
    ) -> Sequence[TagAssignment]:
        """Return assignments that match ``characteristic``."""

        characteristic_id = characteristic.id
        return [
            assignment
            for assignment in assignments
            if assignment.characteristic_id == characteristic_id
        ]
