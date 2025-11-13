from __future__ import annotations
from typing import Any
from .interfaces import AgreementStrategy

# ------------------------------------------------------------
# LatestLabelPercentAgreement
# ------------------------------------------------------------
# This strategy will calculate the percent agreement between
# two taggers (a and b) based on their *latest* labels
# for each (comment, characteristic) pair.
#
# Notes:
# - NA (missing) values should be excluded from calculations.
# - If there are ties when determining the "latest" label,
#   the result must be deterministic (same every time).
# - Implementation will go inside `pairwise` later.
# ------------------------------------------------------------

class LatestLabelPercentAgreement(AgreementStrategy):
    """latest-per-(comment, characteristic), NA excluded; deterministic tie-break."""

    def pairwise(self, tagger_a: "Tagger", tagger_b: "Tagger", char: "Characteristic") -> float:
        """
        Compare two Taggers (a and b) for a specific Characteristic.

        This method calculates the percent agreement between two taggers
        based on their latest assignments for shared (comment, characteristic)
        pairs, excluding NA values.

        Args:
            a (Tagger): The first tagger to compare.
            b (Tagger): The second tagger to compare.
            char (Characteristic): The characteristic under evaluation.

        Returns:
            float: The percent agreement between taggers a and b
                   for the given characteristic, excluding NA values.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        # TODO: Implement the logic for latest-label percent agreement.
        raise NotImplementedError("LatestLabelPercentAgreement.pairwise not implemented yet")
