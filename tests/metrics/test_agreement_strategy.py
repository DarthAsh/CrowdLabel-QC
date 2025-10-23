from __future__ import annotations
import pytest

# Note: We import the concrete class from the strategies folder
from qcc.metrics.agreement_strategy import LatestLabelPercentAgreement
from qcc.domain.tagger import Tagger

def make_empty_tagger() -> Tagger:
    return Tagger(id="t0", meta=None, tagassignments=[])

def test_latest_label_percent_agreement_signature():
    """
    Smoke test: Ensures the LatestLabelPercentAgreement class and its
    pairwise method exist and correctly raise NotImplementedError.
    """
    strategy = LatestLabelPercentAgreement()
    with pytest.raises(NotImplementedError):
        # We assert that calling the method raises the expected error
        strategy.pairwise(make_empty_tagger())
        
@pytest.mark.xfail(reason="Logic for LatestLabelPercentAgreement not yet implemented")
def test_latest_label_percent_agreement_logic_placeholder():
    """Placeholder for future implementation tests when the logic is added."""
    # This test will be skipped (expected to fail) until the logic is written.
    assert False

def test_speed_strategy_is_pure_by_contract():
    strategy = LatestLabelPercentAgreement()
    t = make_empty_tagger()
    with pytest.raises(NotImplementedError):
        strategy.pairwise(t)
    with pytest.raises(NotImplementedError):
        strategy.pairwise(t)