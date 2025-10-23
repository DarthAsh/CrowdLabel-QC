from __future__ import annotations

import pytest

# Note: We import the concrete class from the strategies folder
from qcc.metrics.pattern_strategy import SimpleSequencePatterns
from qcc.domain.tagger import Tagger

def make_empty_tagger() -> Tagger:
    return Tagger(id="t0", meta=None, tagassignments=[])

def test_simple_sequence_patterns_signature():
    """
    Smoke test: Ensures the SimpleSequencePatterns class and its
    analyze method exist and correctly raise NotImplementedError.
    """
    strategy = SimpleSequencePatterns()
    with pytest.raises(NotImplementedError):
        # We assert that calling the method raises the expected error
        strategy.analyze(make_empty_tagger())
        
@pytest.mark.xfail(reason="Logic for SimpleSequencePatterns not yet implemented")
def test_simple_sequence_patterns_logic_placeholder():
    """Placeholder for future implementation tests when the logic is added."""
    # This test will be skipped (expected to fail) until the logic is written.
    assert False

def test_speed_strategy_is_pure_by_contract():
    strategy = SimpleSequencePatterns()
    t = make_empty_tagger()
    with pytest.raises(NotImplementedError):
        strategy.analyze(t)
    with pytest.raises(NotImplementedError):
        strategy.analyze(t)