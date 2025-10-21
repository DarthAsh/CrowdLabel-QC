from __future__ import annotations

import pytest

from qcc.metrics.speed_strategy import LogTrimTaggingSpeed
from qcc.domain.tagger import Tagger


def make_empty_tagger() -> Tagger:
    return Tagger(id="t0", meta=None, tagassignments=[])


def test_speed_strategy_raises_until_implemented():
    strat = LogTrimTaggingSpeed()
    with pytest.raises(NotImplementedError):
        strat.speed_log2(make_empty_tagger())


@pytest.mark.xfail(reason="implementation pending (log2 + top-tail trim)")
def test_speed_strategy_trimming_behavior_placeholder():
    # Placeholder for: intervals -> log2 -> trim top 10% -> mean
    assert False


def test_speed_strategy_is_pure_by_contract():
    strat = LogTrimTaggingSpeed()
    t = make_empty_tagger()
    with pytest.raises(NotImplementedError):
        strat.speed_log2(t)
    with pytest.raises(NotImplementedError):
        strat.speed_log2(t)  # same input → same exception → deterministic
