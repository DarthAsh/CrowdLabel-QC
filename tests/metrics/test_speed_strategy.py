from __future__ import annotations

import pytest

from qcc.metrics.speed_strategy import LogTrimTaggingSpeed
from qcc.domain.tagger import Tagger
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.enums import TagValue
from datetime import datetime, timedelta, timezone



def make_empty_tagger() -> Tagger:
    return Tagger(id="t0", meta=None, tagassignments=[])


def test_speed_strategy_returns_zero_for_insufficient_data():
    strat = LogTrimTaggingSpeed()
    # An empty tagger has fewer than 2 timestamped assignments → 0.0
    assert strat.speed_log2(make_empty_tagger()) == 0.0


def test_log_trim_speed_trims_outliers():
    """Intervals with a single large outlier should be trimmed before mean.
    We create 11 timestamps that produce 10 intervals: nine intervals of 4s
    and one outlier of 1024s. TRIM_FRACTION is 0.1 so the single largest
    interval should be trimmed leaving nine log2(4) == 2.0 values.
    """
    strategy = LogTrimTaggingSpeed()

    # Build timestamps: 10 timestamps spaced by 4s, then one outlier +1024s
    now = datetime.now(timezone.utc)
    timestamps = [now + timedelta(seconds=i * 4) for i in range(10)]
    timestamps.append(timestamps[-1] + timedelta(seconds=1024))

    # Create 11 tag assignments
    assignments = [
        TagAssignment(
            tagger_id="t1",
            comment_id=f"c{i}",
            characteristic_id="char1",
            timestamp=ts,
            value=TagValue.YES,
        )
        for i, ts in enumerate(timestamps)
    ]

    tagger = Tagger(id="t1", meta=None, tagassignments=assignments)

    mean_log2_speed = strategy.speed_log2(tagger)

    assert mean_log2_speed == pytest.approx(2.0)


def test_speed_strategy_is_pure_by_contract():
    strat = LogTrimTaggingSpeed()
    t = make_empty_tagger()
        # Deterministic: repeated calls with same input return same result
    a = strat.speed_log2(t)
    b = strat.speed_log2(t)
    assert a == b
