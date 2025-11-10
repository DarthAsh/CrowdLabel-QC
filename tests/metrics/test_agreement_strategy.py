from datetime import datetime, timedelta

import pytest

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.metrics.agreement_strategy import LatestLabelPercentAgreement


def _assignment(tagger_id: str, comment_id: str, minutes: int, value: TagValue) -> TagAssignment:
    return TagAssignment(
        tagger_id=tagger_id,
        comment_id=comment_id,
        characteristic_id="char",
        value=value,
        timestamp=datetime(2024, 1, 1) + timedelta(minutes=minutes),
    )


class TestLatestLabelPercentAgreement:
    def setup_method(self) -> None:
        self.strategy = LatestLabelPercentAgreement()
        self.char = Characteristic("char", "Test")

    def test_pairwise_uses_latest_labels(self) -> None:
        tagger_a = Tagger(
            "a",
            tagassignments=[
                _assignment("a", "c1", 0, TagValue.YES),
                _assignment("a", "c1", -10, TagValue.NO),
                _assignment("a", "c2", 1, TagValue.NO),
            ],
        )
        tagger_b = Tagger(
            "b",
            tagassignments=[
                _assignment("b", "c1", 2, TagValue.YES),
                _assignment("b", "c2", 3, TagValue.NO),
            ],
        )

        score = self.strategy.pairwise(tagger_a, tagger_b, self.char)
        assert score == pytest.approx(1.0)

    def test_percent_agreement_multiple_taggers(self) -> None:
        assignments = [
            _assignment("a", "c1", 0, TagValue.YES),
            _assignment("b", "c1", 1, TagValue.YES),
            _assignment("c", "c1", 2, TagValue.NO),
            _assignment("a", "c2", 3, TagValue.NO),
            _assignment("b", "c2", 4, TagValue.NO),
            _assignment("c", "c2", 5, TagValue.NO),
        ]

        score = self.strategy.percent_agreement(assignments, self.char)
        # For comment c1: pairs (a,b) agree, (a,c) disagree, (b,c) disagree -> 1/3
        # For comment c2: all agree -> 3 agreements
        # Total pairs = 6, agreements = 4
        assert score == pytest.approx(4 / 6)

    def test_cohens_kappa_without_overlap(self) -> None:
        assignments = [
            _assignment("a", "c1", 0, TagValue.YES),
            _assignment("b", "c2", 0, TagValue.YES),
        ]

        score = self.strategy.cohens_kappa(assignments, self.char)
        assert score == 0.0

    def test_agreement_matrix_is_symmetric(self) -> None:
        assignments = [
            _assignment("a", "c1", 0, TagValue.YES),
            _assignment("b", "c1", 1, TagValue.YES),
            _assignment("a", "c2", 2, TagValue.NO),
            _assignment("b", "c2", 3, TagValue.NO),
        ]

        matrix = self.strategy.agreement_matrix(assignments, self.char)
        assert matrix == {"a": {"a": 1.0, "b": 1.0}, "b": {"a": 1.0, "b": 1.0}}

    def test_per_tagger_metrics_average_pairwise_scores(self) -> None:
        assignments = [
            _assignment("a", "c1", 0, TagValue.YES),
            _assignment("b", "c1", 1, TagValue.YES),
            _assignment("c", "c1", 2, TagValue.NO),
            _assignment("a", "c2", 3, TagValue.NO),
            _assignment("b", "c2", 4, TagValue.NO),
            _assignment("c", "c2", 5, TagValue.NO),
        ]

        per_tagger = self.strategy.per_tagger_metrics(
            assignments,
            self.char,
            ("percent_agreement", "cohens_kappa"),
        )

        assert per_tagger == {
            "a": {"percent_agreement": pytest.approx(0.75), "cohens_kappa": pytest.approx(0.5)},
            "b": {"percent_agreement": pytest.approx(0.75), "cohens_kappa": pytest.approx(0.5)},
            "c": {"percent_agreement": pytest.approx(0.5), "cohens_kappa": pytest.approx(0.0)},
        }

    def test_krippendorff_alpha_reuses_matrix(self) -> None:
        assignments = [
            _assignment("a", "c1", 0, TagValue.YES),
            _assignment("b", "c1", 1, TagValue.YES),
            _assignment("a", "c2", 2, TagValue.NO),
            _assignment("b", "c2", 3, TagValue.NO),
        ]

        score = self.strategy.krippendorff_alpha(assignments, self.char)
        assert score == 1.0
