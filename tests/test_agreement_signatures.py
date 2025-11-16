"""Integration tests for the agreement metrics façade."""

from datetime import datetime, timedelta

from qcc.domain.characteristic import Characteristic
from qcc.domain.enums import TagValue
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.metrics.agreement import AgreementMetrics
from qcc.metrics.agreement_strategy import LatestLabelPercentAgreement


def _make_assignment(
    tagger_id: str,
    comment_id: str,
    characteristic_id: str,
    value: TagValue,
    minutes: int,
) -> TagAssignment:
    return TagAssignment(
        tagger_id=tagger_id,
        comment_id=comment_id,
        characteristic_id=characteristic_id,
        value=value,
        timestamp=datetime(2024, 1, 1, 0, 0, 0) + timedelta(minutes=minutes),
    )


class TestAgreementMetrics:
    """Validate the restored AgreementMetrics façade."""

    def setup_method(self) -> None:
        self.metrics = AgreementMetrics()
        self.char = Characteristic("char1", "Test characteristic")
        self.assignments = [
            _make_assignment("tagger1", "comment1", self.char.id, TagValue.YES, 0),
            _make_assignment("tagger2", "comment1", self.char.id, TagValue.YES, 1),
            _make_assignment("tagger1", "comment2", self.char.id, TagValue.NO, 2),
            _make_assignment("tagger2", "comment2", self.char.id, TagValue.NO, 3),
            # Duplicate rating for tagger1 on comment1 that should be ignored (older)
            _make_assignment("tagger1", "comment1", self.char.id, TagValue.NO, -10),
            # Assignment for a different characteristic that must be filtered out
            _make_assignment("tagger1", "comment3", "other", TagValue.YES, 4),
        ]

    def test_agreement_metrics_creation(self) -> None:
        """AgreementMetrics can be instantiated and exposes helpers."""

        assert isinstance(self.metrics, AgreementMetrics)
        assert isinstance(self.metrics.strategy, LatestLabelPercentAgreement)

    def test_percent_agreement(self) -> None:
        """Percent agreement collapses latest ratings and matches perfectly."""

        score = self.metrics.percent_agreement(self.assignments, self.char)
        assert score == 1.0

    def test_cohens_kappa(self) -> None:
        """Cohen's kappa is 1.0 for perfect agreement."""

        score = self.metrics.cohens_kappa(self.assignments, self.char)
        assert score == 1.0

    def test_krippendorffs_alpha(self) -> None:
        """Krippendorff's alpha reports perfect agreement."""

        score = self.metrics.krippendorffs_alpha(self.assignments, self.char)
        assert score == 1.0

    def test_pairwise_agreement(self) -> None:
        """Pairwise agreement delegates to the strategy implementation."""

        tagger1 = Tagger("tagger1", tagassignments=list(self.assignments))
        tagger2 = Tagger("tagger2", tagassignments=list(self.assignments))

        score = self.metrics.pairwise_agreement(tagger1, tagger2, self.char)
        assert score == 1.0

    def test_agreement_matrix(self) -> None:
        """Agreement matrix yields a symmetric map keyed by tagger id."""

        matrix = self.metrics.agreement_matrix(self.assignments, self.char)
        assert matrix == {"tagger1": {"tagger1": 1.0, "tagger2": 1.0}, "tagger2": {"tagger1": 1.0, "tagger2": 1.0}}

    def test_per_tagger_metrics(self) -> None:
        """Per-tagger metrics mirror the requested agreement methods."""

        per_tagger = self.metrics.per_tagger_metrics(
            self.assignments,
            self.char,
            ("percent_agreement", "cohens_kappa"),
        )

        assert per_tagger == {
            "tagger1": {"percent_agreement": 1.0, "cohens_kappa": 1.0},
            "tagger2": {"percent_agreement": 1.0, "cohens_kappa": 1.0},
        }

    def test_filter_assignments(self) -> None:
        """Filtering retains only assignments for the requested characteristic."""

        filtered = self.metrics._filter_assignments_by_characteristic(self.assignments, self.char)
        assert all(assignment.characteristic_id == self.char.id for assignment in filtered)
        assert len(filtered) == 5

