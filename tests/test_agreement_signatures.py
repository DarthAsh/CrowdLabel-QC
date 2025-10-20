"""Test agreement metrics method signatures."""

import pytest

from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.tagger import Tagger
from qcc.metrics.agreement import AgreementMetrics


class TestAgreementMetrics:
    """Test AgreementMetrics class signatures."""
    
    def test_agreement_metrics_creation(self):
        """Test that AgreementMetrics can be instantiated."""
        metrics = AgreementMetrics()
        assert isinstance(metrics, AgreementMetrics)
    
    def test_percent_agreement_signature(self):
        """Test percent_agreement method signature."""
        metrics = AgreementMetrics()
        char = Characteristic("char1", "Test")
        assignments = []
        
        with pytest.raises(NotImplementedError):
            metrics.percent_agreement(assignments, char)
    
    def test_cohens_kappa_signature(self):
        """Test cohens_kappa method signature."""
        metrics = AgreementMetrics()
        char = Characteristic("char1", "Test")
        assignments = []
        
        with pytest.raises(NotImplementedError):
            metrics.cohens_kappa(assignments, char)
    
    def test_krippendorffs_alpha_signature(self):
        """Test krippendorffs_alpha method signature."""
        metrics = AgreementMetrics()
        char = Characteristic("char1", "Test")
        assignments = []
        
        with pytest.raises(NotImplementedError):
            metrics.krippendorffs_alpha(assignments, char)
    
    def test_pairwise_agreement_signature(self):
        """Test pairwise_agreement method signature."""
        metrics = AgreementMetrics()
        tagger1 = Tagger("tagger1")
        tagger2 = Tagger("tagger2")
        char = Characteristic("char1", "Test")
        
        with pytest.raises(NotImplementedError):
            metrics.pairwise_agreement(tagger1, tagger2, char)
    
    def test_agreement_matrix_signature(self):
        """Test agreement_matrix method signature."""
        metrics = AgreementMetrics()
        char = Characteristic("char1", "Test")
        assignments = []
        
        with pytest.raises(NotImplementedError):
            metrics.agreement_matrix(assignments, char)
    
    def test_filter_assignments_signature(self):
        """Test _filter_assignments_by_characteristic method signature."""
        metrics = AgreementMetrics()
        char = Characteristic("char1", "Test")
        assignments = []
        
        with pytest.raises(NotImplementedError):
            metrics._filter_assignments_by_characteristic(assignments, char)

