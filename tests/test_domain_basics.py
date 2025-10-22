"""Test basic domain model functionality."""

import pytest
from datetime import datetime

from qcc.domain.enums import TagValue
from qcc.domain.characteristic import Characteristic
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.comment import Comment
from qcc.domain.tagger import Tagger


class TestTagValue:
    """Test TagValue enum functionality."""
    
    def test_tag_value_values(self):
        """Test that TagValue has expected values."""
        assert TagValue.YES == "YES"
        assert TagValue.NO == "NO"
        assert TagValue.NA == "NA"
        assert TagValue.UNCERTAIN == "UNCERTAIN"
        assert TagValue.SKIP == "SKIP"
    
    def test_tag_value_string_conversion(self):
        """Test TagValue string conversion."""
        assert str(TagValue.YES) == "YES"
        assert str(TagValue.NO) == "NO"


class TestCharacteristic:
    """Test Characteristic domain model."""
    
    def test_characteristic_creation(self):
        """Test basic characteristic creation."""
        char = Characteristic(
            id="test_char",
            name="Test Characteristic",
            description="A test characteristic"
        )
        assert char.id == "test_char"
        assert char.name == "Test Characteristic"
        assert char.description == "A test characteristic"
        assert TagValue.YES in char.domain
        assert TagValue.NO in char.domain
        assert TagValue.NA in char.domain
    
    def test_characteristic_default_domain(self):
        """Test that characteristic gets default domain when none provided."""
        char = Characteristic(id="test", name="Test")
        assert len(char.domain) == 3
        assert TagValue.YES in char.domain
        assert TagValue.NO in char.domain
        assert TagValue.NA in char.domain
    
    def test_characteristic_methods_exist(self):
        """Test that characteristic methods exist and raise NotImplementedError."""
        char = Characteristic(id="test", name="Test")
        
        with pytest.raises(NotImplementedError):
            char.num_unique_taggers([])
        
        with pytest.raises(NotImplementedError):
            char.agreement_overall([])
        
        with pytest.raises(NotImplementedError):
            char.prevalence([])


class TestTagAssignment:
    """Test TagAssignment domain model."""
    
    def test_tag_assignment_creation(self):
        """Test basic tag assignment creation."""
        timestamp = datetime.now()
        assignment = TagAssignment(
            tagger_id="tagger1",
            comment_id="comment1",
            characteristic_id="char1",
            value=TagValue.YES,
            timestamp=timestamp
        )
        assert assignment.tagger_id == "tagger1"
        assert assignment.comment_id == "comment1"
        assert assignment.characteristic_id == "char1"
        assert assignment.value == TagValue.YES
        assert assignment.timestamp == timestamp
    
    def test_tag_assignment_validation(self):
        """Test tag assignment validation."""
        timestamp = datetime.now()
        
        with pytest.raises(ValueError, match="tagger_id cannot be empty"):
            TagAssignment("", "comment1", "char1", TagValue.YES, timestamp)
        
        with pytest.raises(ValueError, match="comment_id cannot be empty"):
            TagAssignment("tagger1", "", "char1", TagValue.YES, timestamp)
        
        with pytest.raises(ValueError, match="characteristic_id cannot be empty"):
            TagAssignment("tagger1", "comment1", "", TagValue.YES, timestamp)


class TestComment:
    """Test Comment domain model."""
    
    def test_comment_creation(self):
        """Test basic comment creation."""
        comment = Comment(
            id="comment1",
            text="This is a test comment",
            prompt_id="prompt1",
            tagassignments=[]
        )
        assert comment.id == "comment1"
        assert comment.text == "This is a test comment"
        assert comment.prompt_id == "prompt1"
        assert comment.tagassignments == []
    
    def test_comment_validation(self):
        """Test comment validation."""
        with pytest.raises(ValueError, match="comment id cannot be empty"):
            Comment("", "text", "prompt1", [])
        
        with pytest.raises(ValueError, match="comment text cannot be empty"):
            Comment("comment1", "", "prompt1", [])
        
        with pytest.raises(ValueError, match="prompt_id cannot be empty"):
            Comment("comment1", "text", "", [])
    
    def test_comment_methods_exist(self):
        """Test that comment methods exist and raise NotImplementedError."""
        comment = Comment("comment1", "text", "prompt1", [])
        
        with pytest.raises(NotImplementedError):
            comment.unique_taggers()
        
        with pytest.raises(NotImplementedError):
            comment.agreement_for(Characteristic("char1", "Test"))


class TestTagger:
    """Test Tagger domain model."""
    
    def test_tagger_creation(self):
        """Test basic tagger creation."""
        tagger = Tagger(
            id="tagger1",
            meta={"experience": "high"},
            tagassignments=[]
        )
        assert tagger.id == "tagger1"
        assert tagger.meta == {"experience": "high"}
        assert tagger.tagassignments == []
    
    def test_tagger_default_assignments(self):
        """Test that tagger gets empty assignments list by default."""
        tagger = Tagger(id="tagger1")
        assert tagger.tagassignments == []
    
    def test_tagger_methods_exist(self):
        """Test that tagger methods exist and raise NotImplementedError."""
        tagger = Tagger("tagger1")
        
        # tagging_speed is implemented and returns 0.0 when there are
        # insufficient assignments (fewer than 2). Documented behavior.
        assert tagger.tagging_speed() == 0.0
        
        with pytest.raises(NotImplementedError):
            tagger.agreement_with(Tagger("tagger2"), Characteristic("char1", "Test"))
        
        with pytest.raises(NotImplementedError):
            tagger.pattern_signals(Characteristic("char1", "Test"))


