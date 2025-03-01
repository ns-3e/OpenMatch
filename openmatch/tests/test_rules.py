import pytest
from openmatch.match import (
    MatchType,
    FieldConfig,
    MatchRuleConfig,
    create_exact_ssn_rule,
    create_fuzzy_name_dob_rule
)
from openmatch.match.rules import ExactMatcher, FuzzyMatcher, MatchRule

def test_exact_matcher():
    """Test exact matching functionality."""
    matcher = ExactMatcher()
    
    # Test exact matches
    assert matcher.compute_similarity("test", "test") == 1.0
    assert matcher.compute_similarity("Test", "test") == 1.0  # Case insensitive
    assert matcher.compute_similarity("test", "different") == 0.0
    
    # Test None values
    assert matcher.compute_similarity(None, "test") == 0.0
    assert matcher.compute_similarity("test", None) == 0.0
    assert matcher.compute_similarity(None, None) == 0.0

def test_fuzzy_matcher():
    """Test fuzzy matching functionality."""
    matcher = FuzzyMatcher()
    
    # Test with different methods
    assert matcher.compute_similarity("john", "jon", "jaro_winkler") > 0.8
    assert matcher.compute_similarity("john", "jon", "levenshtein") > 0.6
    assert matcher.compute_similarity("john", "jon", "ratio") > 0.7
    
    # Test exact matches
    assert matcher.compute_similarity("test", "test", "jaro_winkler") == 1.0
    
    # Test completely different strings
    assert matcher.compute_similarity("abc", "xyz", "jaro_winkler") < 0.5
    
    # Test None values
    assert matcher.compute_similarity(None, "test") == 0.0
    assert matcher.compute_similarity("test", None) == 0.0
    
    # Test invalid method
    with pytest.raises(ValueError):
        matcher.compute_similarity("test", "test", "invalid_method")

def test_match_rule_exact_ssn():
    """Test exact SSN matching rule."""
    rule = MatchRule(create_exact_ssn_rule())
    
    record1 = {"ssn": "123-45-6789"}
    record2 = {"ssn": "123-45-6789"}
    record3 = {"ssn": "987-65-4321"}
    
    # Test exact match
    assert rule.compute_match_confidence(record1, record2) == 1.0
    
    # Test no match
    assert rule.compute_match_confidence(record1, record3) == 0.0
    
    # Test missing field
    assert rule.compute_match_confidence(record1, {}) == 0.0
    assert rule.compute_match_confidence({}, record2) == 0.0

def test_match_rule_fuzzy_name_dob():
    """Test fuzzy name matching with DOB rule."""
    rule = MatchRule(create_fuzzy_name_dob_rule())
    
    record1 = {
        "first_name": "John",
        "last_name": "Doe",
        "dob": "1990-01-01"
    }
    
    record2 = {
        "first_name": "Jon",  # Slightly different
        "last_name": "Doe",
        "dob": "1990-01-01"
    }
    
    record3 = {
        "first_name": "Jane",
        "last_name": "Doe",
        "dob": "1992-03-15"
    }
    
    # Test similar names, same DOB
    confidence = rule.compute_match_confidence(record1, record2)
    assert confidence > 0.8
    
    # Test different names, different DOB
    confidence = rule.compute_match_confidence(record1, record3)
    assert confidence < 0.7
    
    # Test missing required field (DOB)
    record_missing_dob = {
        "first_name": "John",
        "last_name": "Doe"
    }
    assert rule.compute_match_confidence(record1, record_missing_dob) == 0.0

def test_match_rule_validation():
    """Test match rule validation."""
    # Test invalid weights
    with pytest.raises(ValueError):
        MatchRuleConfig(
            name="invalid_weights",
            fields=[
                FieldConfig(
                    name="field1",
                    match_type=MatchType.EXACT,
                    weight=0.5  # Weights don't sum to 1.0
                )
            ]
        )
    
    # Test empty fields
    with pytest.raises(ValueError):
        MatchRuleConfig(
            name="empty_fields",
            fields=[]
        )

def test_embedding_matcher():
    """Test embedding-based matching functionality."""
    from openmatch.match.matchers import EmbeddingMatcher
    import os
    
    # Force CPU usage for tests
    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    os.environ["PYTORCH_MPS_HIGH_WATERMARK_RATIO"] = "0.0"
    
    matcher = EmbeddingMatcher()
    
    # Test basic similarity
    text1 = "John Doe"
    text2 = "Jon Doe"
    similarity = matcher.compute_similarity(text1, text2)
    print(f"\nSimilarity between '{text1}' and '{text2}': {similarity:.4f}")
    assert 0.0 <= similarity <= 1.0
    assert similarity > 0.7  # Similar names should have reasonably high similarity
    
    # Test different names
    text3 = "Jane Smith"
    similarity2 = matcher.compute_similarity(text1, text3)
    print(f"Similarity between '{text1}' and '{text3}': {similarity2:.4f}")
    assert similarity2 < similarity  # Different names should have lower similarity 