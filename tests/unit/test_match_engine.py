import pytest
from openmatch.match import MatchConfig, MatchEngine

def test_match_config_initialization():
    """Test that MatchConfig is initialized correctly with valid parameters."""
    config = MatchConfig(
        blocking_keys=["zip_code", "name_prefix"],
        comparison_fields=[
            ("name", "fuzzy", 0.8),
            ("address", "address_similarity", 0.7),
            ("phone", "exact", 1.0)
        ],
        min_overall_score=0.85
    )
    
    assert config.blocking_keys == ["zip_code", "name_prefix"]
    assert len(config.comparison_fields) == 3
    assert config.min_overall_score == 0.85

def test_match_config_validation():
    """Test that MatchConfig validates parameters correctly."""
    with pytest.raises(ValueError):
        MatchConfig(
            blocking_keys=[],  # Empty blocking keys should raise error
            comparison_fields=[("name", "fuzzy", 0.8)],
            min_overall_score=0.85
        )
    
    with pytest.raises(ValueError):
        MatchConfig(
            blocking_keys=["zip_code"],
            comparison_fields=[],  # Empty comparison fields should raise error
            min_overall_score=0.85
        )
    
    with pytest.raises(ValueError):
        MatchConfig(
            blocking_keys=["zip_code"],
            comparison_fields=[("name", "fuzzy", 0.8)],
            min_overall_score=1.5  # Score > 1.0 should raise error
        )

def test_match_engine_exact_match(sample_records):
    """Test exact matching functionality."""
    config = MatchConfig(
        blocking_keys=["phone"],
        comparison_fields=[
            ("phone", "exact", 1.0)
        ],
        min_overall_score=1.0
    )
    
    engine = MatchEngine(config)
    matches = engine.find_matches(sample_records)
    
    # CRM_001 and ERP_101 should match (same phone number)
    assert any(
        m for m in matches 
        if m.record1["id"] == "CRM_001" and m.record2["id"] == "ERP_101"
    )

def test_match_engine_fuzzy_match(sample_records):
    """Test fuzzy matching functionality."""
    config = MatchConfig(
        blocking_keys=["phone"],
        comparison_fields=[
            ("name", "fuzzy", 0.8)
        ],
        min_overall_score=0.8
    )
    
    engine = MatchEngine(config)
    matches = engine.find_matches(sample_records)
    
    # All variations of "Acme Corp" should match
    assert len(matches) >= 2  # At least 2 matches between the 3 records

def test_match_engine_address_similarity(sample_records):
    """Test address matching functionality."""
    config = MatchConfig(
        blocking_keys=["name_prefix"],
        comparison_fields=[
            ("address", "address_similarity", 0.7)
        ],
        min_overall_score=0.7
    )
    
    engine = MatchEngine(config)
    matches = engine.find_matches(sample_records)
    
    # "123 Main St" and "123 Main Street" should match
    assert any(
        m for m in matches 
        if m.record1["id"] == "CRM_001" and m.record2["id"] == "ERP_101"
    )

def test_match_engine_multiple_criteria(sample_records):
    """Test matching with multiple criteria and weights."""
    config = MatchConfig(
        blocking_keys=["name_prefix"],
        comparison_fields=[
            ("name", "fuzzy", 0.4),
            ("address", "address_similarity", 0.4),
            ("phone", "exact", 0.2)
        ],
        min_overall_score=0.8
    )
    
    engine = MatchEngine(config)
    matches = engine.find_matches(sample_records)
    
    # Check that matches have appropriate scores
    for match in matches:
        assert match.score >= 0.8
        assert hasattr(match, 'field_scores')  # Should have individual field scores

def test_match_engine_no_matches():
    """Test behavior when no matches are found."""
    records = [
        {"id": "1", "name": "ABC Corp", "phone": "111"},
        {"id": "2", "name": "XYZ Ltd", "phone": "222"}
    ]
    
    config = MatchConfig(
        blocking_keys=["phone"],
        comparison_fields=[
            ("name", "exact", 1.0)
        ],
        min_overall_score=0.9
    )
    
    engine = MatchEngine(config)
    matches = engine.find_matches(records)
    
    assert len(matches) == 0

def test_match_engine_performance(benchmark):
    """Test matching performance with larger dataset."""
    from faker import Faker
    fake = Faker()
    
    # Generate 1000 test records
    records = [
        {
            "id": f"TEST_{i}",
            "name": fake.company(),
            "address": fake.address(),
            "phone": fake.phone_number()
        }
        for i in range(1000)
    ]
    
    config = MatchConfig(
        blocking_keys=["name_prefix"],
        comparison_fields=[
            ("name", "fuzzy", 0.5),
            ("address", "address_similarity", 0.5)
        ],
        min_overall_score=0.8
    )
    
    engine = MatchEngine(config)
    
    # Benchmark the matching operation
    def run_matching():
        return engine.find_matches(records)
    
    result = benchmark(run_matching)
    assert result is not None  # Ensure the operation completed 