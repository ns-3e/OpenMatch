import pytest
from datetime import datetime, timedelta
from openmatch.trust import TrustFramework, TrustScore

def test_trust_framework_initialization(trust_config):
    """Test that TrustFramework is initialized correctly."""
    framework = TrustFramework(trust_config)
    
    assert framework.source_reliability == {
        "CRM": 0.9,
        "ERP": 0.8,
        "LEGACY": 0.6
    }

def test_trust_score_calculation(trust_config, sample_records):
    """Test trust score calculation for individual records."""
    framework = TrustFramework(trust_config)
    
    # Calculate trust score for CRM record
    crm_record = next(r for r in sample_records if r["source"] == "CRM")
    score = framework.calculate_trust_score(crm_record)
    
    assert isinstance(score, TrustScore)
    assert 0 <= score.overall_score <= 1
    assert score.source_score == 0.9  # From trust_config
    assert hasattr(score, 'completeness_score')
    assert hasattr(score, 'timeliness_score')

def test_trust_score_completeness():
    """Test trust score calculation based on record completeness."""
    framework = TrustFramework(TrustConfig(
        source_reliability={"TEST": 1.0}
    ))
    
    complete_record = {
        "source": "TEST",
        "id": "1",
        "name": "Test Corp",
        "address": "123 Test St",
        "phone": "555-0000",
        "last_updated": "2024-02-25"
    }
    
    incomplete_record = {
        "source": "TEST",
        "id": "2",
        "name": "Test Corp",
        # Missing address
        # Missing phone
        "last_updated": "2024-02-25"
    }
    
    complete_score = framework.calculate_trust_score(complete_record)
    incomplete_score = framework.calculate_trust_score(incomplete_record)
    
    assert complete_score.completeness_score > incomplete_score.completeness_score
    assert complete_score.overall_score > incomplete_score.overall_score

def test_trust_score_timeliness():
    """Test trust score calculation based on record timeliness."""
    framework = TrustFramework(TrustConfig(
        source_reliability={"TEST": 1.0}
    ))
    
    now = datetime.now()
    
    recent_record = {
        "source": "TEST",
        "id": "1",
        "name": "Test Corp",
        "last_updated": now.strftime("%Y-%m-%d")
    }
    
    old_record = {
        "source": "TEST",
        "id": "2",
        "name": "Test Corp",
        "last_updated": (now - timedelta(days=365)).strftime("%Y-%m-%d")
    }
    
    recent_score = framework.calculate_trust_score(recent_record)
    old_score = framework.calculate_trust_score(old_record)
    
    assert recent_score.timeliness_score > old_score.timeliness_score
    assert recent_score.overall_score > old_score.overall_score

def test_trust_score_source_reliability():
    """Test trust score calculation based on source reliability."""
    framework = TrustFramework(TrustConfig(
        source_reliability={
            "HIGH_TRUST": 0.9,
            "LOW_TRUST": 0.3
        }
    ))
    
    high_trust_record = {
        "source": "HIGH_TRUST",
        "id": "1",
        "name": "Test Corp"
    }
    
    low_trust_record = {
        "source": "LOW_TRUST",
        "id": "2",
        "name": "Test Corp"
    }
    
    high_score = framework.calculate_trust_score(high_trust_record)
    low_score = framework.calculate_trust_score(low_trust_record)
    
    assert high_score.source_score > low_score.source_score
    assert high_score.overall_score > low_score.overall_score

def test_trust_score_validation():
    """Test validation of trust score inputs."""
    framework = TrustFramework(TrustConfig(
        source_reliability={"TEST": 1.0}
    ))
    
    # Missing required source field
    with pytest.raises(ValueError):
        framework.calculate_trust_score({"id": "1", "name": "Test"})
    
    # Invalid source
    with pytest.raises(ValueError):
        framework.calculate_trust_score({"source": "UNKNOWN", "id": "1"})
    
    # Invalid date format
    with pytest.raises(ValueError):
        framework.calculate_trust_score({
            "source": "TEST",
            "id": "1",
            "last_updated": "invalid-date"
        })

def test_trust_framework_comparison():
    """Test comparison of trust scores between records."""
    framework = TrustFramework(TrustConfig(
        source_reliability={
            "CRM": 0.9,
            "ERP": 0.8
        }
    ))
    
    record1 = {
        "source": "CRM",
        "id": "1",
        "name": "Test Corp",
        "address": "123 Test St",
        "last_updated": "2024-02-25"
    }
    
    record2 = {
        "source": "ERP",
        "id": "2",
        "name": "Test Corp",
        "last_updated": "2024-02-24"
    }
    
    score1 = framework.calculate_trust_score(record1)
    score2 = framework.calculate_trust_score(record2)
    
    comparison = framework.compare_trust_scores(score1, score2)
    assert comparison > 0  # record1 should be more trusted
    
    # Test symmetry
    assert framework.compare_trust_scores(score2, score1) < 0 