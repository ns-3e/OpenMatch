import pytest
from datetime import datetime
from openmatch.config import SurvivorshipRules
from openmatch.survivorship import SurvivorshipEngine

def test_survivorship_rules_initialization(survivorship_rules):
    """Test that survivorship rules are initialized correctly."""
    assert survivorship_rules.priority_fields == {
        "name": ["CRM", "ERP", "LEGACY"],
        "address": ["ERP", "CRM", "LEGACY"],
        "phone": ["CRM", "ERP", "LEGACY"]
    }

def test_survivorship_engine_creation():
    """Test creation of survivorship engine with rules."""
    rules = SurvivorshipRules(
        priority_fields={
            "name": ["CRM", "ERP"],
            "address": ["ERP", "CRM"]
        }
    )
    
    engine = SurvivorshipEngine(rules)
    assert engine.rules == rules

def test_basic_survivorship(sample_records):
    """Test basic survivorship rules application."""
    rules = SurvivorshipRules(
        priority_fields={
            "name": ["CRM", "ERP", "LEGACY"],
            "address": ["ERP", "CRM", "LEGACY"],
            "phone": ["CRM", "ERP", "LEGACY"]
        }
    )
    
    engine = SurvivorshipEngine(rules)
    golden_record = engine.create_golden_record(sample_records)
    
    # Name should come from CRM (highest priority for name)
    assert golden_record["name"] == "Acme Corp"
    
    # Address should come from ERP (highest priority for address)
    assert golden_record["address"] == "123 Main Street"
    
    # Phone should come from CRM (highest priority for phone)
    assert golden_record["phone"] == "555-0101"

def test_survivorship_with_missing_data():
    """Test survivorship when highest priority source has missing data."""
    records = [
        {
            "id": "CRM_001",
            "source": "CRM",
            "name": "Acme Corp",
            # Missing address
            "phone": "555-0101"
        },
        {
            "id": "ERP_101",
            "source": "ERP",
            "name": "ACME Corporation",
            "address": "123 Main Street",
            "phone": "555-0101"
        }
    ]
    
    rules = SurvivorshipRules(
        priority_fields={
            "name": ["CRM", "ERP"],
            "address": ["CRM", "ERP"],
            "phone": ["CRM", "ERP"]
        }
    )
    
    engine = SurvivorshipEngine(rules)
    golden_record = engine.create_golden_record(records)
    
    # Name from CRM (highest priority)
    assert golden_record["name"] == "Acme Corp"
    # Address from ERP (CRM missing address)
    assert golden_record["address"] == "123 Main Street"
    # Phone from CRM (highest priority)
    assert golden_record["phone"] == "555-0101"

def test_survivorship_with_custom_rules():
    """Test survivorship with custom rule functions."""
    records = [
        {
            "id": "1",
            "source": "CRM",
            "value": 100,
            "last_updated": "2024-02-25"
        },
        {
            "id": "2",
            "source": "ERP",
            "value": 200,
            "last_updated": "2024-02-24"
        }
    ]
    
    def max_value_rule(values, sources):
        """Custom rule to select maximum value."""
        max_val = max(values)
        return max_val, sources[values.index(max_val)]
    
    rules = SurvivorshipRules(
        priority_fields={},
        custom_rules={
            "value": max_value_rule
        }
    )
    
    engine = SurvivorshipEngine(rules)
    golden_record = engine.create_golden_record(records)
    
    # Should select maximum value regardless of source
    assert golden_record["value"] == 200

def test_survivorship_with_timestamps():
    """Test survivorship using most recent record."""
    records = [
        {
            "id": "1",
            "source": "CRM",
            "name": "Old Name",
            "last_updated": "2024-01-01"
        },
        {
            "id": "2",
            "source": "CRM",
            "name": "New Name",
            "last_updated": "2024-02-25"
        }
    ]
    
    def most_recent_rule(values, sources, records):
        """Custom rule to select most recent value."""
        dates = [datetime.strptime(r["last_updated"], "%Y-%m-%d") 
                for r in records]
        most_recent_idx = dates.index(max(dates))
        return values[most_recent_idx], sources[most_recent_idx]
    
    rules = SurvivorshipRules(
        priority_fields={},
        custom_rules={
            "name": most_recent_rule
        }
    )
    
    engine = SurvivorshipEngine(rules)
    golden_record = engine.create_golden_record(records)
    
    # Should select most recent name
    assert golden_record["name"] == "New Name"

def test_survivorship_validation():
    """Test validation of survivorship rules and inputs."""
    # Empty priority fields
    with pytest.raises(ValueError):
        SurvivorshipRules(priority_fields={})
    
    # Invalid source in priority list
    with pytest.raises(ValueError):
        SurvivorshipRules(
            priority_fields={
                "name": ["INVALID_SOURCE"]
            }
        )
    
    # Duplicate sources in priority list
    with pytest.raises(ValueError):
        SurvivorshipRules(
            priority_fields={
                "name": ["CRM", "CRM"]
            }
        )

def test_survivorship_conflict_resolution():
    """Test conflict resolution in survivorship rules."""
    records = [
        {
            "id": "1",
            "source": "CRM",
            "name": "Acme Corp",
            "confidence": 0.9
        },
        {
            "id": "2",
            "source": "ERP",
            "name": "ACME Corporation",
            "confidence": 0.95
        }
    ]
    
    def confidence_based_rule(values, sources, records):
        """Custom rule to select value with highest confidence."""
        confidences = [r["confidence"] for r in records]
        max_conf_idx = confidences.index(max(confidences))
        return values[max_conf_idx], sources[max_conf_idx]
    
    rules = SurvivorshipRules(
        priority_fields={},
        custom_rules={
            "name": confidence_based_rule
        }
    )
    
    engine = SurvivorshipEngine(rules)
    golden_record = engine.create_golden_record(records)
    
    # Should select name from record with highest confidence
    assert golden_record["name"] == "ACME Corporation" 