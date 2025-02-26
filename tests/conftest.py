import pytest
from datetime import datetime
from typing import Dict, List

from openmatch import MDMPipeline
from openmatch.config import TrustConfig, SurvivorshipRules

@pytest.fixture
def sample_records() -> List[Dict]:
    """Fixture providing sample test records from different sources."""
    return [
        {
            "id": "CRM_001",
            "source": "CRM",
            "name": "Acme Corp",
            "address": "123 Main St",
            "phone": "555-0101",
            "last_updated": "2024-02-25"
        },
        {
            "id": "ERP_101",
            "source": "ERP",
            "name": "ACME Corporation",
            "address": "123 Main Street",
            "phone": "555-0101",
            "last_updated": "2024-02-24"
        },
        {
            "id": "LEGACY_201",
            "source": "LEGACY",
            "name": "Acme Corp Ltd",
            "address": "123 Main St Suite 100",
            "phone": "5550101",
            "last_updated": "2024-01-15"
        }
    ]

@pytest.fixture
def trust_config() -> TrustConfig:
    """Fixture providing a standard trust configuration."""
    return TrustConfig(
        source_reliability={
            "CRM": 0.9,
            "ERP": 0.8,
            "LEGACY": 0.6
        }
    )

@pytest.fixture
def survivorship_rules() -> SurvivorshipRules:
    """Fixture providing standard survivorship rules."""
    return SurvivorshipRules(
        priority_fields={
            "name": ["CRM", "ERP", "LEGACY"],
            "address": ["ERP", "CRM", "LEGACY"],
            "phone": ["CRM", "ERP", "LEGACY"]
        }
    )

@pytest.fixture
def mdm_pipeline(trust_config, survivorship_rules) -> MDMPipeline:
    """Fixture providing a configured MDM pipeline."""
    return MDMPipeline(
        trust_config=trust_config,
        survivorship_rules=survivorship_rules
    )

@pytest.fixture
def mock_timestamp():
    """Fixture providing a fixed timestamp for testing."""
    return datetime(2024, 2, 25, 12, 0, 0)
