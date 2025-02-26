import pytest
from datetime import datetime
from openmatch import MDMPipeline
from openmatch.config import TrustConfig, SurvivorshipRules
from openmatch.match import MatchConfig

def test_end_to_end_pipeline(sample_records):
    """Test the complete MDM pipeline from matching through golden record creation."""
    # Configure the pipeline
    match_config = MatchConfig(
        blocking_keys=["name_prefix"],
        comparison_fields=[
            ("name", "fuzzy", 0.4),
            ("address", "address_similarity", 0.4),
            ("phone", "exact", 0.2)
        ],
        min_overall_score=0.8
    )
    
    trust_config = TrustConfig(
        source_reliability={
            "CRM": 0.9,
            "ERP": 0.8,
            "LEGACY": 0.6
        }
    )
    
    survivorship_rules = SurvivorshipRules(
        priority_fields={
            "name": ["CRM", "ERP", "LEGACY"],
            "address": ["ERP", "CRM", "LEGACY"],
            "phone": ["CRM", "ERP", "LEGACY"]
        }
    )
    
    # Initialize pipeline
    pipeline = MDMPipeline(
        match_config=match_config,
        trust_config=trust_config,
        survivorship_rules=survivorship_rules
    )
    
    # Process records
    result = pipeline.process_records(sample_records)
    
    # Verify matches were found
    assert len(result.matches) > 0
    
    # Verify golden records were created
    assert len(result.golden_records) > 0
    
    # Verify cross-references were created
    assert len(result.xrefs) > 0
    
    # Verify lineage was tracked
    assert len(result.lineage) > 0
    
    # Verify trust scores were calculated
    assert all(hasattr(record, 'trust_score') for record in result.golden_records)

def test_incremental_processing():
    """Test incremental processing of new records."""
    initial_records = [
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
        }
    ]
    
    new_records = [
        {
            "id": "CRM_002",
            "source": "CRM",
            "name": "Acme Corp",
            "address": "456 New St",
            "phone": "555-0102",
            "last_updated": "2024-02-26"
        }
    ]
    
    pipeline = MDMPipeline(
        match_config=MatchConfig(
            blocking_keys=["name_prefix"],
            comparison_fields=[
                ("name", "fuzzy", 0.8),
                ("phone", "exact", 0.2)
            ],
            min_overall_score=0.7
        ),
        trust_config=TrustConfig(
            source_reliability={
                "CRM": 0.9,
                "ERP": 0.8
            }
        ),
        survivorship_rules=SurvivorshipRules(
            priority_fields={
                "name": ["CRM", "ERP"],
                "address": ["CRM", "ERP"],
                "phone": ["CRM", "ERP"]
            }
        )
    )
    
    # Initial processing
    initial_result = pipeline.process_records(initial_records)
    
    # Incremental processing
    incremental_result = pipeline.process_records(
        new_records,
        existing_golden_records=initial_result.golden_records,
        existing_xrefs=initial_result.xrefs
    )
    
    # Verify new matches were found
    assert len(incremental_result.matches) > 0
    
    # Verify golden records were updated
    assert len(incremental_result.golden_records) >= len(initial_result.golden_records)
    
    # Verify cross-references were updated
    assert len(incremental_result.xrefs) > len(initial_result.xrefs)

def test_conflict_resolution_pipeline():
    """Test the pipeline's handling of conflicting data."""
    records = [
        {
            "id": "CRM_001",
            "source": "CRM",
            "name": "Acme Corp",
            "revenue": 1000000,
            "last_updated": "2024-02-25",
            "confidence": 0.9
        },
        {
            "id": "ERP_101",
            "source": "ERP",
            "name": "ACME Corporation",
            "revenue": 1100000,
            "last_updated": "2024-02-24",
            "confidence": 0.95
        }
    ]
    
    def revenue_resolution_rule(values, sources, records):
        """Custom rule to resolve revenue conflicts."""
        # Use value with highest confidence
        confidences = [r["confidence"] for r in records]
        max_conf_idx = confidences.index(max(confidences))
        return values[max_conf_idx], sources[max_conf_idx]
    
    pipeline = MDMPipeline(
        match_config=MatchConfig(
            blocking_keys=["name_prefix"],
            comparison_fields=[("name", "fuzzy", 1.0)],
            min_overall_score=0.8
        ),
        trust_config=TrustConfig(
            source_reliability={
                "CRM": 0.9,
                "ERP": 0.8
            }
        ),
        survivorship_rules=SurvivorshipRules(
            priority_fields={
                "name": ["CRM", "ERP"]
            },
            custom_rules={
                "revenue": revenue_resolution_rule
            }
        )
    )
    
    result = pipeline.process_records(records)
    
    # Verify golden record has correct revenue (from ERP due to higher confidence)
    golden_record = result.golden_records[0]
    assert golden_record["revenue"] == 1100000
    
    # Verify name comes from CRM (due to priority rules)
    assert golden_record["name"] == "Acme Corp"

def test_pipeline_error_handling():
    """Test the pipeline's error handling capabilities."""
    invalid_records = [
        {
            "id": "CRM_001",
            "source": "CRM",
            "name": "Acme Corp",
            "last_updated": "invalid-date"  # Invalid date format
        },
        {
            "id": "ERP_101",
            "source": "UNKNOWN",  # Invalid source
            "name": "ACME Corporation"
        }
    ]
    
    pipeline = MDMPipeline(
        match_config=MatchConfig(
            blocking_keys=["name_prefix"],
            comparison_fields=[("name", "fuzzy", 1.0)],
            min_overall_score=0.8
        ),
        trust_config=TrustConfig(
            source_reliability={
                "CRM": 0.9,
                "ERP": 0.8
            }
        ),
        survivorship_rules=SurvivorshipRules(
            priority_fields={
                "name": ["CRM", "ERP"]
            }
        )
    )
    
    # Pipeline should handle invalid records gracefully
    with pytest.raises(ValueError) as exc_info:
        pipeline.process_records(invalid_records)
    
    assert "Invalid date format" in str(exc_info.value)
    
    # Test with empty record set
    result = pipeline.process_records([])
    assert len(result.golden_records) == 0
    assert len(result.matches) == 0

def test_pipeline_performance(benchmark):
    """Test the performance of the MDM pipeline."""
    from faker import Faker
    fake = Faker()
    
    # Generate 1000 test records
    records = [
        {
            "id": f"TEST_{i}",
            "source": "CRM" if i % 2 == 0 else "ERP",
            "name": fake.company(),
            "address": fake.address(),
            "phone": fake.phone_number(),
            "last_updated": fake.date_between(
                start_date="-1y",
                end_date="today"
            ).strftime("%Y-%m-%d")
        }
        for i in range(1000)
    ]
    
    pipeline = MDMPipeline(
        match_config=MatchConfig(
            blocking_keys=["name_prefix"],
            comparison_fields=[
                ("name", "fuzzy", 0.4),
                ("address", "address_similarity", 0.4),
                ("phone", "exact", 0.2)
            ],
            min_overall_score=0.8
        ),
        trust_config=TrustConfig(
            source_reliability={
                "CRM": 0.9,
                "ERP": 0.8
            }
        ),
        survivorship_rules=SurvivorshipRules(
            priority_fields={
                "name": ["CRM", "ERP"],
                "address": ["ERP", "CRM"],
                "phone": ["CRM", "ERP"]
            }
        )
    )
    
    # Benchmark the complete pipeline
    def run_pipeline():
        return pipeline.process_records(records)
    
    result = benchmark(run_pipeline)
    assert result is not None
    assert len(result.golden_records) > 0 