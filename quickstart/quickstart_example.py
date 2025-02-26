#!/usr/bin/env python3
"""
OpenMatch Quickstart Example
This script demonstrates the core functionality of OpenMatch for master data management.
"""

from openmatch import MDMPipeline
from openmatch.config import TrustConfig, SurvivorshipRules
from openmatch.match import MatchConfig, MatchEngine
from openmatch.trust import TrustFramework
from openmatch.lineage import LineageTracker

def main():
    # 1. Set up the MDM pipeline with configurations
    pipeline = setup_pipeline()
    
    # 2. Load sample data
    records = load_sample_data()
    
    # 3. Process records and get results
    results = process_records(pipeline, records)
    
    # 4. Analyze and display results
    display_results(results)

def setup_pipeline():
    """Configure and initialize the MDM pipeline."""
    
    # Trust configuration
    trust_config = TrustConfig(
        source_reliability={
            "CRM": 0.9,
            "ERP": 0.8,
            "LEGACY": 0.6
        }
    )
    
    # Survivorship rules
    survivorship_rules = SurvivorshipRules(
        priority_fields={
            "name": ["CRM", "ERP", "LEGACY"],
            "email": ["CRM", "ERP", "LEGACY"],
            "phone": ["ERP", "CRM", "LEGACY"],
            "address": ["CRM", "ERP", "LEGACY"]
        }
    )
    
    # Match configuration
    match_config = MatchConfig(
        blocking_keys=["postal_code", "name_prefix"],
        comparison_fields=[
            ("email", "exact", 1.0),
            ("name", "fuzzy", 0.8),
            ("phone", "phonetic", 0.7),
            ("address", "address_similarity", 0.6)
        ],
        min_overall_score=0.85
    )
    
    # Initialize pipeline
    pipeline = MDMPipeline(
        trust_config=trust_config,
        survivorship_rules=survivorship_rules,
        match_config=match_config
    )
    
    return pipeline

def load_sample_data():
    """Load sample customer records from different sources."""
    return [
        {
            "id": "CRM_001",
            "source": "CRM",
            "name": "Acme Corporation",
            "email": "contact@acme.com",
            "phone": "555-0101",
            "address": "123 Main St, Suite 100",
            "postal_code": "94105"
        },
        {
            "id": "ERP_101",
            "source": "ERP",
            "name": "ACME Corp.",
            "email": "contact@acme.com",
            "phone": "5550101",
            "address": "123 Main Street #100",
            "postal_code": "94105"
        },
        {
            "id": "LEGACY_A1",
            "source": "LEGACY",
            "name": "Acme Corp",
            "email": "info@acme.com",
            "phone": "555-0101",
            "address": "123 Main St Suite 100",
            "postal_code": "94105"
        },
        {
            "id": "CRM_002",
            "source": "CRM",
            "name": "TechStart Solutions",
            "email": "contact@techstart.io",
            "phone": "555-0202",
            "address": "456 Innovation Ave",
            "postal_code": "94107"
        },
        {
            "id": "ERP_102",
            "source": "ERP",
            "name": "Tech Start Solutions Inc.",
            "email": "contact@techstart.io",
            "phone": "5550202",
            "address": "456 Innovation Avenue",
            "postal_code": "94107"
        }
    ]

def process_records(pipeline, records):
    """Process records through the MDM pipeline."""
    
    # Initialize lineage tracker
    lineage = LineageTracker()
    
    # Process records
    results = pipeline.process_records(records)
    
    # Track lineage
    for golden_record in results.golden_records:
        lineage.track_merge(
            source_records=results.source_records[golden_record["id"]],
            golden_record=golden_record
        )
    
    # Add lineage to results
    results.lineage = lineage
    
    return results

def display_results(results):
    """Display the results of MDM processing."""
    
    print("\n=== Golden Records ===")
    for record in results.golden_records:
        print(f"\nGolden Record ID: {record['id']}")
        print("Attributes:")
        for key, value in record.items():
            if key != "id":
                print(f"  {key}: {value}")
    
    print("\n=== Cross References ===")
    for xref in results.xrefs:
        print(f"\nGolden Record: {xref['golden_id']}")
        print("Source Records:")
        for source_ref in xref['source_refs']:
            print(f"  - {source_ref['source']}: {source_ref['id']}")
    
    print("\n=== Trust Scores ===")
    for record_id, score in results.trust_scores.items():
        print(f"Record {record_id}: {score:.2f}")
    
    print("\n=== Lineage ===")
    for golden_id in results.lineage.get_all_golden_records():
        print(f"\nGolden Record: {golden_id}")
        history = results.lineage.get_record_history(golden_id)
        for entry in history:
            print(f"  {entry['timestamp']}: {entry['action']} - {entry['details']}")

if __name__ == "__main__":
    main() 