"""
Example script demonstrating how to use OpenMatch for MDM operations with relationships.
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

from openmatch.match.engine import MatchEngine
from openmatch.match.db_ops import DatabaseOptimizer, BatchProcessor
from openmatch.model.entity import EntityManager
from openmatch.model.config import DataModelConfig
from openmatch.etl.manager import SourceSystemManager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def create_db_connection(config):
    """Create a database connection using the configuration."""
    connection_string = (
        f"postgresql://{config['USER']}:{config['PASSWORD']}"
        f"@{config['HOST']}:{config['PORT']}/{config['NAME']}"
    )
    return create_engine(connection_string)

def initialize_mdm():
    """Initialize the MDM system."""
    # Import settings
    from config.settings import (
        MDM_DB, 
        MATCH_SETTINGS, 
        VECTOR_SETTINGS, 
        MDM_MODEL,
        SOURCE_SYSTEMS
    )

    # Create database connection
    engine = create_db_connection(MDM_DB)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Initialize entity manager with data model
    entity_manager = EntityManager(session)
    entity_manager.initialize_model(MDM_MODEL)

    # Load source system data
    source_system_manager = SourceSystemManager(
        session=session,
        entity_manager=entity_manager,
        config=SOURCE_SYSTEMS
    )
    source_system_manager.sync_entities()


    # Initialize match engine with relationship support
    match_engine = MatchEngine(
        session,
        match_settings=MATCH_SETTINGS,
        vector_settings=VECTOR_SETTINGS,
        entity_manager=entity_manager
    )
    
    return session, match_engine, entity_manager

def process_matches(session, match_engine):
    """Process matches in batch."""
    processor = BatchProcessor(session, match_engine)
    results = processor.process_matches()
    return results

def find_matches_for_record(match_engine, record_data):
    """Find matches for a specific record with related entities."""
    matches = match_engine.find_candidates(record_data)
    return matches

def main():
    # Initialize MDM system
    session, match_engine, entity_manager = initialize_mdm()

    try:
        # Example: Process all pending matches
        print("Processing matches...")
        results = process_matches(session, match_engine)
        print(f"Processed {len(results)} potential matches")

        # Example: Find matches for a sample record with related entities
        sample_record = {
            'person': {
                'first_name': 'John',
                'last_name': 'Smith',
                'email': 'john.smith@example.com',
                'birth_date': '1990-01-01'
            },
            'addresses': [
                {
                    'address_type': 'HOME',
                    'street_1': '123 Main St',
                    'city': 'Boston',
                    'state': 'MA',
                    'postal_code': '02108',
                    'country': 'USA'
                }
            ],
            'phones': [
                {
                    'phone_type': 'MOBILE',
                    'phone_number': '+1-617-555-0123',
                    'country_code': 'US'
                }
            ]
        }

        print("\nFinding matches for sample record...")
        matches = find_matches_for_record(match_engine, sample_record)
        
        print("\nMatch Results:")
        for match_id, similarity, details in matches:
            print(f"\nMatch ID: {match_id}")
            print(f"Overall Similarity Score: {similarity:.2f}")
            if details:
                print("Match Details:")
                print(f"  Person Match Score: {details['person_score']:.2f}")
                if 'address_scores' in details:
                    print("  Address Matches:")
                    for addr_score in details['address_scores']:
                        print(f"    - Score: {addr_score:.2f}")
                if 'phone_scores' in details:
                    print("  Phone Matches:")
                    for phone_score in details['phone_scores']:
                        print(f"    - Score: {phone_score:.2f}")

    finally:
        session.close()

def example_source_system_sync():
    """Example of synchronizing data from a source system."""
    from config.settings import SOURCE_SYSTEMS
    
    # Get CRM system configuration
    crm_config = SOURCE_SYSTEMS['crm_system']
    
    # Create source system connection
    source_engine = create_db_connection(crm_config)
    source_session = sessionmaker(bind=source_engine)()
    
    try:
        # Initialize the source system sync manager
        sync_manager = SourceSystemManager(
            source_session=source_session,
            mdm_session=session,
            entity_manager=entity_manager,
            config=crm_config
        )
        
        # Perform the sync
        stats = sync_manager.sync_entities()
        
        print("\nSync Statistics:")
        print(f"Total Records Processed: {stats['total_processed']}")
        print(f"New Records: {stats['new_records']}")
        print(f"Updated Records: {stats['updated_records']}")
        print(f"Related Entities Synced:")
        for entity_type, count in stats['related_entities'].items():
            print(f"  - {entity_type}: {count}")
            
    finally:
        source_session.close()

if __name__ == "__main__":
    main()
    # Uncomment to run source system sync example
    # example_source_system_sync() 