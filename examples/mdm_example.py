"""
Example script demonstrating how to use OpenMatch for MDM operations.
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
    from config.settings import MDM_DB, MATCH_SETTINGS, VECTOR_SETTINGS

    # Create database connection
    engine = create_db_connection(MDM_DB)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Initialize components
    match_engine = MatchEngine(
        session,
        match_settings=MATCH_SETTINGS,
        vector_settings=VECTOR_SETTINGS
    )
    
    entity_manager = EntityManager(session)
    
    return session, match_engine, entity_manager

def process_matches(session, match_engine):
    """Process matches in batch."""
    processor = BatchProcessor(session, match_engine)
    results = processor.process_matches()
    return results

def find_matches_for_record(match_engine, record_data):
    """Find matches for a specific record."""
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

        # Example: Find matches for a specific record
        sample_record = {
            'first_name': 'John',
            'last_name': 'Smith',
            'email': 'john.smith@example.com',
            'birth_date': '1990-01-01'
        }

        print("\nFinding matches for sample record...")
        matches = find_matches_for_record(match_engine, sample_record)
        
        print("\nMatch Results:")
        for match_id, similarity in matches:
            print(f"Match ID: {match_id}, Similarity Score: {similarity:.2f}")

    finally:
        session.close()

if __name__ == "__main__":
    main() 