"""
Example script demonstrating how to use OpenMatch for MDM operations with relationships.
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

from openmatch.match.engine import MatchEngine
from openmatch.match.config import (
    MatchConfig,
    MatchType,
    MetadataConfig,
    BlockingConfig,
    MatchRuleConfig,
    FieldConfig
)
from openmatch.match.settings import DatabaseConfig
from openmatch.match.system_check import print_model_recommendations
from openmatch.model.entity import EntityManager
from openmatch.model.config import DataModelConfig
from openmatch.manage import MDMManager

def initialize_mdm():
    """Initialize the MDM system."""
    logger.info("Starting MDM initialization...")
    
    # Import settings
    from config.settings import (
        MDM_DB, 
        MATCH_SETTINGS, 
        VECTOR_SETTINGS, 
        MDM_MODEL,
        SOURCE_SYSTEMS,
        MODEL_SETTINGS
    )
    
    logger.debug("Loaded configuration settings")

    # Create database configuration
    db_config = DatabaseConfig.from_dict(MDM_DB)
    logger.debug("Created database configuration")

    # Initialize MDM system
    mdm_manager = MDMManager(db_config)
    mdm_manager.initialize_system(MDM_MODEL)
    logger.info("MDM system initialized successfully!")

    # Initialize match engine with relationship support
    logger.info("Initializing match engine...")
    blocking_config = BlockingConfig(
        blocking_keys=VECTOR_SETTINGS['BLOCKING_KEYS'],
        block_size_limit=VECTOR_SETTINGS.get('BLOCK_SIZE_LIMIT', 1000),
        vector_similarity_threshold=VECTOR_SETTINGS.get('SIMILARITY_THRESHOLD', 0.8)
    )
    
    metadata_config = MetadataConfig(
        schema=MDM_DB.get('SCHEMA', 'public')
    )
    
    match_config = MatchConfig(
        rules=MATCH_SETTINGS['RULES'],
        blocking=blocking_config,
        metadata=metadata_config,
        use_gpu=MODEL_SETTINGS.get('USE_GPU', False)
    )
    
    match_engine = MatchEngine(
        config=match_config,
        db_config=db_config
    )
    logger.debug("Match engine initialized")
    
    return mdm_manager, match_engine

def load_sample_data(mdm_manager):
    """Load sample data into the MDM system."""
    logger.info("Loading sample data...")
    
    # Example records with relationships
    sample_records = [
        {
            'id': 'PERSON001',
            'entity_type': 'person',
            'data': {
                'first_name': 'John',
                'last_name': 'Smith',
                'email': 'john.smith@example.com',
                'birth_date': '1990-01-01',
                'addresses': [
                    {
                        'type': 'HOME',
                        'street_1': '123 Main St',
                        'city': 'Boston',
                        'state': 'MA',
                        'postal_code': '02108',
                        'country': 'USA'
                    }
                ],
                'phones': [
                    {
                        'type': 'MOBILE',
                        'phone_number': '+1-617-555-0123',
                        'country_code': 'US'
                    }
                ]
            }
        },
        {
            'id': 'PERSON002',
            'entity_type': 'person',
            'data': {
                'first_name': 'Jane',
                'last_name': 'Doe',
                'email': 'jane.doe@example.com',
                'birth_date': '1985-05-15',
                'addresses': [
                    {
                        'type': 'HOME',
                        'street_1': '456 Oak Ave',
                        'city': 'Cambridge',
                        'state': 'MA',
                        'postal_code': '02139',
                        'country': 'USA'
                    }
                ],
                'phones': [
                    {
                        'type': 'MOBILE',
                        'phone_number': '+1-617-555-0456',
                        'country_code': 'US'
                    }
                ]
            }
        }
    ]
    
    # Load records into MDM system
    mdm_manager.load_source_records(sample_records, source_system='EXAMPLE_SYSTEM')
    logger.info(f"Loaded {len(sample_records)} sample records")

def process_matches(match_engine, batch_size=100):
    """Process matches in batch."""
    logger.info("Processing batch of records...")
    
    try:
        # Get unmatched records
        records = match_engine.get_unmatched_records(limit=batch_size)
        if not records:
            logger.info("No unmatched records found")
            return []
            
        logger.debug(f"Processing {len(records)} records")
        results = match_engine.process_batch(records)
        
        # Log match results
        for record_id, matches in results.items():
            if matches:
                logger.info(f"Found {len(matches)} matches for record {record_id}")
                for match_id, score in matches:
                    logger.debug(f"Match: {match_id} (score: {score:.2f})")
            else:
                logger.debug(f"No matches found for record {record_id}")
                
        return results
        
    except Exception as e:
        logger.error(f"Error processing matches: {e}", exc_info=True)
        raise

def merge_records(match_engine, matches):
    """Merge matched records."""
    logger.info("Merging matched records...")
    
    try:
        for record_id, record_matches in matches.items():
            if not record_matches:
                continue
                
            # Get the best match
            best_match_id, best_score = record_matches[0]
            
            if best_score >= 0.9:  # High confidence match
                logger.info(f"Merging record {record_id} with {best_match_id} (score: {best_score:.2f})")
                match_engine.merge_records(record_id, best_match_id)
            else:
                logger.debug(f"Match score too low for automatic merge: {best_score:.2f}")
                
    except Exception as e:
        logger.error(f"Error merging records: {e}", exc_info=True)
        raise

def main():
    try:
        logger.info("Starting MDM example...")
        
        # Show system recommendations
        print("\n=== System Analysis and Model Recommendations ===")
        print_model_recommendations()
        print("\nPress Enter to continue with the recommended model, or Ctrl+C to exit...")
        input()
        
        # Initialize MDM system
        mdm_manager, match_engine = initialize_mdm()

        try:
            # Load sample data
            load_sample_data(mdm_manager)

            # Process matches
            print("\nProcessing matches...")
            matches = process_matches(match_engine)
            
            if matches:
                # Merge records with high confidence matches
                merge_records(match_engine, matches)
                print(f"\nProcessed and merged {len(matches)} record sets")
            else:
                print("\nNo matches found in this batch")

        finally:
            mdm_manager.close()
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error in MDM example: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 