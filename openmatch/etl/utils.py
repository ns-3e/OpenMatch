"""
Utility functions for ETL operations.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import hashlib
import json

def generate_entity_id(record: Dict[str, Any], source_system_id: str) -> str:
    """
    Generate a deterministic ID for an entity based on its source data.
    
    Args:
        record: Dictionary containing entity data
        source_system_id: Source system identifier
        
    Returns:
        A deterministic hash string to use as the entity ID
    """
    # Create a copy of the record to avoid modifying the original
    id_data = record.copy()
    
    # Remove any existing ID fields that might interfere with hash generation
    id_data.pop('id', None)
    id_data.pop('_id', None)
    
    # Add source system ID to ensure uniqueness across systems
    id_data['_source_system'] = source_system_id
    
    # Create a deterministic string representation
    sorted_data = json.dumps(id_data, sort_keys=True)
    
    # Generate hash
    return hashlib.sha256(sorted_data.encode()).hexdigest()

def get_last_sync_time(
    session: Any,
    source_system_id: str,
    entity_name: str
) -> Optional[datetime]:
    """
    Get the last successful sync time for a given source system and entity.
    
    Args:
        session: Database session
        source_system_id: Source system identifier
        entity_name: Name of the entity type
        
    Returns:
        Datetime of last successful sync or None if no previous sync
    """
    try:
        result = session.execute(
            """
            SELECT MAX(updated_at)
            FROM mdm.sync_history
            WHERE source_system_id = :source_system_id
            AND entity_name = :entity_name
            AND status = 'SUCCESS'
            """,
            {
                'source_system_id': source_system_id,
                'entity_name': entity_name
            }
        ).scalar()
        return result
    except Exception:
        return None

def update_sync_history(
    session: Any,
    source_system_id: str,
    entity_name: str,
    status: str,
    stats: Dict[str, Any]
) -> None:
    """
    Update the sync history with results of the latest sync operation.
    
    Args:
        session: Database session
        source_system_id: Source system identifier
        entity_name: Name of the entity type
        status: Sync status ('SUCCESS' or 'FAILED')
        stats: Dictionary containing sync statistics
    """
    try:
        session.execute(
            """
            INSERT INTO mdm.sync_history (
                source_system_id,
                entity_name,
                status,
                records_processed,
                new_records,
                updated_records,
                failed_records,
                start_time,
                end_time,
                stats_json
            ) VALUES (
                :source_system_id,
                :entity_name,
                :status,
                :records_processed,
                :new_records,
                :updated_records,
                :failed_records,
                :start_time,
                :end_time,
                :stats_json
            )
            """,
            {
                'source_system_id': source_system_id,
                'entity_name': entity_name,
                'status': status,
                'records_processed': stats['total_processed'],
                'new_records': stats['new_records'],
                'updated_records': stats['updated_records'],
                'failed_records': stats['failed_records'],
                'start_time': stats['start_time'],
                'end_time': stats['end_time'],
                'stats_json': json.dumps(stats)
            }
        )
        session.commit()
    except Exception as e:
        session.rollback()
        raise 