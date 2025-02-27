"""
OpenMatch Lineage Module - Tracks record history and transformations.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime

from .tracker import (
    LineageEventType,
    LineageEvent,
    LineageTracker
)

from .history import (
    HistoryEntry,
    RecordHistory
)

from .xref import (
    RelationType,
    CrossReference,
    CrossReferenceManager
)

__all__ = [
    # Lineage Tracking
    'LineageEventType',
    'LineageEvent',
    'LineageTracker',
    
    # History Management
    'HistoryEntry',
    'RecordHistory',
    
    # Cross References
    'RelationType',
    'CrossReference',
    'CrossReferenceManager'
]

class LineageTracker:
    """Tracks record lineage and merge history."""
    
    def __init__(self):
        self.merge_history = []

    def track_merge(self, source_records: List[Dict[str, Any]], golden_record: Dict[str, Any]) -> None:
        """
        Track a merge operation.
        
        Args:
            source_records: List of source records that were merged
            golden_record: Resulting golden record
        """
        merge_event = {
            'timestamp': datetime.now().isoformat(),
            'golden_record_id': golden_record['id'],
            'source_record_ids': [r['id'] for r in source_records],
            'source_systems': list(set(r.get('source') for r in source_records)),
            'field_sources': self._track_field_sources(source_records, golden_record)
        }
        
        self.merge_history.append(merge_event)

    def _track_field_sources(
        self,
        source_records: List[Dict[str, Any]],
        golden_record: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Track which source record each field in the golden record came from."""
        field_sources = {}
        
        for field, value in golden_record.items():
            if field == 'id':  # Skip ID field
                continue
                
            # Find which source record this value came from
            for record in source_records:
                if record.get(field) == value:
                    field_sources[field] = {
                        'source_record_id': record['id'],
                        'source_system': record.get('source'),
                        'original_value': value
                    }
                    break
        
        return field_sources

    def get_record_history(self, record_id: str) -> List[Dict[str, Any]]:
        """
        Get the merge history for a record.
        
        Args:
            record_id: ID of the record to get history for
            
        Returns:
            List of merge events involving this record
        """
        history = []
        
        for event in self.merge_history:
            if (record_id == event['golden_record_id'] or
                record_id in event['source_record_ids']):
                history.append(event)
        
        return history

    def export(self):
        """Export lineage data."""
        return self.merge_history
