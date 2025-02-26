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
    """Tracks record history and transformations."""
    
    def __init__(self):
        self._history: Dict[str, List[Dict[str, Any]]] = {}
    
    def track_merge(
        self,
        source_records: List[str],
        target_id: str,
        user_id: str,
        confidence_score: float,
        details: Dict[str, Any]
    ):
        """Track a merge operation.
        
        Args:
            source_records: List of source record IDs
            target_id: ID of the resulting golden record
            user_id: ID of the user who performed the merge
            confidence_score: Confidence score of the merge
            details: Additional merge details
        """
        event = {
            "type": "merge",
            "timestamp": datetime.utcnow().isoformat(),
            "source_records": source_records,
            "target_id": target_id,
            "user_id": user_id,
            "confidence_score": confidence_score,
            "details": details
        }
        
        if target_id not in self._history:
            self._history[target_id] = []
        self._history[target_id].append(event)
    
    def get_record_history(self, record_id: str) -> List[Dict[str, Any]]:
        """Get the history of a record.
        
        Args:
            record_id: ID of the record
            
        Returns:
            List of history events
        """
        return self._history.get(record_id, [])
    
    def export(self) -> Dict[str, List[Dict[str, Any]]]:
        """Export all lineage data.
        
        Returns:
            Dictionary of record histories
        """
        return self._history
