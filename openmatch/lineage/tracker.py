"""
Lineage tracker implementation for OpenMatch.
"""

from typing import Dict, List, Optional, Set, Any, Tuple
from enum import Enum
from dataclasses import dataclass
import logging
from datetime import datetime
import networkx as nx
import pandas as pd
import json

from openmatch.utils.logging import setup_logging


class LineageEventType(Enum):
    CREATE = "create"
    UPDATE = "update"
    MERGE = "merge"
    SPLIT = "split"
    DELETE = "delete"
    LINK = "link"
    UNLINK = "unlink"

@dataclass
class LineageEvent:
    event_type: LineageEventType
    timestamp: datetime
    source_ids: List[str]
    target_ids: List[str]
    user_id: str
    details: Dict[str, Any]
    confidence_score: Optional[float] = None

class LineageTracker:
    """Tracker for record history and cross-references."""
    
    def __init__(
        self,
        id_field: str = "id",
        timestamp_field: str = "last_updated",
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the lineage tracker.
        
        Args:
            id_field: Field containing record ID
            timestamp_field: Field containing record timestamp
            logger: Optional logger instance
        """
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.logger = logger or setup_logging(__name__)
        
        # Initialize graph for tracking relationships
        self.graph = nx.DiGraph()
        
        # Track golden record mappings
        self.golden_records = {}  # source_id -> golden_id
        self.source_records = {}  # golden_id -> set(source_ids)
        
        # Track record history
        self.history = {}  # record_id -> List[Dict]
        
        # Store detailed event history
        self.events: Dict[str, List[LineageEvent]] = {}
        
        # Track source system mappings
        self.source_mappings: Dict[str, Dict[str, str]] = {}
        
    def _add_history_entry(
        self,
        record_id: str,
        action: str,
        details: Dict,
        timestamp: Optional[datetime] = None
    ) -> None:
        """Add entry to record history.
        
        Args:
            record_id: ID of the record
            action: Type of action performed
            details: Additional details about the action
            timestamp: Optional timestamp (defaults to now)
        """
        if record_id not in self.history:
            self.history[record_id] = []
            
        entry = {
            "timestamp": timestamp or datetime.now(),
            "action": action,
            **details
        }
        
        self.history[record_id].append(entry)
        
    def track_create(
        self,
        record_id: str,
        user_id: str,
        source_system: str,
        details: Dict[str, Any]
    ) -> None:
        """Track creation of a new record."""
        event = LineageEvent(
            event_type=LineageEventType.CREATE,
            timestamp=datetime.utcnow(),
            source_ids=[],
            target_ids=[record_id],
            user_id=user_id,
            details={
                "source_system": source_system,
                **details
            }
        )
        self._add_event(record_id, event)
        self.graph.add_node(record_id, type="record", source_system=source_system)
        
        # Track history
        self._add_history_entry(
            record_id,
            "created",
            {
                "source_system": source_system,
                "details": details
            },
            event.timestamp
        )
        
    def track_merge(
        self,
        source_ids: List[str],
        target_id: str,
        user_id: str,
        confidence_score: float,
        details: Dict[str, Any]
    ) -> None:
        """Track merging of multiple records into a golden record."""
        event = LineageEvent(
            event_type=LineageEventType.MERGE,
            timestamp=datetime.utcnow(),
            source_ids=source_ids,
            target_ids=[target_id],
            user_id=user_id,
            confidence_score=confidence_score,
            details=details
        )
        
        # Add event to all involved records
        for record_id in source_ids + [target_id]:
            self._add_event(record_id, event)
        
        # Update graph
        for source_id in source_ids:
            self.graph.add_edge(source_id, target_id, type="merge")
            self.golden_records[source_id] = target_id
            
        # Track history
        self._add_history_entry(
            target_id,
            "merged",
            {
                "source_ids": source_ids,
                "target_id": target_id,
                "confidence_score": confidence_score,
                "details": details
            },
            event.timestamp
        )
        
    def track_update(
        self,
        record_id: str,
        user_id: str,
        changes: Dict[str, Tuple[Any, Any]],
        details: Dict[str, Any]
    ) -> None:
        """Track updates to a record."""
        event = LineageEvent(
            event_type=LineageEventType.UPDATE,
            timestamp=datetime.utcnow(),
            source_ids=[record_id],
            target_ids=[record_id],
            user_id=user_id,
            details={
                "changes": changes,
                **details
            }
        )
        self._add_event(record_id, event)
        
        # Track history
        self._add_history_entry(
            record_id,
            "updated",
            {
                "changes": changes,
                "details": details
            },
            event.timestamp
        )
        
    def track_split(
        self,
        source_id: str,
        target_ids: List[str],
        user_id: str,
        details: Dict[str, Any]
    ) -> None:
        """Track splitting of a record into multiple records."""
        event = LineageEvent(
            event_type=LineageEventType.SPLIT,
            timestamp=datetime.utcnow(),
            source_ids=[source_id],
            target_ids=target_ids,
            user_id=user_id,
            details=details
        )
        
        # Add event to all involved records
        for record_id in [source_id] + target_ids:
            self._add_event(record_id, event)
        
        # Update graph
        for target_id in target_ids:
            self.graph.add_edge(source_id, target_id, type="split")
        
        # Track history
        self._add_history_entry(
            source_id,
            "split",
            {
                "target_ids": target_ids,
                "details": details
            },
            event.timestamp
        )
        
    def track_source_mapping(
        self,
        record_id: str,
        source_system: str,
        source_id: str,
        user_id: str
    ) -> None:
        """Track mapping between master record and source system identifier."""
        if source_system not in self.source_mappings:
            self.source_mappings[source_system] = {}
        
        self.source_mappings[source_system][source_id] = record_id
        
        event = LineageEvent(
            event_type=LineageEventType.LINK,
            timestamp=datetime.utcnow(),
            source_ids=[source_id],
            target_ids=[record_id],
            user_id=user_id,
            details={
                "source_system": source_system,
                "mapping_type": "source_system_id"
            }
        )
        self._add_event(record_id, event)
        
        # Track history
        self._add_history_entry(
            record_id,
            "mapped",
            {
                "source_system": source_system,
                "source_id": source_id
            },
            event.timestamp
        )
        
    def get_golden_record(self, record_id: str) -> Optional[str]:
        """Get the golden record ID for a given record."""
        return self.golden_records.get(record_id)
        
    def get_source_records(self, golden_record_id: str) -> List[str]:
        """Get all source records that were merged into a golden record."""
        return [
            source_id for source_id, target_id in self.golden_records.items()
            if target_id == golden_record_id
        ]
        
    def get_record_history(
        self,
        record_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[LineageEvent]:
        """Get the complete history of a record."""
        events = self.events.get(record_id, [])
        
        if start_time:
            events = [e for e in events if e.timestamp >= start_time]
        if end_time:
            events = [e for e in events if e.timestamp <= end_time]
            
        return sorted(events, key=lambda x: x.timestamp)
        
    def get_lineage_graph(
        self,
        record_id: str,
        max_depth: Optional[int] = None
    ) -> nx.DiGraph:
        """Get the lineage graph for a record up to specified depth."""
        if max_depth is None:
            return nx.ego_graph(self.graph, record_id)
        return nx.ego_graph(self.graph, record_id, radius=max_depth)
        
    def export_lineage(self, record_id: str) -> Dict[str, Any]:
        """Export complete lineage information for a record."""
        graph = self.get_lineage_graph(record_id)
        history = self.get_record_history(record_id)
        
        return {
            "record_id": record_id,
            "golden_record_id": self.get_golden_record(record_id),
            "source_records": self.get_source_records(record_id),
            "history": [
                {
                    "event_type": e.event_type.value,
                    "timestamp": e.timestamp.isoformat(),
                    "source_ids": e.source_ids,
                    "target_ids": e.target_ids,
                    "user_id": e.user_id,
                    "details": e.details,
                    "confidence_score": e.confidence_score
                }
                for e in history
            ],
            "graph": {
                "nodes": list(graph.nodes()),
                "edges": list(graph.edges(data=True))
            }
        }
        
    def _add_event(self, record_id: str, event: LineageEvent) -> None:
        """Add an event to a record's history."""
        if record_id not in self.events:
            self.events[record_id] = []
        self.events[record_id].append(event)
        
    def export_history(
        self,
        format: str = "pandas"
    ) -> Union[pd.DataFrame, Dict]:
        """Export full history in specified format.
        
        Args:
            format: Output format ("pandas" or "dict")
            
        Returns:
            History in requested format
        """
        # Flatten history into list of entries
        entries = []
        for record_id, history in self.history.items():
            for entry in history:
                entries.append({
                    "record_id": record_id,
                    **entry
                })
                
        if format == "pandas":
            return pd.DataFrame(entries)
        else:
            return {"entries": entries}
