from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import json

@dataclass
class HistoryEntry:
    timestamp: datetime
    action: str
    field_name: Optional[str] = None
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    user_id: Optional[str] = None
    source_system: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

class RecordHistory:
    def __init__(self):
        self.history: Dict[str, List[HistoryEntry]] = {}

    def add_entry(
        self,
        record_id: str,
        action: str,
        details: Dict[str, Any],
        user_id: Optional[str] = None,
        source_system: Optional[str] = None,
        field_name: Optional[str] = None,
        old_value: Optional[Any] = None,
        new_value: Optional[Any] = None
    ) -> None:
        """Add a history entry for a record."""
        entry = HistoryEntry(
            timestamp=datetime.utcnow(),
            action=action,
            field_name=field_name,
            old_value=old_value,
            new_value=new_value,
            user_id=user_id,
            source_system=source_system,
            details=details
        )
        
        if record_id not in self.history:
            self.history[record_id] = []
            
        self.history[record_id].append(entry)

    def get_history(
        self,
        record_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        actions: Optional[List[str]] = None,
        fields: Optional[List[str]] = None
    ) -> List[HistoryEntry]:
        """Get filtered history for a record."""
        if record_id not in self.history:
            return []
            
        entries = self.history[record_id]
        
        # Apply filters
        if start_time:
            entries = [e for e in entries if e.timestamp >= start_time]
        if end_time:
            entries = [e for e in entries if e.timestamp <= end_time]
        if actions:
            entries = [e for e in entries if e.action in actions]
        if fields:
            entries = [e for e in entries if e.field_name in fields]
            
        return sorted(entries, key=lambda x: x.timestamp)

    def get_field_history(
        self,
        record_id: str,
        field_name: str
    ) -> List[HistoryEntry]:
        """Get history of changes for a specific field."""
        return self.get_history(record_id, fields=[field_name])

    def get_value_at_time(
        self,
        record_id: str,
        field_name: str,
        timestamp: datetime
    ) -> Optional[Any]:
        """Get the value of a field at a specific point in time."""
        entries = self.get_field_history(record_id, field_name)
        
        # Find the most recent value before or at the timestamp
        value = None
        for entry in entries:
            if entry.timestamp <= timestamp and entry.new_value is not None:
                value = entry.new_value
                
        return value

    def export_history(
        self,
        record_id: str,
        format_type: str = "json"
    ) -> str:
        """Export record history in specified format."""
        entries = self.get_history(record_id)
        
        if format_type == "json":
            history_data = [
                {
                    "timestamp": entry.timestamp.isoformat(),
                    "action": entry.action,
                    "field_name": entry.field_name,
                    "old_value": entry.old_value,
                    "new_value": entry.new_value,
                    "user_id": entry.user_id,
                    "source_system": entry.source_system,
                    "details": entry.details
                }
                for entry in entries
            ]
            return json.dumps(history_data, indent=2)
            
        raise ValueError(f"Unsupported format type: {format_type}")

    def compare_versions(
        self,
        record_id: str,
        timestamp1: datetime,
        timestamp2: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """Compare record versions at two points in time."""
        entries = self.get_history(record_id)
        
        # Build state at each timestamp
        state1 = {}
        state2 = {}
        
        for entry in entries:
            if entry.field_name and entry.new_value is not None:
                if entry.timestamp <= timestamp1:
                    state1[entry.field_name] = entry.new_value
                if entry.timestamp <= timestamp2:
                    state2[entry.field_name] = entry.new_value
                    
        # Find differences
        differences = {}
        all_fields = set(state1.keys()) | set(state2.keys())
        
        for field in all_fields:
            value1 = state1.get(field)
            value2 = state2.get(field)
            
            if value1 != value2:
                differences[field] = {
                    "timestamp1_value": value1,
                    "timestamp2_value": value2
                }
                
        return differences
