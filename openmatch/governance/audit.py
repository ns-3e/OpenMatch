from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
import json
import logging
from dataclasses import dataclass
from functools import wraps

class AuditEventType(Enum):
    RECORD_CREATE = "record_create"
    RECORD_UPDATE = "record_update"
    RECORD_DELETE = "record_delete"
    RECORD_MERGE = "record_merge"
    RECORD_MATCH = "record_match"
    ROLE_ASSIGN = "role_assign"
    ROLE_REMOVE = "role_remove"
    EXPORT = "export"
    LOGIN = "login"
    LOGOUT = "logout"

@dataclass
class AuditEvent:
    event_type: AuditEventType
    user_id: str
    timestamp: datetime
    resource_id: Optional[str]
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    status: str = "success"
    error_message: Optional[str] = None

class AuditLogger:
    def __init__(self, log_file: Optional[str] = None):
        self.logger = logging.getLogger("openmatch.audit")
        self.logger.setLevel(logging.INFO)
        
        if log_file:
            handler = logging.FileHandler(log_file)
        else:
            handler = logging.StreamHandler()
            
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "event": %(message)s}'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log_event(self, event: AuditEvent) -> None:
        """Log an audit event."""
        event_dict = {
            "event_type": event.event_type.value,
            "user_id": event.user_id,
            "timestamp": event.timestamp.isoformat(),
            "resource_id": event.resource_id,
            "details": event.details,
            "ip_address": event.ip_address,
            "status": event.status,
            "error_message": event.error_message
        }
        self.logger.info(json.dumps(event_dict))

    def search_events(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_type: Optional[AuditEventType] = None,
        user_id: Optional[str] = None,
        resource_id: Optional[str] = None
    ) -> List[AuditEvent]:
        """Search audit events with filters."""
        # Note: This is a placeholder implementation.
        # In a production environment, this would typically query a database
        # or log aggregation service.
        raise NotImplementedError(
            "Event search requires database/log aggregation implementation"
        )

def audit_action(event_type: AuditEventType):
    """Decorator to automatically log actions."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                result = func(self, *args, **kwargs)
                
                # Extract user_id and resource_id from args/kwargs based on function signature
                user_id = kwargs.get('user_id', 'system')
                resource_id = kwargs.get('resource_id')
                
                # Create audit event
                event = AuditEvent(
                    event_type=event_type,
                    user_id=user_id,
                    timestamp=datetime.utcnow(),
                    resource_id=resource_id,
                    details={
                        "action": func.__name__,
                        "args": str(args),
                        "kwargs": str(kwargs)
                    }
                )
                
                # Log the event
                if hasattr(self, 'audit_logger'):
                    self.audit_logger.log_event(event)
                
                return result
                
            except Exception as e:
                # Log error event
                error_event = AuditEvent(
                    event_type=event_type,
                    user_id=kwargs.get('user_id', 'system'),
                    timestamp=datetime.utcnow(),
                    resource_id=kwargs.get('resource_id'),
                    details={
                        "action": func.__name__,
                        "error": str(e)
                    },
                    status="error",
                    error_message=str(e)
                )
                
                if hasattr(self, 'audit_logger'):
                    self.audit_logger.log_event(error_event)
                
                raise
                
        return wrapper
    return decorator
