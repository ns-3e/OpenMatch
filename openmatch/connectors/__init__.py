from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class Connector(ABC):
    """Base class for all OpenMatch connectors."""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    def read_records(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Read records from the data source."""
        pass
    
    @abstractmethod
    def write_records(self, records: List[Dict[str, Any]], target: str) -> int:
        """Write records to the data source."""
        pass
    
    @abstractmethod
    def update_records(self, records: List[Dict[str, Any]], target: str) -> int:
        """Update existing records in the data source."""
        pass
    
    @abstractmethod
    def delete_records(self, record_ids: List[str], target: str) -> int:
        """Delete records from the data source."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the connection to the data source."""
        pass
