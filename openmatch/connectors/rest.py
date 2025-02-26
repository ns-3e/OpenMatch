from typing import List, Dict, Any, Optional
import requests
from . import Connector

class RestConnector(Connector):
    """Connector for REST API endpoints."""
    
    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        auth: Optional[Dict[str, str]] = None,
        timeout: int = 30
    ):
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
        self.auth = auth
        self.timeout = timeout
        self.session = requests.Session()
        
        if auth:
            if 'username' in auth and 'password' in auth:
                self.session.auth = (auth['username'], auth['password'])
            elif 'token' in auth:
                self.headers['Authorization'] = f"Bearer {auth['token']}"

    def connect(self) -> bool:
        """Test connection to the REST API."""
        try:
            response = self.session.get(
                self.base_url,
                headers=self.headers,
                timeout=self.timeout
            )
            return 200 <= response.status_code < 300
        except Exception as e:
            print(f"Failed to connect to REST API: {str(e)}")
            return False

    def read_records(
        self, 
        query: str,  # In REST context, this is the endpoint path
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Get records from the REST API endpoint."""
        try:
            url = f"{self.base_url}/{query.lstrip('/')}"
            response = self.session.get(
                url,
                params=params,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            # Handle both array and object responses
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Try to find the data array in common response formats
                for key in ['data', 'results', 'items', 'records']:
                    if key in data and isinstance(data[key], list):
                        return data[key]
                return [data]  # Single record response
            return []
        except Exception as e:
            print(f"Error reading records: {str(e)}")
            return []

    def write_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str  # In REST context, this is the endpoint path
    ) -> int:
        """Post records to the REST API endpoint."""
        if not records:
            return 0
            
        try:
            url = f"{self.base_url}/{target.lstrip('/')}"
            count = 0
            
            # Handle both single and batch endpoints
            if len(records) == 1:
                response = self.session.post(
                    url,
                    json=records[0],
                    headers=self.headers,
                    timeout=self.timeout
                )
                response.raise_for_status()
                count = 1
            else:
                response = self.session.post(
                    url,
                    json=records,
                    headers=self.headers,
                    timeout=self.timeout
                )
                response.raise_for_status()
                count = len(records)
                
            return count
        except Exception as e:
            print(f"Error writing records: {str(e)}")
            return 0

    def update_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str  # In REST context, this is the endpoint path
    ) -> int:
        """Update records via the REST API endpoint."""
        if not records or "id" not in records[0]:
            return 0
            
        try:
            count = 0
            base_url = f"{self.base_url}/{target.lstrip('/')}"
            
            for record in records:
                record_id = record.pop('id')
                url = f"{base_url}/{record_id}"
                
                response = self.session.put(
                    url,
                    json=record,
                    headers=self.headers,
                    timeout=self.timeout
                )
                response.raise_for_status()
                count += 1
                
            return count
        except Exception as e:
            print(f"Error updating records: {str(e)}")
            return 0

    def delete_records(
        self, 
        record_ids: List[str], 
        target: str  # In REST context, this is the endpoint path
    ) -> int:
        """Delete records via the REST API endpoint."""
        try:
            count = 0
            base_url = f"{self.base_url}/{target.lstrip('/')}"
            
            for record_id in record_ids:
                url = f"{base_url}/{record_id}"
                response = self.session.delete(
                    url,
                    headers=self.headers,
                    timeout=self.timeout
                )
                response.raise_for_status()
                count += 1
                
            return count
        except Exception as e:
            print(f"Error deleting records: {str(e)}")
            return 0

    def close(self) -> None:
        """Close the session."""
        self.session.close()
