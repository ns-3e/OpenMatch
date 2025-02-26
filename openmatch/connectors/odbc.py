from typing import List, Dict, Any, Optional
import pyodbc
from . import Connector

class ODBCConnector(Connector):
    """Connector for ODBC-compliant databases."""
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        dsn: Optional[str] = None,
        driver: Optional[str] = None,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **odbc_params
    ):
        """
        Initialize ODBC Connector.
        
        Args:
            connection_string: Complete ODBC connection string (optional)
            dsn: Data Source Name (optional)
            driver: ODBC driver name (optional)
            server: Server address (optional)
            database: Database name (optional)
            username: Database username (optional)
            password: Database password (optional)
            **odbc_params: Additional ODBC parameters
            
        Note: Either provide a complete connection_string OR individual parameters
        """
        self.connection_string = connection_string
        
        if not connection_string:
            # Build connection string from parameters
            conn_parts = []
            
            if dsn:
                conn_parts.append(f"DSN={dsn}")
            if driver:
                conn_parts.append(f"Driver={{{driver}}}")
            if server:
                conn_parts.append(f"Server={server}")
            if database:
                conn_parts.append(f"Database={database}")
            if username:
                conn_parts.append(f"UID={username}")
            if password:
                conn_parts.append(f"PWD={password}")
                
            # Add any additional parameters
            for key, value in odbc_params.items():
                conn_parts.append(f"{key}={value}")
                
            self.connection_string = ';'.join(conn_parts)
            
        self._conn = None
        self._cursor = None

    def connect(self) -> bool:
        """Establish ODBC connection."""
        try:
            self._conn = pyodbc.connect(self.connection_string)
            self._cursor = self._conn.cursor()
            return True
        except Exception as e:
            print(f"Failed to establish ODBC connection: {str(e)}")
            return False

    def read_records(
        self, 
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute query and return results."""
        try:
            # Execute query with parameters if provided
            if params:
                self._cursor.execute(query, list(params.values()))
            else:
                self._cursor.execute(query)
                
            # Get column names from description
            columns = [desc[0] for desc in self._cursor.description]
            
            # Fetch results and convert to dictionaries
            results = []
            for row in self._cursor.fetchall():
                results.append(dict(zip(columns, row)))
                
            return results
            
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return []

    def write_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str
    ) -> int:
        """Insert records into target table."""
        if not records:
            return 0
            
        try:
            # Generate insert statement
            columns = list(records[0].keys())
            placeholders = ','.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO {target} ({','.join(columns)}) VALUES ({placeholders})"
            
            # Execute batch insert
            count = 0
            for record in records:
                values = [record[col] for col in columns]
                self._cursor.execute(insert_sql, values)
                count += 1
                
            self._conn.commit()
            return count
            
        except Exception as e:
            print(f"Error writing records: {str(e)}")
            self._conn.rollback()
            return 0

    def update_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str
    ) -> int:
        """Update existing records in target table."""
        if not records or 'id' not in records[0]:
            return 0
            
        try:
            count = 0
            for record in records:
                # Generate update statement
                set_clause = ','.join([
                    f"{k}=?"
                    for k in record.keys()
                    if k != 'id'
                ])
                
                update_sql = f"UPDATE {target} SET {set_clause} WHERE id=?"
                
                # Prepare values (all fields except id, then id at the end)
                values = [
                    v for k, v in record.items()
                    if k != 'id'
                ] + [record['id']]
                
                self._cursor.execute(update_sql, values)
                count += 1
                
            self._conn.commit()
            return count
            
        except Exception as e:
            print(f"Error updating records: {str(e)}")
            self._conn.rollback()
            return 0

    def delete_records(
        self, 
        record_ids: List[str], 
        target: str
    ) -> int:
        """Delete records from target table."""
        if not record_ids:
            return 0
            
        try:
            # Generate delete statement with placeholders
            placeholders = ','.join(['?' for _ in record_ids])
            delete_sql = f"DELETE FROM {target} WHERE id IN ({placeholders})"
            
            # Execute delete
            self._cursor.execute(delete_sql, record_ids)
            count = self._cursor.rowcount
            
            self._conn.commit()
            return count
            
        except Exception as e:
            print(f"Error deleting records: {str(e)}")
            self._conn.rollback()
            return 0

    def close(self) -> None:
        """Close ODBC connection."""
        if self._cursor:
            self._cursor.close()
            
        if self._conn:
            self._conn.close()
            
        self._cursor = None
        self._conn = None 