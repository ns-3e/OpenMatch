from typing import List, Dict, Any, Optional
import pyodbc
from . import Connector

class AzureConnector(Connector):
    """Connector for Azure Synapse Analytics."""
    
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        port: int = 1433
    ):
        self.connection_string = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={server},{port};"
            f"Database={database};"
            f"UID={username};"
            f"PWD={password};"
        )
        self._conn = None
        self._cursor = None

    def connect(self) -> bool:
        """Establish connection to Azure Synapse."""
        try:
            self._conn = pyodbc.connect(self.connection_string)
            self._cursor = self._conn.cursor()
            return True
        except Exception as e:
            print(f"Failed to connect to Azure Synapse: {str(e)}")
            return False

    def read_records(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries."""
        try:
            if params:
                self._cursor.execute(query, params)
            else:
                self._cursor.execute(query)
                
            columns = [column[0] for column in self._cursor.description]
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
        """Insert records into specified target table."""
        if not records:
            return 0
            
        try:
            # Generate insert statement based on first record
            columns = list(records[0].keys())
            placeholders = ",".join(["?" for _ in columns])
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
            self._conn.rollback()
            print(f"Error writing records: {str(e)}")
            return 0

    def update_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str
    ) -> int:
        """Update existing records in target table."""
        if not records or "id" not in records[0]:
            return 0
            
        try:
            count = 0
            for record in records:
                set_clause = ",".join([f"{k}=?" for k in record.keys() if k != "id"])
                update_sql = f"UPDATE {target} SET {set_clause} WHERE id=?"
                
                values = [v for k, v in record.items() if k != "id"]
                values.append(record["id"])
                
                self._cursor.execute(update_sql, values)
                count += 1
                
            self._conn.commit()
            return count
        except Exception as e:
            self._conn.rollback()
            print(f"Error updating records: {str(e)}")
            return 0

    def delete_records(
        self, 
        record_ids: List[str], 
        target: str
    ) -> int:
        """Delete records from target table."""
        try:
            placeholders = ",".join(["?" for _ in record_ids])
            delete_sql = f"DELETE FROM {target} WHERE id IN ({placeholders})"
            
            self._cursor.execute(delete_sql, record_ids)
            count = self._cursor.rowcount
            
            self._conn.commit()
            return count
        except Exception as e:
            self._conn.rollback()
            print(f"Error deleting records: {str(e)}")
            return 0

    def close(self) -> None:
        """Close database connection."""
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
