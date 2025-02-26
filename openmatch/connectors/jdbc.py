from typing import List, Dict, Any, Optional
import jaydebeapi
from . import Connector

class JDBCConnector(Connector):
    """Connector for JDBC-compliant databases."""
    
    def __init__(
        self,
        jdbc_url: str,
        driver_class: str,
        jar_path: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver_args: Optional[List[str]] = None,
        jdbc_properties: Optional[Dict[str, str]] = None
    ):
        """
        Initialize JDBC Connector.
        
        Args:
            jdbc_url: JDBC URL for the database
            driver_class: Fully qualified Java class name of the JDBC driver
            jar_path: Path to the JAR file containing the JDBC driver
            username: Database username (optional)
            password: Database password (optional)
            driver_args: Additional driver arguments (optional)
            jdbc_properties: Additional JDBC properties (optional)
        """
        self.jdbc_url = jdbc_url
        self.driver_class = driver_class
        self.jar_path = jar_path
        self.username = username
        self.password = password
        self.driver_args = driver_args or []
        self.jdbc_properties = jdbc_properties or {}
        
        self._conn = None
        self._cursor = None

    def connect(self) -> bool:
        """Establish JDBC connection."""
        try:
            # Build connection arguments
            conn_args = []
            
            if self.username and self.password:
                conn_args = [self.username, self.password]
            
            if self.driver_args:
                conn_args.extend(self.driver_args)
                
            # Establish connection
            self._conn = jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                conn_args,
                self.jar_path,
                **self.jdbc_properties
            )
            
            self._cursor = self._conn.cursor()
            return True
            
        except Exception as e:
            print(f"Failed to establish JDBC connection: {str(e)}")
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
        """Close JDBC connection."""
        if self._cursor:
            self._cursor.close()
            
        if self._conn:
            self._conn.close()
            
        self._cursor = None
        self._conn = None 