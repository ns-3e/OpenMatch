from typing import List, Dict, Any, Optional
from databricks import sql
from . import Connector

class DatabricksConnector(Connector):
    """Connector for Databricks SQL Analytics."""
    
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ):
        self.connection_params = {
            "server_hostname": server_hostname,
            "http_path": http_path,
            "access_token": access_token
        }
        self.catalog = catalog
        self.schema = schema
        self._conn = None
        self._cursor = None

    def connect(self) -> bool:
        """Establish connection to Databricks."""
        try:
            self._conn = sql.connect(**self.connection_params)
            self._cursor = self._conn.cursor()
            
            if self.catalog and self.schema:
                self._cursor.execute(f"USE CATALOG {self.catalog}")
                self._cursor.execute(f"USE SCHEMA {self.schema}")
            
            return True
        except Exception as e:
            print(f"Failed to connect to Databricks: {str(e)}")
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
                
            columns = [desc[0] for desc in self._cursor.description]
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
            placeholders = ",".join(["%s" for _ in columns])
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
            return 0

    def update_records(
        self, 
        records: List[Dict[str, Any]], 
        target: str
    ) -> int:
        """Update existing records in target table using MERGE."""
        if not records or "id" not in records[0]:
            return 0
            
        try:
            count = 0
            for record in records:
                set_clause = ",".join([f"{k}=%s" for k in record.keys() if k != "id"])
                merge_sql = f"""
                MERGE INTO {target} target
                USING (SELECT %s as id) source
                ON target.id = source.id
                WHEN MATCHED THEN
                    UPDATE SET {set_clause}
                """
                
                values = [record["id"]] + [v for k, v in record.items() if k != "id"]
                self._cursor.execute(merge_sql, values)
                count += 1
                
            self._conn.commit()
            return count
        except Exception as e:
            print(f"Error updating records: {str(e)}")
            return 0

    def delete_records(
        self, 
        record_ids: List[str], 
        target: str
    ) -> int:
        """Delete records from target table."""
        try:
            placeholders = ",".join(["%s" for _ in record_ids])
            delete_sql = f"DELETE FROM {target} WHERE id IN ({placeholders})"
            
            self._cursor.execute(delete_sql, record_ids)
            self._conn.commit()
            
            return len(record_ids)  # Databricks doesn't support rowcount
        except Exception as e:
            print(f"Error deleting records: {str(e)}")
            return 0

    def close(self) -> None:
        """Close database connection."""
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
