from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class FieldConfig:
    """Configuration for a field in a source or target table.
    
    Attributes:
        name: The name of the field
        data_type: The data type of the field (e.g., 'text', 'integer', etc.)
        is_key: Whether this field is a primary key
        is_parent_key: Whether this field references a parent record
    """
    name: str
    data_type: str
    is_key: bool = False
    is_parent_key: bool = False

@dataclass
class TableConfig:
    """Configuration for a source or target table.
    
    Attributes:
        table_name: The name of the table
        fields: List of field configurations
        key_field: The name of the primary key field
        parent_key: The name of the field referencing a parent record
    """
    table_name: str
    fields: List[FieldConfig]
    key_field: Optional[str] = None
    parent_key: Optional[str] = None

@dataclass
class SourceConfig:
    """Configuration for source database."""
    
    def __init__(
        self,
        schema: str,
        tables: List[TableConfig],
        source_system_id: str,
        database: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        """Initialize source configuration."""
        self.schema = schema
        self.tables = tables
        self.source_system_id = source_system_id
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        
        # Build table lookup
        self.table_map = {table.table_name: table for table in tables}
        
    def get_parent_table(self, table_name: str) -> Optional[str]:
        """Get the parent table name for a given table.
        
        Args:
            table_name: Name of the table to find parent for
            
        Returns:
            Optional[str]: Name of parent table if exists, None otherwise
        """
        table_config = self.table_map.get(table_name)
        if not table_config or not table_config.parent_key:
            return None
            
        # Find the table that this table references
        for other_table in self.tables:
            if other_table.table_name != table_name:
                for field in other_table.fields:
                    if field.is_key and field.name == 'id':
                        # Check if this table's parent key references the other table's id
                        if table_config.parent_key.replace('_id', '') == other_table.table_name.rstrip('s'):
                            return other_table.table_name
        return None

@dataclass
class TargetConfig:
    schema: str
    database: str
    host: str = 'localhost'
    port: int = 5432
    user: str = 'postgres'
    password: str = 'postgres' 