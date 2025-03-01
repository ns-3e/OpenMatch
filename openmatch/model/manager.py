"""
Data model manager implementation.
"""

from typing import Dict, List, Optional, Any, Union
import logging
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
import json

from .config import (
    DataModelConfig,
    EntityConfig,
    FieldConfig,
    DataType,
    PhysicalModelConfig
)


class DataModelManager:
    """Manages data model and physical tables."""
    
    def __init__(self, data_model: DataModelConfig, db_engine: sa.engine.Engine):
        """Initialize manager with data model configuration."""
        self.data_model = data_model
        self.engine = db_engine
        self.logger = logging.getLogger(__name__)
        
    def create_physical_model(self):
        """Create physical tables."""
        try:
            # Create schema if not exists
            with self.engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS mdm"))
                
            metadata = sa.MetaData(schema='mdm')
            
            # Create golden records table
            golden_table = sa.Table(
                'golden_records',
                metadata,
                sa.Column('id', sa.String(255), primary_key=True),
                sa.Column('source', sa.String(50), nullable=False),
                sa.Column('data', sa.JSON, nullable=False),
                sa.Column('created_at', sa.DateTime, default=datetime.now),
                sa.Column('updated_at', sa.DateTime, onupdate=datetime.now),
                schema='mdm'  # Explicitly set schema
            )
            
            # Create all tables
            metadata.create_all(self.engine)
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to create physical model: {e}")
            raise
        
    def store_golden_record(self, record: Dict[str, Any]):
        """Store a golden record in the database.
        
        Args:
            record: Dictionary containing the golden record data
        """
        try:
            # Ensure required fields are present
            if 'id' not in record:
                raise ValueError("Golden record must have an 'id' field")
            
            # Convert any nested dictionaries to JSON strings
            processed_record = {}
            for key, value in record.items():
                if isinstance(value, dict):
                    processed_record[key] = json.dumps(value)
                else:
                    processed_record[key] = value
            
            # Create or update record
            with self.engine.begin() as conn:
                # Check if record exists
                exists = conn.execute(
                    text("SELECT 1 FROM mdm.golden_records WHERE id = :id"),
                    {"id": record["id"]}
                ).fetchone() is not None
                
                if exists:
                    # Update existing record
                    conn.execute(
                        text(
                            """
                            UPDATE mdm.golden_records
                            SET data = :data,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id
                            """
                        ),
                        {
                            "id": record["id"],
                            "data": json.dumps(processed_record)
                        }
                    )
                else:
                    # Insert new record
                    conn.execute(
                        text(
                            """
                            INSERT INTO mdm.golden_records (id, source, data, created_at, updated_at)
                            VALUES (:id, :source, :data, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            """
                        ),
                        {
                            "id": record["id"],
                            "source": record.get("source", "GOLDEN"),
                            "data": json.dumps(processed_record)
                        }
                    )
        except Exception as e:
            self.logger.error(f"Failed to store golden record: {e}")
            raise
        
    def get_golden_records(self) -> List[Dict[str, Any]]:
        """Retrieve all golden records.
        
        Returns:
            List of golden records
        """
        try:
            stmt = text("SELECT * FROM mdm.golden_records")
            with self.engine.connect() as conn:
                result = conn.execute(stmt)
                return [dict(row) for row in result]
                
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to retrieve golden records: {str(e)}")
            raise

    def _get_sa_type(self, field: FieldConfig) -> sa.types.TypeEngine:
        """Convert OpenMatch data type to SQLAlchemy type."""
        type_map = {
            DataType.STRING: sa.String,
            DataType.INTEGER: sa.Integer,
            DataType.FLOAT: sa.Float,
            DataType.BOOLEAN: sa.Boolean,
            DataType.DATE: sa.Date,
            DataType.DATETIME: sa.DateTime,
            DataType.JSON: sa.JSON,
            DataType.ARRAY: sa.ARRAY
        }
        return type_map[field.data_type]()

    def _create_table(
        self,
        table_config: Dict[str, Any],
        schema: Optional[str] = None
    ) -> None:
        """Create a database table from configuration."""
        metadata = sa.MetaData(schema=schema)
        
        # Create column definitions
        columns = []
        for col in table_config["columns"]:
            sa_type = (
                sa.String if isinstance(col["type"], str)
                else self._get_sa_type(col["type"])
            )
            
            column = sa.Column(
                col["name"],
                sa_type,
                primary_key=col.get("primary_key", False),
                nullable=col.get("nullable", True),
                unique=col.get("unique", False)
            )
            
            if col.get("foreign_key"):
                ref_table, ref_col = col["foreign_key"].split(".")
                column.foreign_key = sa.ForeignKey(f"{ref_table}.{ref_col}")
                
            columns.append(column)
            
        # Create table
        table = sa.Table(
            table_config["name"],
            metadata,
            *columns
        )
        
        # Add indexes
        for idx in table_config.get("indexes", []):
            sa.Index(
                f"{table.name}_{idx['name']}",
                *[table.c[col] for col in idx["columns"]],
                unique=idx.get("unique", False)
            )
            
        # Create table in database
        try:
            table.create(self.engine)
            self.logger.info(f"Created table {table.name}")
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to create table {table.name}: {str(e)}")
            raise

    def _drop_table(self, table_name: str, schema: str) -> None:
        """Drop a table if it exists.

        Args:
            table_name: Name of the table to drop
            schema: Schema name
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name}"))
                self.logger.info(f"Dropped table {schema}.{table_name}")
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to drop table {schema}.{table_name}: {str(e)}")
            raise

    def discover_source_schema(
        self,
        source_name: str,
        table_name: str
    ) -> Dict[str, Any]:
        """Discover schema from a source system table."""
        source_config = self.data_model.source_systems.get(source_name)
        if not source_config:
            raise ValueError(f"Unknown source system: {source_name}")
            
        try:
            # Create engine for source system
            source_engine = sa.create_engine(
                source_config.connection_details["connection_string"]
            )
            
            # Reflect table
            metadata = sa.MetaData()
            table = sa.Table(
                table_name,
                metadata,
                autoload_with=source_engine
            )
            
            # Convert to OpenMatch schema
            schema = {
                "name": table_name,
                "fields": []
            }
            
            for col in table.columns:
                field = {
                    "name": col.name,
                    "data_type": str(col.type),
                    "required": not col.nullable,
                    "primary_key": col.primary_key,
                    "unique": col.unique,
                }
                if col.foreign_keys:
                    fk = next(iter(col.foreign_keys))
                    field["foreign_key"] = f"{fk.column.table.name}.{fk.column.name}"
                    
                schema["fields"].append(field)
                
            return schema
            
        except Exception as e:
            self.logger.error(
                f"Failed to discover schema for {source_name}.{table_name}: {str(e)}"
            )
            raise

    def apply_field_mappings(
        self,
        source_name: str,
        entity_name: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply field mappings from source to entity."""
        source_config = self.data_model.source_systems.get(source_name)
        if not source_config:
            raise ValueError(f"Unknown source system: {source_name}")
            
        entity_config = self.data_model.entities.get(entity_name)
        if not entity_config:
            raise ValueError(f"Unknown entity: {entity_name}")
            
        # Get field mappings for this entity
        mappings = source_config.field_mappings.get(entity_name, {})
        
        # Apply mappings
        result = {}
        for target_field, source_field in mappings.items():
            if source_field in data:
                result[target_field] = data[source_field]
                
        return result

    def validate_entity_data(
        self,
        entity_name: str,
        data: Dict[str, Any]
    ) -> List[str]:
        """Validate data against entity configuration."""
        entity_config = self.data_model.entities.get(entity_name)
        if not entity_config:
            raise ValueError(f"Unknown entity: {entity_name}")
            
        errors = []
        
        # Check required fields
        for field in entity_config.fields:
            if field.required and field.name not in data:
                errors.append(f"Missing required field: {field.name}")
                
        # Validate field values
        for field_name, value in data.items():
            field = next(
                (f for f in entity_config.fields if f.name == field_name),
                None
            )
            if not field:
                errors.append(f"Unknown field: {field_name}")
                continue
                
            # Type validation
            if value is not None:
                try:
                    self._validate_field_value(field, value)
                except ValueError as e:
                    errors.append(str(e))
                    
            # Custom validation rules
            for rule_name, rule_config in field.validation_rules.items():
                if not self._validate_custom_rule(value, rule_config):
                    errors.append(
                        f"Field {field_name} failed validation rule: {rule_name}"
                    )
                    
        return errors

    def _validate_field_value(self, field: FieldConfig, value: Any) -> None:
        """Validate a field value against its configuration."""
        if field.data_type == DataType.STRING:
            if not isinstance(value, str):
                raise ValueError(
                    f"Field {field.name} expects string, got {type(value)}"
                )
        elif field.data_type == DataType.INTEGER:
            if not isinstance(value, int):
                raise ValueError(
                    f"Field {field.name} expects integer, got {type(value)}"
                )
        elif field.data_type == DataType.FLOAT:
            if not isinstance(value, (int, float)):
                raise ValueError(
                    f"Field {field.name} expects number, got {type(value)}"
                )
        elif field.data_type == DataType.BOOLEAN:
            if not isinstance(value, bool):
                raise ValueError(
                    f"Field {field.name} expects boolean, got {type(value)}"
                )
        elif field.data_type == DataType.DATE:
            if not isinstance(value, datetime):
                raise ValueError(
                    f"Field {field.name} expects date, got {type(value)}"
                )
        elif field.data_type == DataType.DATETIME:
            if not isinstance(value, datetime):
                raise ValueError(
                    f"Field {field.name} expects datetime, got {type(value)}"
                )

    def _validate_custom_rule(
        self,
        value: Any,
        rule_config: Dict[str, Any]
    ) -> bool:
        """Validate a value against a custom validation rule."""
        rule_type = rule_config.get("type")
        
        if rule_type == "regex":
            import re
            pattern = rule_config.get("pattern", "")
            return bool(re.match(pattern, str(value)))
            
        elif rule_type == "range":
            min_val = rule_config.get("min")
            max_val = rule_config.get("max")
            if min_val is not None and value < min_val:
                return False
            if max_val is not None and value > max_val:
                return False
            return True
            
        elif rule_type == "enum":
            allowed_values = rule_config.get("values", [])
            return value in allowed_values
            
        elif rule_type == "custom":
            func = rule_config.get("function")
            if callable(func):
                return func(value)
                
        return True  # Unknown rule type 