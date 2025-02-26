"""
Data model management implementation.
"""

from typing import Dict, List, Optional, Any, Union
import logging
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

from .config import (
    DataModelConfig,
    EntityConfig,
    FieldConfig,
    DataType,
    PhysicalModelConfig
)


class DataModelManager:
    """Manager for data model operations."""

    def __init__(
        self,
        config: DataModelConfig,
        engine: sa.Engine,
        logger: Optional[logging.Logger] = None
    ):
        """Initialize data model manager.
        
        Args:
            config: Data model configuration
            engine: SQLAlchemy engine for database operations
            logger: Optional logger instance
        """
        self.config = config
        self.engine = engine
        self.logger = logger or logging.getLogger(__name__)
        
        # Validate configuration
        errors = config.validate()
        if errors:
            raise ValueError(f"Invalid configuration: {', '.join(errors)}")

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

    def create_physical_model(self) -> None:
        """Create the physical tables in the database based on the data model configuration."""
        physical_model = self.config.to_physical_model()

        for entity_name, tables in physical_model.items():
            try:
                # Drop existing tables first
                self._drop_table(tables["master"]["name"], self.config.physical_model.schema_name)
                self._drop_table(tables["history"]["name"], self.config.physical_model.schema_name)
                self._drop_table(tables["xref"]["name"], self.config.physical_model.schema_name)

                # Create master table
                self._create_table(tables["master"], self.config.physical_model.schema_name)

                # Create history table
                self._create_table(tables["history"], self.config.physical_model.schema_name)

                # Create cross-reference table
                self._create_table(tables["xref"], self.config.physical_model.schema_name)

                self.logger.info(f"Created tables for entity {entity_name}")
            except Exception as e:
                self.logger.error(f"Failed to create tables for entity {entity_name}: {str(e)}")
                raise

    def discover_source_schema(
        self,
        source_name: str,
        table_name: str
    ) -> Dict[str, Any]:
        """Discover schema from a source system table."""
        source_config = self.config.source_systems.get(source_name)
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
        source_config = self.config.source_systems.get(source_name)
        if not source_config:
            raise ValueError(f"Unknown source system: {source_name}")
            
        entity_config = self.config.entities.get(entity_name)
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
        entity_config = self.config.entities.get(entity_name)
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