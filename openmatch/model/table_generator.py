"""
Table generation and management for OpenMatch.

This module provides functionality for automatically generating and managing
master and cross-reference tables based on user-defined data models.
"""

from typing import Dict, List, Optional, Any, Type
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import text
import logging
from datetime import datetime

from .models import Model, XrefModel
from .fields import Field, CharField, DateTimeField, FloatField


class TableGenerator:
    """Handles automatic generation of master and xref tables.
    
    This class is responsible for creating the physical database tables
    that store master records and cross-references. It handles schema
    generation, index creation, and table management.
    
    Attributes:
        engine: SQLAlchemy engine instance
        schema: Database schema name
        metadata: SQLAlchemy MetaData instance
        logger: Logger instance
    """

    # Standard metadata columns for all tables
    METADATA_COLUMNS = {
        'record_id': sa.Column('record_id', sa.String(36), primary_key=True),
        'source_system': sa.Column('source_system', sa.String(100), nullable=False),
        'source_id': sa.Column('source_id', sa.String(255), nullable=False),
        'created_at': sa.Column('created_at', sa.DateTime, default=datetime.utcnow),
        'updated_at': sa.Column('updated_at', sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
        'status': sa.Column('status', sa.String(50), default='ACTIVE'),
        'version': sa.Column('version', sa.Integer, default=1),
    }

    # Additional columns for xref tables
    XREF_COLUMNS = {
        'master_record_id': sa.Column('master_record_id', sa.String(36), nullable=True),
        'match_score': sa.Column('match_score', sa.Float, nullable=True),
        'match_status': sa.Column('match_status', sa.String(50), default='UNMATCHED'),
        'match_date': sa.Column('match_date', sa.DateTime, nullable=True),
    }

    def __init__(self, engine: sa.engine.Engine, schema: str = 'mdm'):
        """Initialize table generator.
        
        Args:
            engine: SQLAlchemy engine instance
            schema: Database schema name (default: 'mdm')
        """
        self.engine = engine
        self.schema = schema
        self.logger = logging.getLogger(__name__)
        self.metadata = sa.MetaData(schema=schema)

    def _get_column_type(self, field: Field) -> sa.types.TypeEngine:
        """Convert OpenMatch field type to SQLAlchemy column type.
        
        Args:
            field: OpenMatch field instance
            
        Returns:
            SQLAlchemy type engine instance
            
        Raises:
            ValueError: If field type is not supported
        """
        type_map = {
            'CharField': sa.String,
            'TextField': sa.Text,
            'IntegerField': sa.Integer,
            'FloatField': sa.Float,
            'BooleanField': sa.Boolean,
            'DateTimeField': sa.DateTime,
            'DateField': sa.Date,
            'JSONField': sa.JSON,
        }
        
        field_type = field.__class__.__name__
        if field_type not in type_map:
            raise ValueError(f"Unsupported field type: {field_type}")
            
        base_type = type_map[field_type]
        
        if field_type == 'CharField':
            return base_type(field.max_length)
        return base_type()

    def _create_table_from_model(self, model_cls: Type[Model], is_xref: bool = False) -> sa.Table:
        """Create SQLAlchemy Table object from OpenMatch model."""
        columns = []
        
        # Add metadata columns
        columns.extend(self.METADATA_COLUMNS.values())
        
        # Add xref-specific columns if needed
        if is_xref:
            columns.extend(self.XREF_COLUMNS.values())
        
        # Add model-specific columns
        for field_name, field in model_cls.get_fields().items():
            # Skip if column already exists in metadata
            if field_name in self.METADATA_COLUMNS or field_name in self.XREF_COLUMNS:
                continue
                
            column_type = self._get_column_type(field)
            column = sa.Column(
                field_name,
                column_type,
                nullable=field.null,
                unique=getattr(field, 'unique', False),
                index=getattr(field, 'index', False)
            )
            columns.append(column)
        
        # Create table
        table_name = f"{model_cls.__name__.lower()}_{'xref' if is_xref else 'master'}"
        return sa.Table(table_name, self.metadata, *columns)

    def generate_tables(self, model_cls: Type[Model]) -> Dict[str, sa.Table]:
        """Generate master and xref tables for a model.
        
        This method:
        1. Creates the master table with all model fields
        2. Creates the cross-reference table if enabled
        3. Sets up appropriate indexes
        4. Creates the tables in the database
        
        Args:
            model_cls: OpenMatch model class
            
        Returns:
            Dict containing master and xref table objects
            
        Raises:
            Exception: If table generation fails
        """
        try:
            # Create master table
            master_table = self._create_table_from_model(model_cls)
            
            # Create xref table if model has xref enabled
            xref_table = None
            if getattr(model_cls._meta, 'xref', True):
                xref_table = self._create_table_from_model(model_cls, is_xref=True)
            
            # Create tables in database
            self.metadata.create_all(self.engine, tables=[master_table])
            if xref_table:
                self.metadata.create_all(self.engine, tables=[xref_table])
            
            # Create indexes
            self._create_indexes(master_table)
            if xref_table:
                self._create_indexes(xref_table)
            
            return {
                'master': master_table,
                'xref': xref_table
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate tables for model {model_cls.__name__}: {str(e)}")
            raise

    def _create_indexes(self, table: sa.Table) -> None:
        """Create indexes for a table."""
        with self.engine.begin() as conn:
            # Create composite index on source_system and source_id
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table.name}_source "
                f"ON {self.schema}.{table.name} (source_system, source_id)"
            ))
            
            # Create index on status
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table.name}_status "
                f"ON {self.schema}.{table.name} (status)"
            ))
            
            # Create index on created_at
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table.name}_created_at "
                f"ON {self.schema}.{table.name} (created_at)"
            ))
            
            if 'master_record_id' in table.columns:
                # Create index on master_record_id for xref tables
                conn.execute(text(
                    f"CREATE INDEX IF NOT EXISTS idx_{table.name}_master_record "
                    f"ON {self.schema}.{table.name} (master_record_id)"
                )) 