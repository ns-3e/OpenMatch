"""
Record management and lifecycle tracking for OpenMatch.

This module provides functionality for managing record ingestion, linking,
and lifecycle tracking between master and cross-reference tables.
"""

from typing import Dict, List, Optional, Any, Type, Tuple
import sqlalchemy as sa
from sqlalchemy.sql import text
import logging
from datetime import datetime
import uuid

from .models import Model
from .table_generator import TableGenerator


class RecordManager:
    """Manages record ingestion, linking, and lifecycle tracking."""

    def __init__(self, engine: sa.engine.Engine, schema: str = 'mdm'):
        """Initialize record manager.
        
        Args:
            engine: SQLAlchemy engine instance
            schema: Database schema name (default: 'mdm')
        """
        self.engine = engine
        self.schema = schema
        self.logger = logging.getLogger(__name__)

    def ingest_record(
        self,
        model_cls: Type[Model],
        data: Dict[str, Any],
        source_system: str,
        source_id: str
    ) -> Tuple[str, str]:
        """Ingest a new record into both master and xref tables.
        
        Args:
            model_cls: OpenMatch model class
            data: Record data
            source_system: Source system identifier
            source_id: Source record identifier
            
        Returns:
            Tuple of (record_id, master_record_id)
        """
        try:
            # Validate data against model
            if error := model_cls.validate(data):
                raise ValueError(f"Invalid record data: {error}")

            # Generate record ID
            record_id = str(uuid.uuid4())
            
            # Prepare common data
            now = datetime.utcnow()
            common_data = {
                'record_id': record_id,
                'source_system': source_system,
                'source_id': source_id,
                'created_at': now,
                'updated_at': now,
                'status': 'ACTIVE',
                'version': 1
            }
            
            # Insert into master table
            master_data = {**common_data, **data}
            master_table = f"{self.schema}.{model_cls.__name__.lower()}_master"
            
            with self.engine.begin() as conn:
                conn.execute(
                    text(f"INSERT INTO {master_table} ({', '.join(master_data.keys())}) "
                         f"VALUES ({', '.join([':' + k for k in master_data.keys()])})"
                    ),
                    master_data
                )
            
            # Insert into xref table if enabled
            if getattr(model_cls._meta, 'xref', True):
                xref_data = {
                    **common_data,
                    'master_record_id': record_id,  # Initially, xref points to itself as master
                    'match_status': 'UNMATCHED',
                    'match_score': None,
                    'match_date': None
                }
                xref_table = f"{self.schema}.{model_cls.__name__.lower()}_xref"
                
                with self.engine.begin() as conn:
                    conn.execute(
                        text(f"INSERT INTO {xref_table} ({', '.join(xref_data.keys())}) "
                             f"VALUES ({', '.join([':' + k for k in xref_data.keys()])})"
                        ),
                        xref_data
                    )
            
            return record_id, record_id
            
        except Exception as e:
            self.logger.error(f"Failed to ingest record: {str(e)}")
            raise

    def link_records(
        self,
        model_cls: Type[Model],
        source_record_id: str,
        master_record_id: str,
        match_score: float
    ) -> None:
        """Link a source record to a master record.
        
        Args:
            model_cls: OpenMatch model class
            source_record_id: Source record ID to link
            master_record_id: Master record ID to link to
            match_score: Confidence score of the match
        """
        try:
            if not getattr(model_cls._meta, 'xref', True):
                raise ValueError(f"Model {model_cls.__name__} does not have xref enabled")
                
            xref_table = f"{self.schema}.{model_cls.__name__.lower()}_xref"
            now = datetime.utcnow()
            
            with self.engine.begin() as conn:
                # Update xref record
                conn.execute(
                    text(f"""
                        UPDATE {xref_table}
                        SET master_record_id = :master_id,
                            match_status = 'MATCHED',
                            match_score = :score,
                            match_date = :match_date,
                            updated_at = :updated_at
                        WHERE record_id = :source_id
                    """),
                    {
                        'master_id': master_record_id,
                        'score': match_score,
                        'match_date': now,
                        'updated_at': now,
                        'source_id': source_record_id
                    }
                )
                
        except Exception as e:
            self.logger.error(f"Failed to link records: {str(e)}")
            raise

    def update_record(
        self,
        model_cls: Type[Model],
        record_id: str,
        data: Dict[str, Any]
    ) -> None:
        """Update a record in both master and xref tables.
        
        Args:
            model_cls: OpenMatch model class
            record_id: Record ID to update
            data: Updated record data
        """
        try:
            # Validate data against model
            if error := model_cls.validate(data):
                raise ValueError(f"Invalid record data: {error}")
                
            now = datetime.utcnow()
            master_table = f"{self.schema}.{model_cls.__name__.lower()}_master"
            
            # Update master record
            update_data = {**data, 'updated_at': now}
            set_clause = ', '.join([f"{k} = :{k}" for k in update_data.keys()])
            
            with self.engine.begin() as conn:
                conn.execute(
                    text(f"""
                        UPDATE {master_table}
                        SET {set_clause},
                            version = version + 1
                        WHERE record_id = :record_id
                    """),
                    {**update_data, 'record_id': record_id}
                )
                
            # Update xref record if enabled
            if getattr(model_cls._meta, 'xref', True):
                xref_table = f"{self.schema}.{model_cls.__name__.lower()}_xref"
                with self.engine.begin() as conn:
                    conn.execute(
                        text(f"""
                            UPDATE {xref_table}
                            SET updated_at = :updated_at
                            WHERE record_id = :record_id
                        """),
                        {'updated_at': now, 'record_id': record_id}
                    )
                    
        except Exception as e:
            self.logger.error(f"Failed to update record: {str(e)}")
            raise

    def get_record_history(
        self,
        model_cls: Type[Model],
        record_id: str
    ) -> List[Dict[str, Any]]:
        """Get the history of changes for a record.
        
        Args:
            model_cls: OpenMatch model class
            record_id: Record ID to get history for
            
        Returns:
            List of record versions with changes
        """
        try:
            master_table = f"{self.schema}.{model_cls.__name__.lower()}_master"
            history_table = f"{self.schema}.{model_cls.__name__.lower()}_history"
            
            with self.engine.begin() as conn:
                # Get current record
                current = conn.execute(
                    text(f"SELECT * FROM {master_table} WHERE record_id = :id"),
                    {'id': record_id}
                ).fetchone()
                
                if not current:
                    raise ValueError(f"Record {record_id} not found")
                
                # Get history records if history tracking is enabled
                history = []
                if getattr(model_cls._meta, 'history', True):
                    history = conn.execute(
                        text(f"SELECT * FROM {history_table} WHERE record_id = :id ORDER BY valid_from DESC"),
                        {'id': record_id}
                    ).fetchall()
                
                return [dict(current)] + [dict(h) for h in history]
                
        except Exception as e:
            self.logger.error(f"Failed to get record history: {str(e)}")
            raise

    def delete_record(
        self,
        model_cls: Type[Model],
        record_id: str,
        hard_delete: bool = False
    ) -> None:
        """Delete a record from both master and xref tables.
        
        Args:
            model_cls: OpenMatch model class
            record_id: Record ID to delete
            hard_delete: If True, physically delete the record; otherwise, soft delete
        """
        try:
            master_table = f"{self.schema}.{model_cls.__name__.lower()}_master"
            now = datetime.utcnow()
            
            with self.engine.begin() as conn:
                if hard_delete:
                    # Physical delete
                    conn.execute(
                        text(f"DELETE FROM {master_table} WHERE record_id = :id"),
                        {'id': record_id}
                    )
                else:
                    # Soft delete
                    conn.execute(
                        text(f"""
                            UPDATE {master_table}
                            SET status = 'DELETED',
                                updated_at = :updated_at
                            WHERE record_id = :id
                        """),
                        {'id': record_id, 'updated_at': now}
                    )
                
                # Update xref records if enabled
                if getattr(model_cls._meta, 'xref', True):
                    xref_table = f"{self.schema}.{model_cls.__name__.lower()}_xref"
                    if hard_delete:
                        conn.execute(
                            text(f"DELETE FROM {xref_table} WHERE record_id = :id"),
                            {'id': record_id}
                        )
                    else:
                        conn.execute(
                            text(f"""
                                UPDATE {xref_table}
                                SET status = 'DELETED',
                                    updated_at = :updated_at
                                WHERE record_id = :id
                            """),
                            {'id': record_id, 'updated_at': now}
                        )
                        
        except Exception as e:
            self.logger.error(f"Failed to delete record: {str(e)}")
            raise 