"""
Source System Manager for ETL operations.

This module provides functionality for synchronizing data from source systems
into the MDM database.
"""

import logging
import time
import math
from datetime import datetime
from typing import Dict, List, Optional, Any, Generator, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import (
    Progress,
    SpinnerColumn,
    TimeElapsedColumn,
    BarColumn,
    TextColumn,
    MofNCompleteColumn,
    TaskProgressColumn,
    TimeRemainingColumn
)
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..model.entity import EntityManager
from ..match.settings import DatabaseConfig
from .config import TableConfig, SourceConfig, TargetConfig
import json

logger = logging.getLogger(__name__)

class ETLStats:
    """Statistics for ETL operations."""
    
    def __init__(self):
        self.start_time = time.time()
        self.total_records = 0
        self.processed_records = 0
        self.failed_records = 0
        self.current_batch = 0
        self.total_batches = 0
        self.records_per_second = 0.0
        self.estimated_time_remaining = 0
        
    def update(self, processed: int = 0, failed: int = 0):
        """Update ETL statistics."""
        self.processed_records += processed
        self.failed_records += failed
        elapsed_time = time.time() - self.start_time
        self.records_per_second = self.processed_records / elapsed_time if elapsed_time > 0 else 0
        remaining_records = self.total_records - self.processed_records
        self.estimated_time_remaining = remaining_records / self.records_per_second if self.records_per_second > 0 else 0
        
    def get_stats_table(self) -> Table:
        """Get a rich table with current stats."""
        table = Table(title="ETL Progress Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Records", f"{self.total_records:,}")
        table.add_row("Processed Records", f"{self.processed_records:,}")
        table.add_row("Failed Records", f"{self.failed_records:,}")
        table.add_row("Current Batch", f"{self.current_batch}/{self.total_batches}")
        table.add_row("Records/Second", f"{self.records_per_second:.2f}")
        table.add_row("Est. Time Remaining", f"{self.estimated_time_remaining:.1f}s")
        
        return table

class SourceSystemManager:
    """Manages ETL operations from source systems to MDM database.
    
    This class handles the extraction and transformation of data from source systems
    into the MDM database. It manages source system connections, data mapping,
    and synchronization statistics.
    
    Attributes:
        mdm_session: SQLAlchemy session for MDM database
        entity_manager: EntityManager instance for managing entity operations
        config: Source system configuration dictionary
        source_system_id: Identifier for the source system
        source_session: SQLAlchemy session for source system database
        entity_mappings: Dictionary mapping source entities to MDM entities
        stats: ETL statistics tracker
    """

    def __init__(
        self,
        mdm_session: Session,
        entity_manager: EntityManager,
        config: Dict[str, Any],
        source_session: Optional[Session] = None,
        source_system_id: str = None
    ):
        """Initialize the SourceSystemManager.

        Args:
            mdm_session: SQLAlchemy session for MDM database
            entity_manager: EntityManager instance for managing entity operations
            config: Source system configuration dictionary
            source_session: Optional pre-configured source system session
            source_system_id: Optional source system identifier
        """
        self.mdm_session = mdm_session
        self.entity_manager = entity_manager
        self.config = config
        self.source_system_id = source_system_id or config.get('SOURCE_SYSTEM_ID', 'DEFAULT')
        
        if source_session:
            self.source_session = source_session
        else:
            engine = self._create_source_connection()
            self.source_session = Session(engine)

        self.entity_mappings = config.get('ENTITY_MAPPINGS', {})
        self._sync_stats = self._init_sync_stats()
        self.stats = ETLStats()
        self.console = Console()

    def _create_source_connection(self):
        """Create a database connection to the source system.
        
        Returns:
            SQLAlchemy engine instance configured for the source database
        """
        connection_string = (
            f"{self.config['ENGINE']}://{self.config['USER']}:{self.config['PASSWORD']}"
            f"@{self.config['HOST']}:{self.config['PORT']}/{self.config['NAME']}"
        )
        return create_engine(connection_string)

    def _init_sync_stats(self) -> Dict:
        """Initialize synchronization statistics."""
        return {
            'total_processed': 0,
            'new_records': 0,
            'updated_records': 0,
            'failed_records': 0,
            'related_entities': {},
            'start_time': datetime.now(),
            'end_time': None
        }

    def sync_entities(self, last_sync: Optional[datetime] = None) -> Dict:
        """
        Synchronize entities from the source system to MDM.

        Args:
            last_sync: Optional datetime to only sync records modified after this time

        Returns:
            Dictionary containing sync statistics
        """
        try:
            for entity_name, mapping in self.entity_mappings.items():
                logger.info(f"Starting sync for entity: {entity_name}")
                self._sync_entity(entity_name, mapping, last_sync)
                
            self.mdm_session.commit()
            
        except Exception as e:
            logger.error(f"Error during sync: {str(e)}")
            self.mdm_session.rollback()
            raise
        
        finally:
            self._sync_stats['end_time'] = datetime.now()
            
        return self._sync_stats

    def _sync_entity(self, entity_name: str, mapping: Dict, last_sync: Optional[datetime]):
        """
        Synchronize a single entity type from source to MDM.

        Args:
            entity_name: Name of the entity to sync
            mapping: Entity mapping configuration
            last_sync: Optional last sync datetime
        """
        query = text(mapping['query'])
        params = {'last_sync': last_sync, 'source_system': self.source_system_id}
        
        try:
            results = self.source_session.execute(query, params)
            
            for row in results:
                record = dict(row)
                self._sync_stats['total_processed'] += 1
                
                try:
                    # Process main entity
                    entity = self.entity_manager.upsert_entity(
                        entity_name,
                        record,
                        source_system_id=self.source_system_id
                    )
                    
                    # Process child entities if any
                    if 'child_entities' in mapping:
                        self._sync_child_entities(
                            entity_name,
                            entity.id,
                            mapping['child_entities']
                        )
                    
                    if entity.is_new:
                        self._sync_stats['new_records'] += 1
                    else:
                        self._sync_stats['updated_records'] += 1
                        
                except Exception as e:
                    logger.error(f"Failed to sync record: {str(e)}")
                    self._sync_stats['failed_records'] += 1
                    continue
                    
        except SQLAlchemyError as e:
            logger.error(f"Database error during sync: {str(e)}")
            raise

    def _sync_child_entities(
        self,
        parent_entity_name: str,
        parent_id: str,
        child_mappings: Dict
    ):
        """
        Synchronize child entities for a given parent entity.

        Args:
            parent_entity_name: Name of the parent entity
            parent_id: ID of the parent record
            child_mappings: Mapping configurations for child entities
        """
        for child_name, child_mapping in child_mappings.items():
            if child_name not in self._sync_stats['related_entities']:
                self._sync_stats['related_entities'][child_name] = 0
                
            query = text(child_mapping['query'])
            params = {'parent_id': parent_id, 'source_system': self.source_system_id}
            
            try:
                results = self.source_session.execute(query, params)
                
                for row in results:
                    record = dict(row)
                    self.entity_manager.upsert_entity(
                        child_name,
                        record,
                        source_system_id=self.source_system_id
                    )
                    self._sync_stats['related_entities'][child_name] += 1
                    
            except SQLAlchemyError as e:
                logger.error(
                    f"Error syncing child entity {child_name} for parent {parent_id}: {str(e)}"
                )
                continue

    def get_sync_stats(self) -> Dict:
        """Get the current synchronization statistics."""
        return self._sync_stats 

class ETLManager:
    """Manages ETL process for loading data into MDM system.
    
    This class orchestrates the entire ETL (Extract, Transform, Load) process
    for getting data from source systems into the MDM system. It handles
    data extraction, transformation rules, loading strategies, and error handling.
    
    Attributes:
        source_conn: Connection to the source database
        target_conn: Connection to the target MDM database
        stats: Statistics about the ETL process
    """

    def __init__(self, source_config: SourceConfig, target_config: TargetConfig):
        """Initialize ETL manager with source and target configurations."""
        self.source_config = source_config
        self.target_config = target_config
        self.source_conn = None
        self.target_conn = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Establish database connections."""
        try:
            # Connect to source database
            self.source_conn = psycopg2.connect(
                dbname=self.source_config.database,
                user=self.source_config.user,
                password=self.source_config.password,
                host=self.source_config.host,
                port=self.source_config.port,
                cursor_factory=RealDictCursor
            )
            
            # Connect to target database
            self.target_conn = psycopg2.connect(
                dbname=self.target_config.database,
                user=self.target_config.user,
                password=self.target_config.password,
                host=self.target_config.host,
                port=self.target_config.port
            )
            
            self.logger.info("Database connections established successfully")
            
        except Exception as e:
            self.logger.error(f"Error establishing database connections: {str(e)}")
            raise

    def close(self):
        """Close database connections."""
        if self.source_conn:
            self.source_conn.close()
        if self.target_conn:
            self.target_conn.close()
        logger.info("Database connections closed")

    def _get_total_records(self, cur: psycopg2.extensions.cursor, table_config: TableConfig) -> int:
        """Get total number of records to process for a table."""
        query = f"""
            SELECT COUNT(*) as count 
            FROM {self.source_config.schema}.{table_config.table_name}
        """
        cur.execute(query)
        result = cur.fetchone()
        return result['count']

    def _process_batch(self, source_cur, target_cur, table_config: TableConfig, offset: int, batch_size: int) -> int:
        """Process a batch of records."""
        # Build query
        query = f"""
            SELECT * 
            FROM {self.source_config.schema}.{table_config.table_name}
            LIMIT %(batch_size)s OFFSET %(offset)s
        """
        
        params = {
            'batch_size': batch_size,
            'offset': offset
        }
        
        source_cur.execute(query, params)
        records = source_cur.fetchall()
        
        for record in records:
            try:
                self._load_record(target_cur, table_config, record)
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                raise
        
        return len(records)

    def _load_record(self, cur, table_config, record):
        """Load a single record into the target table."""
        try:
            # Prepare field names and values
            field_names = []
            values = []
            update_fields = []
            target_data = {}
            
            # Track if timestamp fields are already present
            has_created_at = False
            has_updated_at = False
            
            for field in table_config.fields:
                if field.name in record:
                    field_names.append(field.name)
                    values.append(f"%({field.name})s")
                    if not field.is_key:  # Don't update key fields
                        update_fields.append(f"{field.name} = EXCLUDED.{field.name}")
                    target_data[field.name] = record[field.name]
                    
                    # Check for timestamp fields
                    if field.name == 'created_at':
                        has_created_at = True
                    elif field.name == 'updated_at':
                        has_updated_at = True
            
            # Add timestamp fields if not present
            if not has_created_at:
                field_names.append('created_at')
                values.append('CURRENT_TIMESTAMP')
            if not has_updated_at:
                field_names.append('updated_at')
                values.append('CURRENT_TIMESTAMP')
                update_fields.append('updated_at = CURRENT_TIMESTAMP')
            
            # Construct SQL
            insert_sql = f"""
                INSERT INTO {self.target_config.schema}.{table_config.table_name}
                ({', '.join(field_names)})
                VALUES ({', '.join(values)})
                ON CONFLICT (id) DO UPDATE SET
                {', '.join(update_fields)}
            """
            
            # Execute insert
            cur.execute(insert_sql, target_data)
            self.logger.debug(f"Inserted/updated record in {table_config.table_name}")
            
        except Exception as e:
            self.logger.error(f"Error inserting record into {table_config.table_name}: {str(e)}")
            raise

    def _create_target_tables(self, cur):
        """Create target tables if they don't exist."""
        try:
            for table_config in self.source_config.tables:
                # Drop existing table
                cur.execute(f"DROP TABLE IF EXISTS {self.target_config.schema}.{table_config.table_name} CASCADE")
                
                # Get field definitions
                field_defs = []
                constraints = []
                has_id = False
                has_created_at = False
                has_updated_at = False
                
                for field in table_config.fields:
                    if field.name == 'id':
                        has_id = True
                        field_def = f"{field.name} {field.data_type} PRIMARY KEY"
                    elif field.name == 'created_at':
                        has_created_at = True
                        field_def = f"{field.name} {field.data_type} DEFAULT CURRENT_TIMESTAMP"
                    elif field.name == 'updated_at':
                        has_updated_at = True
                        field_def = f"{field.name} {field.data_type} DEFAULT CURRENT_TIMESTAMP"
                    else:
                        field_def = f"{field.name} {field.data_type}"
                    field_defs.append(field_def)
                    
                    # Add foreign key constraints
                    if field.is_parent_key:
                        parent_table = self.source_config.get_parent_table(table_config.table_name)
                        if parent_table:
                            constraints.append(
                                f"FOREIGN KEY ({field.name}) REFERENCES {self.target_config.schema}.{parent_table} (id)"
                            )
                
                # Add metadata fields if they don't exist
                if not has_created_at:
                    field_defs.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                if not has_updated_at:
                    field_defs.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                
                # Create table
                create_table_sql = f"""
                    CREATE TABLE {self.target_config.schema}.{table_config.table_name} (
                        {', '.join(field_defs)}
                        {', ' + ', '.join(constraints) if constraints else ''}
                    )
                """
                
                cur.execute(create_table_sql)
                
                # Create indexes
                if table_config.parent_key:
                    index_name = f"idx_{table_config.table_name}_{table_config.parent_key}"
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {self.target_config.schema}.{table_config.table_name} ({table_config.parent_key})
                    """)
                
                self.logger.info(f"Created/verified table {table_config.table_name}")
                
        except Exception as e:
            self.logger.error(f"Error creating target tables: {str(e)}")
            raise

    def load_data(self):
        """Load data from source to target.
        
        Executes the complete ETL process, extracting data from the source system,
        applying transformations, and loading it into the target MDM system.
        Handles errors and maintains statistics throughout the process.
        """
        try:
            if not self.source_conn or not self.target_conn:
                self.connect()
            
            source_cur = self.source_conn.cursor()
            target_cur = self.target_conn.cursor()
            
            # Create target tables
            self._create_target_tables(target_cur)
            self.target_conn.commit()
            
            # Process each table
            for table_config in self.source_config.tables:
                logger.info(f"Processing table: {table_config.table_name}")
                
                # Get total records
                total_records = self._get_total_records(source_cur, table_config)
                logger.info(f"Found {total_records} records to process")
                
                # Process in batches
                offset = 0
                batch_size = 1000
                processed_records = 0
                
                while offset < total_records:
                    try:
                        records_processed = self._process_batch(
                            source_cur, target_cur, table_config, offset, batch_size
                        )
                        processed_records += records_processed
                        offset += batch_size
                        self.target_conn.commit()
                        
                    except Exception as e:
                        self.target_conn.rollback()
                        logger.error(f"Error processing batch: {str(e)}")
                        raise
                
                logger.info(f"Table {table_config.table_name} completed: {processed_records} processed")
            
        except Exception as e:
            logger.error(f"Error in ETL process: {str(e)}")
            raise
        
        finally:
            if self.source_conn:
                self.source_conn.close()
            if self.target_conn:
                self.target_conn.close()

    def get_stats(self) -> Dict:
        """Get current ETL statistics.
        
        Returns:
            Dict containing various ETL process statistics including:
            - total_records: Total number of records to process
            - processed_records: Number of records processed so far
            - failed_records: Number of records that failed processing
            - records_per_second: Processing rate
            - estimated_time_remaining: Estimated time to completion
            - elapsed_time: Total time elapsed since start
        """
        return {
            'total_records': self.stats.total_records,
            'processed_records': self.stats.processed_records,
            'failed_records': self.stats.failed_records,
            'records_per_second': self.stats.records_per_second,
            'estimated_time_remaining': self.stats.estimated_time_remaining,
            'elapsed_time': time.time() - self.stats.start_time
        } 