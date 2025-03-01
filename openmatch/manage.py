#!/usr/bin/env python
"""
Management script for OpenMatch MDM system.
Similar to Django's manage.py, this script provides commands for managing the MDM system.
"""
import os
import sys
import argparse
import logging
import logging.config
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from openmatch.match.settings import MDM_DB, VECTOR_SETTINGS, LOGGING
from openmatch.match.db_ops import DatabaseOptimizer
from openmatch.match.engine import MatchEngine
from .match.settings import DatabaseConfig
from .model.entity import EntityManager
from .model.config import DataModelConfig

# Set up logging
logging.config.dictConfig(LOGGING)
logger = logging.getLogger('openmatch')

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class MDMManager:
    """Manages MDM system initialization and operations."""
    
    def __init__(self, db_config: DatabaseConfig):
        """Initialize MDM manager with database configuration."""
        self.db_config = db_config
        self.engine = None
        self.session = None
        self.entity_manager = None
        
    def initialize_system(self, data_model: Optional[DataModelConfig] = None):
        """Initialize the complete MDM system.
        
        Args:
            data_model: Optional data model configuration
        """
        try:
            # Setup database and schema
            self.setup_database()
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                f"postgresql://{self.db_config.user}:{self.db_config.password}"
                f"@{self.db_config.host}:{self.db_config.port}/{self.db_config.name}"
            )
            
            # Create MDM tables
            self.create_mdm_tables()
            
            # Create session
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            
            # Initialize entity manager
            self.entity_manager = EntityManager(self.session)
            if data_model:
                self.entity_manager.initialize_model(data_model)
                
            logger.info("MDM system initialization completed successfully!")
            
        except Exception as e:
            logger.error(f"Error during MDM initialization: {e}")
            raise
            
    def setup_database(self):
        """Set up the MDM database and schema."""
        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            user=self.db_config.user,
            password=self.db_config.password
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        try:
            # Create MDM database if it doesn't exist
            cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.db_config.name}'")
            if not cur.fetchone():
                logger.info(f"Creating database {self.db_config.name}...")
                cur.execute(f"CREATE DATABASE {self.db_config.name}")
            
            # Close connection to server and connect to mdm database
            cur.close()
            conn.close()
            
            # Connect to MDM database
            conn = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.name
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            
            # Create MDM schema if it doesn't exist
            logger.info("Creating MDM schema...")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.db_config.schema}")
            
            # Try to create pgvector extension
            try:
                logger.info("Creating pgvector extension...")
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                logger.info("pgvector extension created successfully!")
            except psycopg2.Error as e:
                logger.warning(f"Could not create pgvector extension. Vector operations will use fallback mode: {e}")
            
            logger.info("Database setup completed successfully!")
            
        except Exception as e:
            logger.error(f"Error during database setup: {e}")
            raise
            
        finally:
            cur.close()
            conn.close()
            
    def create_mdm_tables(self):
        """Create MDM tables."""
        logger.info("Creating MDM tables...")
        
        # Check if pgvector is available
        has_vector = False
        with self.engine.connect() as conn:
            try:
                result = conn.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'"))
                has_vector = result.fetchone() is not None
                if has_vector:
                    logger.info("pgvector extension is available, creating tables with vector support")
                else:
                    logger.info("pgvector extension is not available, creating tables without vector support")
            except:
                logger.info("Could not check for pgvector extension, creating tables without vector support")
        
        # Create tables using raw SQL for more control
        with self.engine.begin() as conn:  # Use transaction
            try:
                # Source Records table
                base_source_records = """
                    CREATE TABLE IF NOT EXISTS mdm.source_records (
                        id VARCHAR(255) PRIMARY KEY,
                        source_system VARCHAR(100) NOT NULL,
                        entity_type VARCHAR(100) NOT NULL,
                        data JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                
                # Golden Records table
                base_golden_records = """
                    CREATE TABLE IF NOT EXISTS mdm.golden_records (
                        id VARCHAR(255) PRIMARY KEY,
                        entity_type VARCHAR(100) NOT NULL,
                        data JSONB NOT NULL,
                        match_score FLOAT,
                        confidence_score FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                
                # Create base tables
                conn.execute(text(base_source_records))
                conn.execute(text(base_golden_records))
                
                # Add vector columns if pgvector is available
                if has_vector:
                    try:
                        conn.execute(text("""
                            ALTER TABLE mdm.source_records 
                            ADD COLUMN IF NOT EXISTS vector_embedding VECTOR(384)
                        """))
                        conn.execute(text("""
                            ALTER TABLE mdm.golden_records 
                            ADD COLUMN IF NOT EXISTS vector_embedding VECTOR(384)
                        """))
                        logger.info("Added vector columns to tables")
                    except Exception as e:
                        logger.warning(f"Failed to add vector columns: {e}")
                        has_vector = False  # Disable vector features if column addition fails
                
                # Match Results table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS mdm.match_results (
                        id SERIAL PRIMARY KEY,
                        source_record_id VARCHAR(255) REFERENCES mdm.source_records(id),
                        golden_record_id VARCHAR(255) REFERENCES mdm.golden_records(id),
                        match_score FLOAT NOT NULL,
                        match_type VARCHAR(50) NOT NULL,
                        match_rule_id VARCHAR(100),
                        match_model_id VARCHAR(100),
                        match_details JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Match Rules table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS mdm.match_rules (
                        id VARCHAR(100) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        description TEXT,
                        config JSONB NOT NULL,
                        is_active BOOLEAN DEFAULT true,
                        priority INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Create indexes
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_source_records_type ON mdm.source_records(entity_type);
                    CREATE INDEX IF NOT EXISTS idx_source_records_source ON mdm.source_records(source_system);
                    CREATE INDEX IF NOT EXISTS idx_golden_records_type ON mdm.golden_records(entity_type);
                    CREATE INDEX IF NOT EXISTS idx_match_results_scores ON mdm.match_results(match_score);
                """))
                
                # Create vector indexes if pgvector is available
                if has_vector:
                    try:
                        conn.execute(text("""
                            CREATE INDEX IF NOT EXISTS idx_source_records_vector 
                            ON mdm.source_records 
                            USING ivfflat (vector_embedding vector_cosine_ops)
                            WITH (lists = 100)
                        """))
                        
                        conn.execute(text("""
                            CREATE INDEX IF NOT EXISTS idx_golden_records_vector 
                            ON mdm.golden_records 
                            USING ivfflat (vector_embedding vector_cosine_ops)
                            WITH (lists = 100)
                        """))
                        logger.info("Created vector indexes")
                    except Exception as e:
                        logger.warning(f"Failed to create vector indexes: {e}")
                    
                logger.info("MDM tables created successfully!")
                
            except Exception as e:
                logger.error(f"Error creating tables: {e}")
                raise
                
    def load_source_data(self, data_file: str, source_system: str):
        """Load source data from a JSON file into the MDM system.
        
        Args:
            data_file: Path to JSON file containing source records
            source_system: Name of the source system
        """
        logger.info(f"Loading source data from {data_file}...")
        
        try:
            with open(data_file, 'r') as f:
                records = json.load(f)
                
            for record in records:
                # Add source system information
                record['source_system'] = source_system
                record['created_at'] = datetime.utcnow()
                
                # Insert into source_records table
                stmt = text("""
                    INSERT INTO mdm.source_records (
                        id, source_system, entity_type, data, created_at
                    ) VALUES (
                        :id, :source_system, :entity_type, :data, :created_at
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        data = EXCLUDED.data,
                        updated_at = CURRENT_TIMESTAMP
                """)
                
                self.session.execute(stmt, {
                    'id': record.get('id'),
                    'source_system': source_system,
                    'entity_type': record.get('entity_type'),
                    'data': json.dumps(record, cls=DateTimeEncoder),
                    'created_at': record['created_at']
                })
                
            self.session.commit()
            logger.info(f"Successfully loaded {len(records)} records from {source_system}")
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error loading source data: {e}")
            raise
            
    def load_source_records(self, records: List[Dict[str, Any]], source_system: str):
        """Load source records directly from a list of dictionaries.
        
        Args:
            records: List of record dictionaries
            source_system: Name of the source system
        """
        logger.info(f"Loading {len(records)} records from {source_system}...")
        
        try:
            for record in records:
                # Add source system information
                record['source_system'] = source_system
                record['created_at'] = datetime.utcnow()
                
                # Insert into source_records table
                stmt = text("""
                    INSERT INTO mdm.source_records (
                        id, source_system, entity_type, data, created_at
                    ) VALUES (
                        :id, :source_system, :entity_type, :data, :created_at
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        data = EXCLUDED.data,
                        updated_at = CURRENT_TIMESTAMP
                """)
                
                self.session.execute(stmt, {
                    'id': record.get('id'),
                    'source_system': source_system,
                    'entity_type': record.get('entity_type'),
                    'data': json.dumps(record, cls=DateTimeEncoder),
                    'created_at': record['created_at']
                })
                
            self.session.commit()
            logger.info(f"Successfully loaded {len(records)} records from {source_system}")
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error loading source records: {e}")
            raise
            
    def close(self):
        """Close database connections."""
        if self.session:
            self.session.close()

def get_database_url():
    """Get SQLAlchemy database URL from settings."""
    return f"postgresql://{MDM_DB['USER']}:{MDM_DB['PASSWORD']}@{MDM_DB['HOST']}:{MDM_DB['PORT']}/{MDM_DB['NAME']}"

def get_session():
    """Create a database session."""
    engine = create_engine(get_database_url())
    Session = sessionmaker(bind=engine)
    return Session()

def init_db(args):
    """Initialize the database schema and extensions."""
    session = get_session()
    
    try:
        # Create schema if it doesn't exist
        session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {MDM_DB['SCHEMA']}"))
        
        # Initialize database optimizer with vector support
        optimizer = DatabaseOptimizer(session, MDM_DB)
        
        # Set up vector extension and tables
        optimizer.setup_vector_extension()
        
        # Set up other required tables
        optimizer.setup_job_tracking_tables()
        optimizer.setup_partitions()
        optimizer.create_materialized_views()
        optimizer.optimize_tables()
        
        session.commit()
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Database initialization failed: {str(e)}")
        raise
    finally:
        session.close()

def process_matches(args):
    """Process matches for records."""
    session = get_session()
    
    try:
        # Initialize match engine
        engine = MatchEngine(session)
        
        # Process matches
        processor = BatchProcessor(
            session=session,
            match_engine=engine,
            batch_size=args.batch_size or PROCESSING['BATCH_SIZE']
        )
        
        processor.process_matches()
        logger.info("Match processing completed successfully")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Match processing failed: {str(e)}")
        raise
    finally:
        session.close()

def refresh_views(args):
    """Refresh materialized views."""
    session = get_session()
    
    try:
        optimizer = DatabaseOptimizer(session, MDM_DB)
        optimizer.refresh_materialized_views(concurrent=True)
        logger.info("Materialized views refreshed successfully")
        
    except Exception as e:
        session.rollback()
        logger.error(f"View refresh failed: {str(e)}")
        raise
    finally:
        session.close()

def main():
    """Main entry point for the management script."""
    parser = argparse.ArgumentParser(description='OpenMatch MDM management tool')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # init_db command
    init_parser = subparsers.add_parser('init_db', help='Initialize database schema and extensions')
    
    # process_matches command
    process_parser = subparsers.add_parser('process_matches', help='Process matches for records')
    process_parser.add_argument('--batch-size', type=int, help='Batch size for processing')
    
    # refresh_views command
    refresh_parser = subparsers.add_parser('refresh_views', help='Refresh materialized views')
    
    args = parser.parse_args()
    
    if args.command == 'init_db':
        init_db(args)
    elif args.command == 'process_matches':
        process_matches(args)
    elif args.command == 'refresh_views':
        refresh_views(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 