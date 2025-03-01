#!/usr/bin/env python
"""
Management script for OpenMatch MDM system.
Similar to Django's manage.py, this script provides commands for managing the MDM system.
"""
import os
import sys
import argparse
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from match.settings import MDM_DB, VECTOR_SETTINGS, LOGGING
from match.db_ops import DatabaseOptimizer
from match.engine import MatchEngine

# Set up logging
logging.config.dictConfig(LOGGING)
logger = logging.getLogger('openmatch')

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