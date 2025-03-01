"""
Database initialization and schema creation utilities.
"""
import logging
from typing import Optional
from .database import DatabaseConnector, DatabaseConfig
from .schema import Base

logger = logging.getLogger(__name__)

def init_database(
    host: str = "localhost",
    port: int = 5432,
    database: str = "mdm_db",
    username: str = "postgres",
    password: Optional[str] = None,
    schema: str = "mdm"
) -> DatabaseConnector:
    """
    Initialize the database connection and create all necessary tables.
    
    Args:
        host: Database host
        port: Database port
        database: Database name
        username: Database username
        password: Database password
        schema: Schema name for MDM tables
        
    Returns:
        DatabaseConnector instance
    """
    try:
        # Create database configuration
        config = DatabaseConfig(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema
        )
        
        # Initialize database connector
        connector = DatabaseConnector(config)
        connector.initialize()
        
        # Create all tables
        Base.metadata.schema = schema
        Base.metadata.create_all(connector.engine)
        
        logger.info(f"Successfully initialized database and created schema '{schema}'")
        return connector
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

def reset_schema(connector: DatabaseConnector) -> None:
    """
    Drop and recreate all tables in the schema.
    Use with caution as this will delete all data!
    
    Args:
        connector: DatabaseConnector instance
    """
    try:
        # Drop all tables
        Base.metadata.drop_all(connector.engine)
        
        # Recreate all tables
        Base.metadata.create_all(connector.engine)
        
        logger.info(f"Successfully reset schema '{connector.config.schema}'")
        
    except Exception as e:
        logger.error(f"Failed to reset schema: {str(e)}")
        raise 