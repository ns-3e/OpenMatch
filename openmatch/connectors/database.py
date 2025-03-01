"""
Database connection and configuration management for OpenMatch MDM operations.
"""
from typing import Optional, Dict, Any
import logging
from sqlalchemy import create_engine, MetaData, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Configuration for database connection."""
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "mdm_db",
        username: str = "postgres",
        password: str = None,
        schema: str = "mdm"
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.schema = schema

    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

class DatabaseConnector:
    """Manages database connections and operations for MDM.
    
    This class handles all database-related operations including connection management,
    session handling, and basic database operations. It provides a unified interface
    for interacting with the MDM database.
    
    Attributes:
        config: Database configuration object containing connection details
        engine: SQLAlchemy engine instance
        session_factory: SQLAlchemy session factory
    """
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._engine: Optional[Engine] = None
        self._session_factory = None
        self.metadata = MetaData(schema=config.schema)

    def initialize(self):
        """Initialize database connection and create schema if needed."""
        try:
            self._engine = create_engine(
                self.config.get_connection_string(),
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10
            )
            self._session_factory = sessionmaker(bind=self._engine)
            
            # Create schema if it doesn't exist
            with self._engine.connect() as conn:
                # Create schema using text()
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema}"))
                conn.commit()
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database connection: {str(e)}")
            raise

    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine instance."""
        if not self._engine:
            self.initialize()
        return self._engine

    @contextmanager
    def session(self) -> Session:
        """Provide a transactional scope around a series of operations."""
        if not self._session_factory:
            self.initialize()
            
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def check_connection(self) -> bool:
        """Test database connection.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return True
        except SQLAlchemyError:
            return False

    def get_table_names(self) -> list:
        """Get all tables in the MDM schema.
        
        Returns:
            list: List of table names in the configured schema
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema=self.config.schema) 