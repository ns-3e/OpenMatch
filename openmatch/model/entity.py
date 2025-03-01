"""
Entity management and operations.
"""

from typing import Dict, List, Optional, Any, Union
import logging
from datetime import datetime
import uuid
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
import json
import psycopg2

from .config import DataModelConfig
from .manager import DataModelManager


class EntityManager:
    """Manages entity operations and relationships.
    
    This class is responsible for managing entity lifecycle operations including:
    - Creating and updating entities
    - Managing relationships between entities
    - Handling entity versioning and history
    - Cross-referencing entities across source systems
    
    Attributes:
        session: SQLAlchemy session for database operations
        model_registry: Registry of entity models
        _cache: Cache for frequently accessed entities
    """
    
    def __init__(self, target_config):
        """Initialize entity manager.
        
        Args:
            target_config: Target database configuration
        """
        self.target_config = target_config
        self.model_registry = {}
        self._cache = {}
        self.logger = logging.getLogger(__name__)
        self.data_model_manager = None
        self.session = None
        self._setup_session()
        
    def _setup_session(self):
        """Set up database session."""
        try:
            engine = sa.create_engine(
                f"postgresql://{self.target_config.username}:{self.target_config.password}"
                f"@{self.target_config.host}:{self.target_config.port}/{self.target_config.database}"
            )
            Session = sa.orm.sessionmaker(bind=engine)
            self.session = Session()
            self.logger.debug("Database session created successfully")
        except Exception as e:
            self.logger.error(f"Failed to create database session: {e}")
            raise
        
    def initialize(self):
        """Initialize the MDM schema and tables."""
        try:
            # Create schema if not exists
            conn = psycopg2.connect(
                dbname=self.target_config.database,
                user=self.target_config.user,
                password=self.target_config.password,
                host=self.target_config.host,
                port=self.target_config.port
            )
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            
            with conn.cursor() as cur:
                # Create schema
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.target_config.schema}")
                
                # Create pgvector extension if available
                try:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                    self.logger.info("Created pgvector extension")
                except Exception as e:
                    self.logger.warning(f"Could not create pgvector extension. Vector operations will use fallback mode: {str(e)}")
                
                # Create base tables
                self._create_base_tables(cur)
                
            conn.close()
            self.logger.info("Database setup completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Error during initialization: {str(e)}")
            raise
            
    def _create_base_tables(self, cur):
        """Create base MDM tables."""
        try:
            # Create UUID extension if not exists
            cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            
            # Create golden records table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.target_config.schema}.golden_records (
                    id UUID PRIMARY KEY,
                    entity_type VARCHAR(50) NOT NULL,
                    source_system VARCHAR(50) NOT NULL,
                    source_id VARCHAR(255) NOT NULL,
                    data JSONB NOT NULL,
                    match_status VARCHAR(50) DEFAULT 'UNMATCHED',
                    match_group_id UUID,
                    match_score FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create match groups table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.target_config.schema}.match_groups (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    entity_type VARCHAR(50) NOT NULL,
                    master_record_id UUID,
                    confidence_score FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create match pairs table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.target_config.schema}.match_pairs (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    record_id_1 UUID REFERENCES {self.target_config.schema}.golden_records(id),
                    record_id_2 UUID REFERENCES {self.target_config.schema}.golden_records(id),
                    match_score FLOAT NOT NULL,
                    match_status VARCHAR(50) DEFAULT 'PENDING',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.logger.info("MDM tables created successfully!")
            
        except Exception as e:
            self.logger.error(f"Error creating MDM tables: {str(e)}")
            raise
            
    def initialize_model(self, model_config: DataModelConfig):
        """Initialize the data model with given configuration."""
        try:
            self.data_model_manager = DataModelManager(
                data_model=model_config,
                db_engine=self.session.bind
            )
            self.data_model_manager.create_physical_model()
            self.logger.info("Successfully initialized data model")
        except Exception as e:
            self.logger.error(f"Failed to initialize data model: {e}")
            raise
            
    def create_entity(self, entity_type: str, data: Dict[str, Any]) -> str:
        """Create a new entity.
        
        Args:
            entity_type: Type of entity (e.g., 'person', 'organization')
            data: Entity data dictionary
            
        Returns:
            str: ID of created entity
        """
        try:
            # Generate unique ID if not provided
            if 'id' not in data:
                data['id'] = str(uuid.uuid4())
                
            # Store in golden records
            self.data_model_manager.store_golden_record({
                'id': data['id'],
                'type': entity_type,
                'data': data,
                'source': data.get('source', 'MANUAL')
            })
            
            return data['id']
            
        except Exception as e:
            self.logger.error(f"Failed to create entity: {e}")
            raise
            
    def get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve an entity by ID.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            Optional[Dict]: Entity data if found, None otherwise
        """
        try:
            stmt = text(
                """
                SELECT data 
                FROM mdm.golden_records 
                WHERE id = :id
                """
            )
            result = self.session.execute(stmt, {'id': entity_id}).fetchone()
            return json.loads(result[0]) if result else None
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve entity {entity_id}: {e}")
            raise
            
    def update_entity(self, entity_id: str, data: Dict[str, Any]) -> bool:
        """Update an existing entity.
        
        Args:
            entity_id: Entity ID
            data: Updated entity data
            
        Returns:
            bool: True if successful, False if entity not found
        """
        try:
            data['id'] = entity_id
            self.data_model_manager.store_golden_record(data)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update entity {entity_id}: {e}")
            raise
            
    def delete_entity(self, entity_id: str) -> bool:
        """Delete an entity.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            bool: True if deleted, False if not found
        """
        try:
            stmt = text(
                """
                DELETE FROM mdm.golden_records 
                WHERE id = :id
                """
            )
            result = self.session.execute(stmt, {'id': entity_id})
            self.session.commit()
            return result.rowcount > 0
            
        except Exception as e:
            self.logger.error(f"Failed to delete entity {entity_id}: {e}")
            raise
            
    def search_entities(
        self,
        entity_type: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Search for entities with optional filtering.
        
        Args:
            entity_type: Optional entity type filter
            filters: Optional dictionary of field filters
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            List[Dict]: List of matching entities
        """
        try:
            query = "SELECT data FROM mdm.golden_records WHERE 1=1"
            params = {}
            
            if entity_type:
                query += " AND data->>'type' = :entity_type"
                params['entity_type'] = entity_type
                
            if filters:
                for key, value in filters.items():
                    query += f" AND data->>'{key}' = :{key}"
                    params[key] = value
                    
            query += " LIMIT :limit OFFSET :offset"
            params.update({'limit': limit, 'offset': offset})
            
            stmt = text(query)
            results = self.session.execute(stmt, params).fetchall()
            
            return [json.loads(row[0]) for row in results]
            
        except Exception as e:
            self.logger.error(f"Failed to search entities: {e}")
            raise
            
    def __del__(self):
        """Clean up resources."""
        if self.session:
            self.session.close() 