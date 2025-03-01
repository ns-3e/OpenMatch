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

from .config import DataModelConfig
from .manager import DataModelManager


class EntityManager:
    """Manages entity operations and relationships."""
    
    def __init__(self, session: Session):
        """Initialize entity manager with database session."""
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.data_model_manager = None
        
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