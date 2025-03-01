"""
Source System Manager for ETL operations.

This module provides functionality for synchronizing data from source systems
into the MDM database.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..model.entity import EntityManager

logger = logging.getLogger(__name__)

class SourceSystemManager:
    """Manages ETL operations from source systems to MDM database."""

    def __init__(
        self,
        mdm_session: Session,
        entity_manager: EntityManager,
        config: Dict[str, Any],
        source_session: Optional[Session] = None,
        source_system_id: str = None
    ):
        """
        Initialize the SourceSystemManager.

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

    def _create_source_connection(self):
        """Create a database connection to the source system."""
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