"""
Model management module for OpenMatch.
"""

from .manager import DataModelManager
from .config import (
    DataModelConfig,
    EntityConfig,
    FieldConfig,
    DataType,
    PhysicalModelConfig
)

__all__ = [
    'DataModelManager',
    'DataModelConfig',
    'EntityConfig',
    'FieldConfig',
    'DataType',
    'PhysicalModelConfig'
]

class DataModelManager:
    """Manages data model and physical tables."""
    def __init__(self, data_model, engine):
        self.data_model = data_model
        self.engine = engine
        
    def create_physical_model(self):
        """Create physical tables."""
        pass 