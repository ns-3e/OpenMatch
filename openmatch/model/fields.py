"""
Field definitions for OpenMatch models.
"""

from typing import Any, Dict, Optional, Type, List, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import numpy as np


class Field:
    """Base field class for model attributes."""
    
    def __init__(
        self,
        verbose_name: Optional[str] = None,
        description: Optional[str] = None,
        required: bool = True,
        unique: bool = False,
        primary_key: bool = False,
        default: Any = None,
        choices: Optional[list] = None,
        validators: Optional[list] = None,
        **kwargs
    ):
        self.verbose_name = verbose_name
        self.description = description
        self.required = required
        self.unique = unique
        self.primary_key = primary_key
        self.default = default
        self.choices = choices
        self.validators = validators or []
        self.name = None  # Set by ModelBase during model creation
        
    def contribute_to_class(self, cls, name):
        """Hook for performing additional initialization when adding to class."""
        self.name = name
        setattr(cls, name, None)


class CharField(Field):
    """String field."""
    
    def __init__(
        self,
        max_length: Optional[int] = None,
        min_length: Optional[int] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.max_length = max_length
        self.min_length = min_length


class IntegerField(Field):
    """Integer field."""
    
    def __init__(
        self,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.min_value = min_value
        self.max_value = max_value


class FloatField(Field):
    """Float field."""
    
    def __init__(
        self,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        decimal_places: Optional[int] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.min_value = min_value
        self.max_value = max_value
        self.decimal_places = decimal_places


class BooleanField(Field):
    """Boolean field."""
    
    def __init__(self, **kwargs):
        kwargs['required'] = kwargs.get('required', False)
        super().__init__(**kwargs)


class DateTimeField(Field):
    """DateTime field."""
    
    def __init__(
        self,
        auto_now: bool = False,
        auto_now_add: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add


class JSONField(Field):
    """JSON field."""
    
    def __init__(
        self,
        schema: Optional[Dict] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.schema = schema


class ForeignKey(Field):
    """Foreign key relationship."""
    
    def __init__(
        self,
        to: str,
        on_delete: str = 'CASCADE',
        related_name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.to = to
        self.on_delete = on_delete
        self.related_name = related_name


class ManyToManyField(Field):
    """Many-to-many relationship."""
    
    def __init__(
        self,
        to: str,
        through: Optional[str] = None,
        through_fields: Optional[tuple] = None,
        related_name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.to = to
        self.through = through
        self.through_fields = through_fields
        self.related_name = related_name 


class VectorField(Field):
    """Field for storing vector embeddings with support for vector operations."""
    
    def __init__(
        self,
        dimensions: int,
        distance_metric: str = 'cosine',
        index_type: str = 'ivfflat',
        lists: int = 100,  # IVFFlat parameter: number of lists to partition vectors into
        probes: int = 10,  # Search parameter: number of lists to probe
        **kwargs
    ):
        """Initialize vector field.
        
        Args:
            dimensions: Size of the vector (number of dimensions)
            distance_metric: Distance metric to use ('cosine', 'l2', 'inner')
            index_type: Type of index to use ('ivfflat', 'hnsw')
            lists: Number of lists for IVFFlat index
            probes: Number of lists to probe during search
        """
        super().__init__(**kwargs)
        self.dimensions = dimensions
        self.distance_metric = distance_metric
        self.index_type = index_type
        self.lists = lists
        self.probes = probes
        
    def validate_value(self, value: Union[list, np.ndarray]) -> Optional[str]:
        """Validate vector value."""
        if value is None and not self.required:
            return None
            
        try:
            if isinstance(value, list):
                value = np.array(value)
            
            if not isinstance(value, np.ndarray):
                return "Value must be a list or numpy array"
                
            if len(value.shape) != 1:
                return "Vector must be 1-dimensional"
                
            if value.shape[0] != self.dimensions:
                return f"Vector must have exactly {self.dimensions} dimensions"
                
        except Exception as e:
            return str(e)
            
        return None
        
    def to_db_value(self, value: Union[list, np.ndarray]) -> list:
        """Convert value to database format."""
        if value is None:
            return None
            
        if isinstance(value, list):
            return value
            
        return value.tolist()
        
    def from_db_value(self, value: list) -> np.ndarray:
        """Convert database value to numpy array."""
        if value is None:
            return None
            
        return np.array(value)
        
    def get_index_definition(self) -> Dict[str, Any]:
        """Get vector index definition."""
        return {
            'type': self.index_type,
            'params': {
                'distance_metric': self.distance_metric,
                'lists': self.lists,
                'probes': self.probes,
            }
        } 