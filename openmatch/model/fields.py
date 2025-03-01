"""
Field classes for OpenMatch models.

This module provides the field types used to define model attributes in OpenMatch.
Each field type handles validation, conversion, and storage of different data types.
"""

from typing import Any, Dict, Optional, Type, List, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import numpy as np


class Field:
    """Base class for model fields.
    
    This class provides the foundation for all field types in OpenMatch models.
    It handles basic field operations like initialization, value getting/setting,
    and type conversion.
    
    Attributes:
        null: Whether the field can be null
        blank: Whether the field can be blank (empty string)
        default: Default value for the field
        unique: Whether the field value must be unique
        primary_key: Whether this field is the primary key
        help_text: Description of the field
        verbose_name: Human-readable name for the field
        name: Name of the field (set by model)
        model: Model class this field belongs to
    """
    
    def __init__(
        self,
        null: bool = False,
        blank: bool = False,
        default: Any = None,
        unique: bool = False,
        primary_key: bool = False,
        help_text: str = "",
        verbose_name: str = None
    ):
        """Initialize field with configuration options.
        
        Args:
            null: Whether the field can be null
            blank: Whether the field can be blank
            default: Default value for the field
            unique: Whether the field value must be unique
            primary_key: Whether this field is the primary key
            help_text: Description of the field
            verbose_name: Human-readable name for the field
        """
        self.null = null
        self.blank = blank
        self.default = default
        self.unique = unique
        self.primary_key = primary_key
        self.help_text = help_text
        self.verbose_name = verbose_name
        
        self.name = None
        self.model = None
        
    def contribute_to_class(self, cls: Type, name: str):
        """Initialize field on model class.
        
        Args:
            cls: Model class this field belongs to
            name: Name of the field
        """
        self.name = name
        self.model = cls
        setattr(cls, name, self)
        
    def __get__(self, instance, owner):
        """Get field value from instance.
        
        Args:
            instance: Model instance
            owner: Model class
            
        Returns:
            Field value or field instance if accessed on class
        """
        if instance is None:
            return self
        return instance.__dict__.get(self.name, self.default)
        
    def __set__(self, instance, value):
        """Set field value on instance.
        
        Args:
            instance: Model instance
            value: Value to set
        """
        instance.__dict__[self.name] = self.to_python(value)
        
    def to_python(self, value: Any) -> Any:
        """Convert value to Python type.
        
        Args:
            value: Value to convert
            
        Returns:
            Converted value
        """
        if value is None and self.null:
            return None
        return value
        
    def get_prep_value(self, value: Any) -> Any:
        """Prepare value for database storage.
        
        Args:
            value: Value to prepare
            
        Returns:
            Value ready for database storage
        """
        return value


class CharField(Field):
    """String field with optional maximum length.
    
    Attributes:
        max_length: Maximum length of the string
    """
    
    def __init__(self, max_length: int = None, **kwargs):
        """Initialize CharField.
        
        Args:
            max_length: Maximum length of the string
            **kwargs: Additional field options
        """
        super().__init__(**kwargs)
        self.max_length = max_length
        
    def to_python(self, value: Any) -> Optional[str]:
        """Convert value to string.
        
        Args:
            value: Value to convert
            
        Returns:
            String value or None if null
        """
        if value is None and self.null:
            return None
        return str(value)


class IntegerField(Field):
    """Integer field."""
    
    def to_python(self, value: Any) -> Optional[int]:
        if value is None and self.null:
            return None
        return int(value)


class FloatField(Field):
    """Floating point number field."""
    
    def to_python(self, value: Any) -> Optional[float]:
        """Convert value to float.
        
        Args:
            value: Value to convert
            
        Returns:
            Float value or None if null
        """
        if value is None and self.null:
            return None
        return float(value)


class BooleanField(Field):
    """Boolean field."""
    
    def to_python(self, value: Any) -> Optional[bool]:
        """Convert value to boolean.
        
        Args:
            value: Value to convert
            
        Returns:
            Boolean value or None if null
        """
        if value is None and self.null:
            return None
        return bool(value)


class DateTimeField(Field):
    """DateTime field."""
    
    def __init__(self, auto_now: bool = False, auto_now_add: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add
        
    def to_python(self, value: Any) -> Optional[datetime]:
        if value is None and self.null:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise ValueError(f"Cannot convert {value} to datetime")
        
    def __set__(self, instance, value):
        """Set datetime value with auto_now/auto_now_add support."""
        if self.auto_now or (self.auto_now_add and instance.__dict__.get(self.name) is None):
            value = datetime.now()
        super().__set__(instance, value)


class JSONField(Field):
    """JSON field with optional schema validation.
    
    Attributes:
        schema: Optional JSON schema for validation
    """
    
    def __init__(
        self,
        schema: Optional[Dict] = None,
        **kwargs
    ):
        """Initialize JSONField.
        
        Args:
            schema: Optional JSON schema for validation
            **kwargs: Additional field options
        """
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