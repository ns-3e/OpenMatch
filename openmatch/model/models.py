"""
Base model classes for OpenMatch.
"""

from typing import Any, Dict, List, Optional, Type, ClassVar
from dataclasses import dataclass, field
import inspect
import json
from datetime import datetime
from copy import deepcopy

from .fields import Field, CharField, DateTimeField, FloatField


class ModelOptions:
    """Class to store model metadata."""
    
    def __init__(self, options=None):
        self.table_name = None
        self.verbose_name = None
        self.verbose_name_plural = None
        self.description = None
        self.indexes = []
        self.unique_together = []
        self.ordering = []
        self.history = True
        self.xref = True
        
        # Apply any provided options
        if options:
            for key, value in options.__dict__.items():
                if not key.startswith('_'):
                    setattr(self, key, value)


class ModelBase(type):
    """Metaclass for OpenMatch models."""
    
    def __new__(cls, name, bases, attrs):
        """Create a new model class."""
        # Skip processing for Model class itself
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            return super().__new__(cls, name, bases, attrs)
            
        # Create new class
        new_attrs = {}
        fields = {}
        meta = None
        
        # Process attributes
        for key, value in attrs.items():
            if key == 'Meta':
                meta = value
            elif isinstance(value, Field):
                fields[key] = value
            else:
                new_attrs[key] = value
                
        # Create and store model options
        options = ModelOptions(meta)
        if not options.table_name:
            options.table_name = name.lower()
            
        # Store fields and options in class
        new_attrs['_fields'] = fields
        new_attrs['_meta'] = options
        
        # Create the class
        new_class = super().__new__(cls, name, bases, new_attrs)
        
        # Initialize fields
        for field_name, field in fields.items():
            field.contribute_to_class(new_class, field_name)
            
        return new_class


class Model(metaclass=ModelBase):
    """Base class for OpenMatch models.
    
    This class provides the foundation for all MDM entity models. It implements
    core functionality for model instance management, serialization, validation,
    and data access.
    
    Attributes:
        _fields: Dictionary of field instances defined on the model
        _meta: Model metadata options
        _data: Dictionary storing the actual field values
    
    Example:
        class Person(Model):
            first_name = CharField(max_length=100)
            last_name = CharField(max_length=100)
            birth_date = DateField()
            
            class Meta:
                table_name = 'persons'
                indexes = [('first_name', 'last_name')]
    """
    
    class Meta:
        """Model metadata configuration class.
        
        This inner class allows configuration of model-wide settings like:
        - table_name: Name of the database table
        - verbose_name: Human-readable name for the model
        - indexes: Database indexes to create
        - ordering: Default ordering for queries
        """
        pass
    
    def __init__(self, **kwargs):
        """Initialize model instance with field values.
        
        Args:
            **kwargs: Field values to set on the instance
        """
        self._data = {}
        for name, value in kwargs.items():
            setattr(self, name, value)
            self._data[name] = value
            
    @classmethod
    def get_fields(cls) -> Dict[str, Field]:
        """Get all fields defined on the model.
        
        Returns:
            Dictionary mapping field names to Field instances
        """
        return cls._fields
        
    @classmethod
    def get_field(cls, name: str) -> Optional[Field]:
        """Get a specific field by name.
        
        Args:
            name: Name of the field to retrieve
            
        Returns:
            Field instance if found, None otherwise
        """
        return cls._fields.get(name)
        
    @classmethod
    def get_meta(cls) -> ModelOptions:
        """Get model metadata options.
        
        Returns:
            ModelOptions instance containing model configuration
        """
        return cls._meta

    def to_dict(self) -> Dict[str, Any]:
        """Convert model instance to dictionary.
        
        Returns:
            Dictionary containing all field values
        """
        return self._data.copy()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Model':
        """Create model instance from dictionary.
        
        Args:
            data: Dictionary of field values
            
        Returns:
            New model instance initialized with the provided data
        """
        return cls(**data)

    def to_json(self) -> str:
        """Convert model instance to JSON string.
        
        Returns:
            JSON string representation of the model data
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> 'Model':
        """Create model instance from JSON string.
        
        Args:
            json_str: JSON string containing field values
            
        Returns:
            New model instance initialized with the parsed JSON data
        """
        data = json.loads(json_str)
        return cls.from_dict(data)

    def __eq__(self, other: Any) -> bool:
        """Compare model instances for equality.
        
        Args:
            other: Another object to compare with
            
        Returns:
            True if other is same type and has identical field values
        """
        if not isinstance(other, self.__class__):
            return False
        return self._data == other._data

    def __repr__(self) -> str:
        """Get string representation of model instance.
        
        Returns:
            String showing class name and field values
        """
        attrs = [f"{k}={v!r}" for k, v in self._data.items()]
        return f"{self.__class__.__name__}({', '.join(attrs)})"

    def copy(self) -> 'Model':
        """Create a deep copy of the model instance.
        
        Returns:
            New model instance with identical field values
        """
        return self.__class__(**deepcopy(self._data))

    def update(self, data: Dict[str, Any]) -> None:
        """Update model instance with new data.
        
        Args:
            data: Dictionary of field values to update
        """
        for key, value in data.items():
            if key in self._fields:
                setattr(self, key, value)
                self._data[key] = value

    @classmethod
    def validate(cls, data: Dict[str, Any]) -> Optional[str]:
        """Validate data against model fields.
        
        Args:
            data: Dictionary of field values to validate
            
        Returns:
            Error message if validation fails, None otherwise
        """
        for field_name, field in cls._fields.items():
            if not field.null and field_name not in data:
                return f"Field '{field_name}' is required"
            if field_name in data:
                try:
                    field.validate(data[field_name])
                except ValueError as e:
                    return str(e)
        return None


class HistoryModel(Model):
    """Base class for models with history tracking."""
    
    valid_from = DateTimeField(auto_now_add=True)
    valid_to = DateTimeField(null=True, blank=True)
    change_type = CharField(max_length=50)
    change_user = CharField(max_length=100)
    
    class Meta:
        abstract = True


class XrefModel(Model):
    """Base class for cross-reference models."""
    
    source_id = CharField(max_length=100)
    target_id = CharField(max_length=100)
    source_system = CharField(max_length=50)
    confidence_score = FloatField(null=True)
    valid_from = DateTimeField(auto_now_add=True)
    valid_to = DateTimeField(null=True, blank=True)
    
    class Meta:
        abstract = True 