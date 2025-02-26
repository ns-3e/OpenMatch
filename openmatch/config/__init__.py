"""
OpenMatch Configuration Module - Defines configuration classes for MDM operations.
"""

from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

class MatchType(str, Enum):
    """Types of matching operations."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    PHONETIC = "phonetic"
    NUMERIC = "numeric"
    DATE = "date"
    ADDRESS = "address"

class ComparisonOperator(str, Enum):
    """Comparison operators for conditional rules."""
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"

class NullHandling(str, Enum):
    """Strategies for handling null values."""
    IGNORE = "ignore"
    MATCH = "match"
    NO_MATCH = "no_match"

@dataclass
class FieldMatchConfig:
    """Configuration for field matching."""
    match_type: MatchType
    threshold: float = 0.8
    case_sensitive: bool = False
    null_handling: NullHandling = NullHandling.IGNORE

@dataclass
class ConditionalRule:
    """Rule for conditional matching."""
    field: str
    operator: ComparisonOperator
    value: Any

@dataclass
class SegmentConfig:
    """Configuration for data segmentation."""
    name: str
    conditions: List[ConditionalRule]

@dataclass
class ValidationRules:
    """Rules for data validation."""
    field_rules: Dict[str, Dict[str, Any]]

@dataclass
class PhysicalModelConfig:
    """Configuration for physical data model."""
    schema_name: str
    table_prefix: str
    master_table_settings: Dict[str, Any]
    history_table_settings: Dict[str, Any]
    xref_table_settings: Dict[str, Any]

@dataclass
class DataModelConfig:
    """Configuration for data model."""
    entity_type: str
    required_fields: List[str]
    field_types: Dict[str, str]
    complex_fields: Optional[Dict[str, Dict[str, str]]] = None
    validation_rules: Optional[ValidationRules] = None
    physical_model: Optional[PhysicalModelConfig] = None
    
    def set_validation_rules(self, rules: ValidationRules) -> None:
        """Set validation rules for the data model.
        
        Args:
            rules: Validation rules configuration
        """
        self.validation_rules = rules
    
    def set_physical_model_config(self, config: PhysicalModelConfig) -> None:
        """Set physical model configuration.
        
        Args:
            config: Physical model configuration
        """
        self.physical_model = config
    
    def validate(self) -> List[str]:
        """Validate the data model configuration.
        
        Returns:
            List of validation error messages, empty if valid
        """
        errors = []
        
        # Check required fields
        if not self.entity_type:
            errors.append("Entity type is required")
        
        if not self.required_fields:
            errors.append("At least one required field must be specified")
        
        if not self.field_types:
            errors.append("Field types must be specified")
        
        # Check that all required fields have types
        missing_types = set(self.required_fields) - set(self.field_types.keys())
        if missing_types:
            errors.append(f"Missing field types for required fields: {', '.join(missing_types)}")
        
        # Check complex fields
        if self.complex_fields:
            for field_name, field_def in self.complex_fields.items():
                if not isinstance(field_def, dict):
                    errors.append(f"Complex field {field_name} must be a dictionary")
                elif not field_def:
                    errors.append(f"Complex field {field_name} must have at least one subfield")
        
        # Check validation rules
        if self.validation_rules:
            for field, rules in self.validation_rules.field_rules.items():
                if field not in self.field_types and field not in (self.complex_fields or {}):
                    errors.append(f"Validation rules specified for unknown field: {field}")
        
        # Check physical model
        if self.physical_model:
            if not self.physical_model.schema_name:
                errors.append("Physical model schema name is required")
            if not self.physical_model.table_prefix:
                errors.append("Physical model table prefix is required")
        
        return errors
    
    def to_physical_model(self) -> Dict[str, Dict[str, Any]]:
        """Convert data model config to physical model representation.
        
        Returns:
            Dict containing table configurations for master, history and xref tables
        """
        # Check if physical model config exists
        if not self.physical_model:
            raise ValueError("Physical model configuration is required")
            
        # Construct base table name
        base_name = f"{self.physical_model.table_prefix}_{self.entity_type}"
        
        # Define master table
        master_columns = []
        for field_name, field_type in self.field_types.items():
            column = {
                "name": field_name,
                "type": field_type,
                "nullable": field_name not in self.required_fields
            }
            master_columns.append(column)
            
        # Add complex fields
        for field_name, subfields in self.complex_fields.items():
            for subfield_name, subfield_type in subfields.items():
                column = {
                    "name": f"{field_name}_{subfield_name}",
                    "type": subfield_type,
                    "nullable": True
                }
                master_columns.append(column)
                
        # Add audit fields if enabled
        if self.physical_model.master_table_settings.get("include_audit_fields"):
            # Check for existing audit fields
            existing_fields = {col["name"] for col in master_columns}
            audit_columns = []
            
            if "created_at" not in existing_fields:
                audit_columns.append({"name": "created_at", "type": "timestamp", "nullable": False})
            if "created_by" not in existing_fields:
                audit_columns.append({"name": "created_by", "type": "string", "nullable": False})
            if "updated_at" not in existing_fields:
                audit_columns.append({"name": "updated_at", "type": "timestamp", "nullable": True})
            if "updated_by" not in existing_fields:
                audit_columns.append({"name": "updated_by", "type": "string", "nullable": True})
                
            master_columns.extend(audit_columns)
            
        master_table = {
            "name": base_name,
            "columns": master_columns,
            "indexes": [
                {"name": "pk", "columns": ["id"], "unique": True},
                {"name": "source", "columns": ["source"]}
            ]
        }
        
        # Define history table if enabled
        history_table = None
        if self.physical_model.history_table_settings.get("track_changes"):
            history_columns = master_columns.copy()
            history_columns.extend([
                {"name": "valid_from", "type": "timestamp", "nullable": False},
                {"name": "valid_to", "type": "timestamp", "nullable": True},
                {"name": "change_type", "type": "string", "nullable": False}
            ])
            
            history_table = {
                "name": f"{base_name}_history",
                "columns": history_columns,
                "indexes": [
                    {"name": "id_valid", "columns": ["id", "valid_from"]}
                ]
            }
            
        # Define cross-reference table if enabled
        xref_table = None
        if self.physical_model.xref_table_settings:
            xref_columns = [
                {"name": "source_id", "type": "string", "nullable": False},
                {"name": "target_id", "type": "string", "nullable": False},
                {"name": "match_type", "type": "string", "nullable": False},
                {"name": "match_score", "type": "float", "nullable": False},
                {"name": "matched_at", "type": "timestamp", "nullable": False},
                {"name": "matched_by", "type": "string", "nullable": False}
            ]
            
            if self.physical_model.xref_table_settings.get("include_match_details"):
                xref_columns.extend([
                    {"name": "match_details", "type": "json", "nullable": True}
                ])
                
            if self.physical_model.xref_table_settings.get("include_confidence_scores"):
                xref_columns.extend([
                    {"name": "confidence_scores", "type": "json", "nullable": True}
                ])
                
            xref_table = {
                "name": f"{base_name}_xref",
                "columns": xref_columns,
                "indexes": [
                    {"name": "source_target", "columns": ["source_id", "target_id"], "unique": True}
                ]
            }
            
        return {
            self.entity_type: {
                "master": master_table,
                "history": history_table,
                "xref": xref_table
            }
        }

@dataclass
class SurvivorshipRules:
    """Rules for data survivorship."""
    priority_fields: Dict[str, List[str]]
    merge_rules: Dict[str, str]

@dataclass
class TrustConfig:
    """Configuration for trust scoring."""
    source_reliability: Dict[str, float]
    field_weights: Dict[str, float]
    completeness_weight: float = 0.3
    accuracy_weight: float = 0.4
    timeliness_weight: float = 0.3
    min_trust_score: float = 0.0
    max_trust_score: float = 1.0
    default_source_weight: float = 0.5
    default_field_weight: float = 0.5 