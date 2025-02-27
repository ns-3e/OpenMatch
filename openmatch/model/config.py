"""
Configuration classes for data model management.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union, ClassVar
from enum import Enum
from datetime import datetime
import os
import yaml
from pathlib import Path


class DataType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    JSON = "json"
    ARRAY = "array"


class RelationType(Enum):
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_MANY = "many_to_many"
    # TODO: Add the ablility to add custom relation types
    # E.g., "ONE_TO_N" (one to many constrained by n)
        # Ex. A Record can have up to 3 phone numbers
        
        
@dataclass
class FieldConfig:
    """Configuration for an entity field."""
    name: str
    data_type: DataType
    description: Optional[str] = None
    required: bool = False
    unique: bool = False
    primary_key: bool = False
    foreign_key: Optional[str] = None  # Format: "entity.field"
    default_value: Optional[Any] = None
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RelationshipConfig:
    """Configuration for entity relationships."""
    name: str
    source_entity: str
    target_entity: str
    relation_type: RelationType
    source_field: str
    target_field: str
    cascade_delete: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EntityConfig:
    """Configuration for a business entity."""
    name: str
    description: Optional[str] = None
    fields: List[FieldConfig] = field(default_factory=list)
    relationships: List[RelationshipConfig] = field(default_factory=list)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_field(self, field: FieldConfig) -> None:
        """Add a field to the entity."""
        if any(f.name == field.name for f in self.fields):
            raise ValueError(f"Field {field.name} already exists")
        self.fields.append(field)

    def add_relationship(self, relationship: RelationshipConfig) -> None:
        """Add a relationship to the entity."""
        if any(r.name == relationship.name for r in self.relationships):
            raise ValueError(f"Relationship {relationship.name} already exists")
        self.relationships.append(relationship)


@dataclass
class SourceSystemConfig:
    """Configuration for a source system."""
    name: str
    type: str  # e.g., "database", "api", "file"
    connection_details: Dict[str, Any]
    schema_discovery: Dict[str, Any] = field(default_factory=dict)
    field_mappings: Dict[str, Dict[str, str]] = field(default_factory=dict)
    transformation_rules: Dict[str, Any] = field(default_factory=dict)
    load_config: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PhysicalModelConfig:
    """Configuration for physical data model."""
    table_prefix: str = "mdm_"
    schema_name: Optional[str] = None
    partition_strategy: Optional[Dict[str, Any]] = None
    storage_options: Dict[str, Any] = field(default_factory=dict)
    index_options: Dict[str, Any] = field(default_factory=dict)
    history_table_suffix: str = "_history"
    xref_table_suffix: str = "_xref"


@dataclass
class DataModelConfig:
    """Main configuration for data model management."""
    entities: Dict[str, EntityConfig]
    source_systems: Dict[str, SourceSystemConfig]
    physical_model: PhysicalModelConfig
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    DEFAULT_CONFIG_PATHS: ClassVar[List[str]] = [
        'openmatch_model.yaml',
        'config/openmatch_model.yaml',
        os.path.expanduser('~/.openmatch/model.yaml'),
        '/etc/openmatch/model.yaml'
    ]

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> 'DataModelConfig':
        """Load configuration from a YAML file.
        
        Args:
            config_path: Optional path to configuration file. If not provided,
                        will search in default locations.
                        
        Returns:
            DataModelConfig instance
            
        Raises:
            FileNotFoundError: If no configuration file is found
            ValueError: If configuration is invalid
        """
        # Find configuration file
        if config_path:
            if not os.path.exists(config_path):
                raise FileNotFoundError(
                    f"Configuration file not found: {config_path}"
                )
            paths = [config_path]
        else:
            paths = cls.DEFAULT_CONFIG_PATHS
            
        config_file = None
        for path in paths:
            if os.path.exists(path):
                config_file = path
                break
                
        if not config_file:
            raise FileNotFoundError(
                "No configuration file found. Please create one at one of: "
                f"{', '.join(paths)}"
            )
            
        # Load and parse configuration
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
            
        # Convert configuration data to objects
        entities = {
            name: EntityConfig(
                name=name,
                description=entity.get('description'),
                fields=[
                    FieldConfig(
                        name=field['name'],
                        data_type=DataType(field['data_type']),
                        description=field.get('description'),
                        required=field.get('required', False),
                        unique=field.get('unique', False),
                        primary_key=field.get('primary_key', False),
                        foreign_key=field.get('foreign_key'),
                        default_value=field.get('default_value'),
                        validation_rules=field.get('validation_rules', {}),
                        metadata=field.get('metadata', {})
                    )
                    for field in entity.get('fields', [])
                ],
                relationships=[
                    RelationshipConfig(
                        name=rel['name'],
                        source_entity=rel['source_entity'],
                        target_entity=rel['target_entity'],
                        relation_type=RelationType(rel['relation_type']),
                        source_field=rel['source_field'],
                        target_field=rel['target_field'],
                        cascade_delete=rel.get('cascade_delete', False),
                        metadata=rel.get('metadata', {})
                    )
                    for rel in entity.get('relationships', [])
                ],
                indexes=entity.get('indexes', []),
                metadata=entity.get('metadata', {})
            )
            for name, entity in config_data.get('entities', {}).items()
        }
        
        source_systems = {
            name: SourceSystemConfig(
                name=name,
                type=source['type'],
                connection_details=source['connection_details'],
                schema_discovery=source.get('schema_discovery', {}),
                field_mappings=source.get('field_mappings', {}),
                transformation_rules=source.get('transformation_rules', {}),
                load_config=source.get('load_config', {}),
                metadata=source.get('metadata', {})
            )
            for name, source in config_data.get('source_systems', {}).items()
        }
        
        physical_model = PhysicalModelConfig(
            table_prefix=config_data.get('physical_model', {}).get('table_prefix', 'mdm_'),
            schema_name=config_data.get('physical_model', {}).get('schema_name'),
            partition_strategy=config_data.get('physical_model', {}).get('partition_strategy'),
            storage_options=config_data.get('physical_model', {}).get('storage_options', {}),
            index_options=config_data.get('physical_model', {}).get('index_options', {}),
            history_table_suffix=config_data.get('physical_model', {}).get('history_table_suffix', '_history'),
            xref_table_suffix=config_data.get('physical_model', {}).get('xref_table_suffix', '_xref')
        )
        
        return cls(
            entities=entities,
            source_systems=source_systems,
            physical_model=physical_model,
            metadata=config_data.get('metadata', {})
        )

    def validate(self) -> List[str]:
        """Validate the data model configuration."""
        errors = []

        # Validate entities
        for entity_name, entity in self.entities.items():
            # Check for primary key
            if not any(f.primary_key for f in entity.fields):
                errors.append(f"Entity {entity_name} must have a primary key")

            # Validate foreign keys
            for field in entity.fields:
                if field.foreign_key:
                    ref_entity, ref_field = field.foreign_key.split(".")
                    if ref_entity not in self.entities:
                        errors.append(
                            f"Entity {entity_name} references unknown entity {ref_entity}"
                        )
                    elif not any(
                        f.name == ref_field for f in self.entities[ref_entity].fields
                    ):
                        errors.append(
                            f"Entity {entity_name} references unknown field {ref_field}"
                        )

            # Validate relationships
            for rel in entity.relationships:
                if rel.target_entity not in self.entities:
                    errors.append(
                        f"Entity {entity_name} has relationship to unknown entity {rel.target_entity}"
                    )

        return errors

    def to_physical_model(self) -> Dict[str, Any]:
        """Generate physical model configuration for all entities."""
        physical_model = {}
        prefix = self.physical_model.table_prefix
        schema = self.physical_model.schema_name

        for entity_name, entity in self.entities.items():
            # Master table
            master_table = {
                "name": f"{prefix}{entity_name}",
                "schema": schema,
                "columns": [
                    {
                        "name": f.name,
                        "type": f.data_type.value,
                        "nullable": not f.required,
                        "unique": f.unique,
                        "primary_key": f.primary_key,
                        "foreign_key": f.foreign_key
                    }
                    for f in entity.fields
                ],
                "indexes": entity.indexes,
                "partition_key": self.physical_model.partition_strategy.get(entity_name) if self.physical_model.partition_strategy else None
            }

            # History table
            history_table = {
                "name": f"{prefix}{entity_name}{self.physical_model.history_table_suffix}",
                "schema": schema,
                "columns": master_table["columns"] + [
                    {"name": "valid_from", "type": "datetime", "nullable": False},
                    {"name": "valid_to", "type": "datetime", "nullable": True},
                    {"name": "change_type", "type": "string", "nullable": False},
                    {"name": "change_user", "type": "string", "nullable": False}
                ]
            }

            # Cross-reference table
            xref_table = {
                "name": f"{prefix}{entity_name}{self.physical_model.xref_table_suffix}",
                "schema": schema,
                "columns": [
                    {"name": "source_id", "type": "string", "nullable": False},
                    {"name": "target_id", "type": "string", "nullable": False},
                    {"name": "source_system", "type": "string", "nullable": False},
                    {"name": "confidence_score", "type": "float", "nullable": True},
                    {"name": "valid_from", "type": "datetime", "nullable": False},
                    {"name": "valid_to", "type": "datetime", "nullable": True}
                ]
            }

            physical_model[entity_name] = {
                "master": master_table,
                "history": history_table,
                "xref": xref_table
            }

        return physical_model 