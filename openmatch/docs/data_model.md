# Data Model Management

The OpenMatch Data Model Management module provides comprehensive capabilities for defining, managing, and evolving your MDM data model. This includes entity configuration, relationship management, source system integration, and physical model management.

## Table of Contents
1. [Core Concepts](#core-concepts)
2. [Entity Configuration](#entity-configuration)
3. [Relationships](#relationships)
4. [Source System Integration](#source-system-integration)
5. [Physical Model Management](#physical-model-management)
6. [Examples](#examples)

## Core Concepts

### Data Types

OpenMatch supports the following data types:
```python
DataType.STRING    # Text data
DataType.INTEGER   # Whole numbers
DataType.FLOAT     # Decimal numbers
DataType.BOOLEAN   # True/False values
DataType.DATE      # Date values
DataType.DATETIME  # Date and time values
DataType.JSON      # JSON/Document data
DataType.ARRAY     # Array/List data
```

### Relationship Types

Available relationship types between entities:
```python
RelationType.ONE_TO_ONE    # One-to-one relationship
RelationType.ONE_TO_MANY   # One-to-many relationship
RelationType.MANY_TO_MANY  # Many-to-many relationship
```

## Entity Configuration

Entities are the core building blocks of your MDM data model. Each entity represents a business object (e.g., Customer, Product, Location).

### Field Configuration

Fields define the attributes of an entity:

```python
from openmatch.model import FieldConfig, DataType

# Define a field
field = FieldConfig(
    name="email",
    data_type=DataType.STRING,
    description="Primary email address",
    required=True,
    unique=True,
    validation_rules={
        "format": {
            "type": "regex",
            "pattern": r"^[^@]+@[^@]+\.[^@]+$"
        }
    }
)
```

Field properties:
- `name`: Field identifier
- `data_type`: Type of data (from DataType enum)
- `description`: Optional field description
- `required`: Whether the field is mandatory
- `unique`: Whether values must be unique
- `primary_key`: Whether field is primary key
- `foreign_key`: Reference to another entity's field
- `default_value`: Default value if none provided
- `validation_rules`: Custom validation rules
- `metadata`: Additional metadata

### Entity Definition

Entities combine fields and relationships:

```python
from openmatch.model import EntityConfig

# Define an entity
customer = EntityConfig(
    name="customer",
    description="Customer master data",
    fields=[
        FieldConfig(
            name="customer_id",
            data_type=DataType.STRING,
            required=True,
            primary_key=True
        ),
        FieldConfig(
            name="name",
            data_type=DataType.STRING,
            required=True
        )
    ],
    indexes=[
        {
            "name": "name_idx",
            "columns": ["name"],
            "unique": False
        }
    ]
)
```

## Relationships

Relationships define how entities are connected:

```python
from openmatch.model import RelationshipConfig, RelationType

# Define a relationship
relationship = RelationshipConfig(
    name="customer_address",
    source_entity="address",
    target_entity="customer",
    relation_type=RelationType.MANY_TO_ONE,
    source_field="customer_id",
    target_field="customer_id",
    cascade_delete=True
)
```

Relationship properties:
- `name`: Relationship identifier
- `source_entity`: Entity where relationship starts
- `target_entity`: Entity where relationship ends
- `relation_type`: Type of relationship
- `source_field`: Field in source entity
- `target_field`: Field in target entity
- `cascade_delete`: Whether to cascade deletes
- `metadata`: Additional metadata

## Source System Integration

Configure source systems and field mappings:

```python
from openmatch.model import SourceSystemConfig

# Configure source system
crm_config = SourceSystemConfig(
    name="CRM",
    type="database",
    connection_details={
        "connection_string": "postgresql://user:pass@localhost:5432/crm"
    },
    field_mappings={
        "customer": {
            "customer_id": "id",
            "name": "full_name",
            "email": "email_address"
        }
    },
    transformation_rules={
        "name": "upper",
        "email": "lower"
    }
)
```

### Schema Discovery

Automatically discover source system schemas:

```python
# Initialize manager
manager = DataModelManager(config, engine)

# Discover schema
schema = manager.discover_source_schema("CRM", "customers")
print(schema)
```

### Field Mapping

Map source fields to MDM model:

```python
# Source data
source_data = {
    "id": "C123",
    "full_name": "Acme Corp",
    "email_address": "contact@acme.com"
}

# Apply mappings
mapped_data = manager.apply_field_mappings("CRM", "customer", source_data)
```

## Physical Model Management

Configure and manage physical storage:

```python
from openmatch.model import PhysicalModelConfig

# Configure physical model
physical_config = PhysicalModelConfig(
    table_prefix="mdm_",
    schema_name="master_data",
    partition_strategy={
        "customer": {
            "column": "created_at",
            "interval": "1 month"
        }
    },
    storage_options={
        "tablespace": "mdm_space"
    }
)
```

### Table Creation

The system automatically creates:
1. Master tables (current state)
2. History tables (temporal tracking)
3. Cross-reference tables (entity resolution)

```python
# Create all tables
manager.create_physical_model()
```

Generated tables include:
- `mdm_customer` (master)
- `mdm_customer_history`
- `mdm_customer_xref`

## Examples

### Complete Model Configuration

```python
from openmatch.model import DataModelConfig

# Create configuration
config = DataModelConfig(
    entities={
        "customer": customer_entity,
        "address": address_entity
    },
    source_systems={
        "CRM": crm_config,
        "ERP": erp_config
    },
    physical_model=physical_config
)

# Initialize manager
manager = DataModelManager(config, engine)

# Create physical model
manager.create_physical_model()
```

### Data Validation

Validate data against model:

```python
# Data to validate
data = {
    "customer_id": "C123",
    "name": "Acme Corp",
    "email": "invalid-email"
}

# Validate
errors = manager.validate_entity_data("customer", data)
if errors:
    print("Validation errors:", errors)
```

## Best Practices

1. **Entity Design**
   - Use meaningful names
   - Define clear relationships
   - Include proper constraints
   - Document with descriptions

2. **Field Configuration**
   - Set appropriate data types
   - Define validation rules
   - Consider indexing needs
   - Use meaningful defaults

3. **Source Integration**
   - Map fields explicitly
   - Include transformations
   - Validate mapped data
   - Monitor performance

4. **Physical Model**
   - Choose proper partitioning
   - Optimize indexes
   - Monitor table sizes
   - Plan for scaling

## Performance Considerations

1. **Schema Design**
   - Proper indexing strategy
   - Efficient partitioning
   - Normalized relationships
   - Storage optimization

2. **Data Loading**
   - Batch processing
   - Parallel loading
   - Transaction management
   - Error handling

3. **Query Optimization**
   - Index usage
   - Join optimization
   - Partition pruning
   - Cache utilization 