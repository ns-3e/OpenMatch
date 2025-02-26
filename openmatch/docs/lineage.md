# Record Lineage & History Tracking

OpenMatch provides comprehensive record lineage and history tracking capabilities through its `lineage` module. This module helps track the complete lifecycle of records, their relationships, and changes over time.

## Core Components

### 1. Lineage Tracking (`tracker.py`)

The lineage tracker maintains a complete audit trail of record changes and relationships.

#### Key Features:
- Event-based tracking of record lifecycle
- Support for various event types:
  - `CREATE`: Record creation
  - `UPDATE`: Record modifications
  - `MERGE`: Combining multiple records
  - `SPLIT`: Splitting records
  - `DELETE`: Record deletion
  - `LINK`: Record relationships
  - `UNLINK`: Removing relationships

#### Usage Example:
```python
from openmatch.lineage import LineageTracker

tracker = LineageTracker()

# Track record creation
tracker.track_create(
    record_id="CUST_001",
    user_id="john.doe",
    source_system="CRM",
    details={"initial_data": {...}}
)

# Track record merge
tracker.track_merge(
    source_ids=["CUST_001", "CUST_002"],
    target_id="GOLDEN_001",
    user_id="jane.smith",
    confidence_score=0.95,
    details={"merge_reason": "Same customer"}
)
```

### 2. History Management (`history.py`)

The history component maintains detailed change history at both record and field levels.

#### Key Features:
- Granular field-level change tracking
- Temporal querying capabilities
- Version comparison
- Export functionality

#### Usage Example:
```python
from openmatch.lineage import RecordHistory

history = RecordHistory()

# Add history entry
history.add_entry(
    record_id="CUST_001",
    action="update",
    field_name="email",
    old_value="old@email.com",
    new_value="new@email.com",
    user_id="john.doe",
    source_system="CRM",
    details={"update_reason": "Customer request"}
)

# Get field history
email_history = history.get_field_history("CUST_001", "email")

# Compare versions
differences = history.compare_versions(
    record_id="CUST_001",
    timestamp1=datetime(2024, 1, 1),
    timestamp2=datetime(2024, 2, 1)
)
```

### 3. Cross-Reference Management (`xref.py`)

The cross-reference manager handles relationships between records across different systems and domains.

#### Key Features:
- Multiple relationship types:
  - `SAME_AS`: Identity relationship
  - `PARENT_OF`/`CHILD_OF`: Hierarchical relationships
  - `RELATED_TO`: Generic relationship
  - `DERIVED_FROM`: Lineage relationship
  - `SUPERSEDES`/`REPLACED_BY`: Versioning relationships
- Temporal validity tracking
- Confidence scoring
- Graph-based relationship navigation

#### Usage Example:
```python
from openmatch.lineage import CrossReferenceManager, RelationType

xref_mgr = CrossReferenceManager()

# Add cross-reference
xref_mgr.add_xref(
    source_id="CRM_001",
    target_id="ERP_101",
    relation_type=RelationType.SAME_AS,
    source_system="CRM",
    target_system="ERP",
    confidence_score=0.98
)

# Find related entities
related = xref_mgr.get_related_entities(
    entity_id="CRM_001",
    relation_type=RelationType.SAME_AS,
    at_time=datetime.utcnow()
)

# Get system-specific ID
erp_id = xref_mgr.get_system_id("CRM_001", "ERP")
```

## Integration Features

### 1. Graph-Based Analysis
- Built on NetworkX for efficient graph operations
- Support for relationship path finding
- Subgraph extraction and analysis
- Visualization capabilities

### 2. Temporal Analysis
- Point-in-time querying
- Temporal validity windows
- Historical state reconstruction
- Version comparison

### 3. Export Capabilities
- JSON export format
- Structured history exports
- Cross-reference exports
- Graph exports for visualization

## Best Practices

1. **Event Tracking**
   - Track all significant record changes
   - Include user attribution
   - Maintain detailed metadata
   - Use appropriate event types

2. **History Management**
   - Track field-level changes
   - Include change reasons
   - Maintain temporal consistency
   - Regular history cleanup

3. **Cross-References**
   - Use appropriate relationship types
   - Include confidence scores
   - Maintain temporal validity
   - Regular relationship validation

## Performance Considerations

1. **Storage Efficiency**
   - Efficient graph representation
   - Optimized history storage
   - Regular cleanup of old data

2. **Query Performance**
   - Indexed lookups
   - Cached frequently accessed data
   - Optimized graph traversal

3. **Scalability**
   - Distributed graph support
   - Batch processing capabilities
   - Incremental updates 