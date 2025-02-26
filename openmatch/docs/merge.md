# Record Merge Processing

OpenMatch provides sophisticated record merging capabilities through its `merge` module. This module handles the complex task of combining matched records into golden records using configurable strategies and maintaining full traceability.

## Core Components

### 1. Merge Processor (`processor.py`)

The merge processor orchestrates the entire merge process, from handling matches to creating and managing golden records.

#### Key Features:
- Golden record creation and management
- Cross-reference tracking
- Merge operation details and history
- Rollback capabilities
- Cluster-based match grouping

#### Usage Example:
```python
from openmatch.merge import MergeProcessor
from openmatch.merge.strategies import DefaultMergeStrategy

processor = MergeProcessor(
    strategy=DefaultMergeStrategy(),
    id_field="id",
    source_field="source",
    timestamp_field="last_updated"
)

# Process matches
matches = [
    (record1, record2, 0.95),  # (record1, record2, similarity_score)
    (record3, record4, 0.92)
]

golden_records = processor.merge_matches(
    matches,
    trust_scores={
        "RECORD_001": {"total": 0.9},
        "RECORD_002": {"total": 0.8}
    }
)

# Access merge results
xrefs = processor.get_xrefs()
source_records = processor.get_source_records("GOLDEN_001")
merge_details = processor.get_merge_details("GOLDEN_001")

# Rollback if needed
processor.rollback_merge("GOLDEN_001")
```

### 2. Merge Strategies (`strategies.py`)

The merge strategies component provides different algorithms for combining record attributes into golden records.

#### Available Strategies:

1. **Default Merge Strategy**
   - Uses most trusted or recent values
   - Handles missing values
   - Supports trust scores
   - Timestamp-based selection

```python
from openmatch.merge.strategies import DefaultMergeStrategy

strategy = DefaultMergeStrategy(
    id_field="id",
    timestamp_field="last_updated"
)

golden_record = strategy.merge_records(
    records=[record1, record2],
    golden_id="GOLDEN_001",
    trust_scores=trust_scores
)
```

2. **Weighted Merge Strategy**
   - Field-specific weight configuration
   - Weighted value combination
   - Numeric field averaging
   - Trust score integration

```python
from openmatch.merge.strategies import WeightedMergeStrategy

strategy = WeightedMergeStrategy(
    field_weights={
        "name": 0.8,
        "address": 0.6,
        "email": 1.0
    }
)

golden_record = strategy.merge_records(
    records=[record1, record2],
    golden_id="GOLDEN_001",
    trust_scores=trust_scores
)
```

3. **Custom Merge Strategy**
   - Field-specific merge functions
   - Custom logic implementation
   - Specialized data type handling
   - Complex merge rules

```python
from openmatch.merge.strategies import CustomMergeStrategy

def merge_addresses(values, trust_scores=None):
    # Custom address merging logic
    return standardized_address

strategy = CustomMergeStrategy(
    merge_functions={
        "address": merge_addresses,
        "phone": merge_phone_numbers
    }
)
```

## Integration Features

### 1. Trust Score Integration
- Source system reliability
- Record completeness scoring
- Field-level confidence
- Temporal relevance

### 2. Cross-Reference Management
- Bidirectional mapping
- Source record tracking
- Golden record linkage
- System-specific IDs

### 3. Merge Traceability
- Full merge history
- Source record tracking
- Strategy documentation
- Rollback support

## Best Practices

1. **Strategy Selection**
   - Choose appropriate strategy for data type
   - Consider field importance
   - Balance trust vs. recency
   - Test with sample data

2. **Trust Score Configuration**
   - Define reliable sources
   - Set appropriate weights
   - Consider temporal factors
   - Validate scoring model

3. **Error Handling**
   - Handle missing values
   - Validate merged results
   - Log merge decisions
   - Enable rollbacks

## Performance Considerations

1. **Memory Management**
   - Batch processing
   - Efficient data structures
   - Resource cleanup
   - Cache management

2. **Processing Optimization**
   - Parallel processing
   - Efficient clustering
   - Indexed lookups
   - Batch operations

3. **Scalability**
   - Distributed processing
   - Incremental merging
   - Resource monitoring
   - Performance metrics

## Common Use Cases

1. **Customer Data Integration**
   ```python
   strategy = DefaultMergeStrategy()
   processor = MergeProcessor(strategy)
   
   # Merge customer records
   golden_customers = processor.merge_matches([
       (crm_record, web_record, 0.95),
       (erp_record, email_record, 0.92)
   ])
   ```

2. **Product Data Harmonization**
   ```python
   strategy = WeightedMergeStrategy(
       field_weights={
           "name": 1.0,
           "description": 0.8,
           "specifications": 0.9
       }
   )
   
   processor = MergeProcessor(strategy)
   golden_products = processor.merge_matches(product_matches)
   ```

3. **Address Standardization**
   ```python
   def standardize_address(addresses, scores):
       # Custom address standardization logic
       return best_standardized_address
   
   strategy = CustomMergeStrategy({
       "address": standardize_address
   })
   
   processor = MergeProcessor(strategy)
   golden_addresses = processor.merge_matches(address_matches)
   ``` 