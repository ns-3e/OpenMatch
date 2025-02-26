# Data Trust & Quality Framework

OpenMatch provides a comprehensive trust and data quality framework through its `trust` module. This framework enables sophisticated scoring, validation, and survivorship decisions based on configurable rules and metrics.

## Core Components

### 1. Trust Framework (`framework.py`)

The trust framework orchestrates the overall trust and quality assessment process.

#### Key Features:
- Record-level trust scoring
- Quality dimension evaluation
- Survivorship rule application
- Conflict resolution
- Merge auditing

#### Usage Example:
```python
from openmatch.trust import TrustFramework, TrustConfig

# Configure trust framework
config = TrustConfig(
    sources={
        "CRM": {"reliability_score": 0.9},
        "ERP": {"reliability_score": 0.8}
    },
    quality={
        "dimensions": {
            "completeness": 0.3,
            "accuracy": 0.3,
            "timeliness": 0.2,
            "consistency": 0.2
        }
    }
)

framework = TrustFramework(config)

# Process records
processed_records = framework.process_records(
    records=[record1, record2],
    source="CRM"
)

# Merge with trust scores
merged_record = framework.merge_records(
    records=[record1, record2],
    trust_scores={
        "record1": 0.9,
        "record2": 0.8
    }
)
```

### 2. Trust Scoring (`scoring.py`)

The scoring component calculates various quality and trust metrics for records.

#### Quality Dimensions:
- **Completeness**: Field population and data presence
- **Accuracy**: Data precision and source reliability
- **Consistency**: Cross-field validation rules
- **Timeliness**: Data freshness and age
- **Uniqueness**: Duplicate detection
- **Validity**: Format and value validation

#### Usage Example:
```python
from openmatch.trust import TrustScoring, TrustConfig

scoring = TrustScoring(config)

# Calculate scores for a record
scores = scoring.calculate_record_scores(
    record={
        "id": "CUST_001",
        "name": "Acme Corp",
        "created_at": "2024-02-26T00:00:00Z"
    },
    source="CRM"
)

print(f"Trust Score: {scores['trust_score']}")
print(f"Quality Score: {scores['quality_score']}")
print("Dimension Scores:", scores['dimension_scores'])
```

### 3. Trust Rules (`rules.py`)

The rules component defines and applies survivorship and validation rules.

#### Rule Types:
- Field validation rules
- Cross-field consistency rules
- Source-specific rules
- Custom validation functions
- Survivorship rules

#### Usage Example:
```python
from openmatch.trust import TrustRules, TrustConfig

config = TrustConfig(
    quality={
        "validity_rules": {
            "email": {
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "required": True
            },
            "phone": {
                "pattern": r"^\+?1?\d{9,15}$",
                "required": False
            }
        },
        "consistency_rules": [
            {
                "fields": ["start_date", "end_date"],
                "rule": "start_date <= end_date"
            }
        ]
    }
)

rules = TrustRules(config)

# Apply survivorship rules
golden_record = rules.apply_survivorship_rules(
    records=[record1, record2],
    trust_scores={
        "record1": 0.9,
        "record2": 0.8
    }
)
```

### 4. Trust Configuration (`config.py`)

The configuration component provides a structured way to define trust and quality settings.

#### Configuration Areas:
- Source system reliability
- Quality dimension weights
- Validation rules
- Survivorship rules
- Caching settings

#### Usage Example:
```python
from openmatch.trust import TrustConfig

config = TrustConfig(
    sources={
        "CRM": {
            "reliability_score": 0.9,
            "validation_rules": {
                "email": {"pattern": r"^[^@]+@[^@]+\.[^@]+$"},
                "phone": {"pattern": r"^\+?1?\d{9,15}$"}
            }
        }
    },
    quality={
        "dimensions": {
            "completeness": 0.3,
            "accuracy": 0.3,
            "timeliness": 0.2,
            "consistency": 0.2
        },
        "timeliness_decay": "30d",
        "completeness_rules": {
            "name": 1.0,
            "email": 0.8,
            "phone": 0.6
        }
    },
    trust_score_weights={
        "source_reliability": 0.6,
        "data_quality": 0.4
    }
)
```

## Integration Features

### 1. DataFrame Integration
- Batch record processing
- DataFrame-level operations
- Efficient scoring
- Result aggregation

```python
import pandas as pd

# Process DataFrame
df = pd.DataFrame([
    {"id": 1, "name": "Acme Corp"},
    {"id": 2, "name": "Globex Inc"}
])

processed_df = framework.process_dataframe(df, source="CRM")

# Merge multiple DataFrames
merged_df = framework.merge_dataframes(
    dfs=[df1, df2],
    sources=["CRM", "ERP"]
)
```

### 2. Caching Support
- LRU caching for scores
- Configurable cache size
- Performance optimization
- Memory management

### 3. Audit Capabilities
- Merge decision tracking
- Score history
- Rule application logging
- Source attribution

## Best Practices

1. **Source Configuration**
   - Define reliable source systems
   - Set appropriate reliability scores
   - Configure validation rules
   - Specify update tracking

2. **Quality Rules**
   - Define critical fields
   - Set reasonable thresholds
   - Balance dimension weights
   - Test rule effectiveness

3. **Performance Optimization**
   - Enable appropriate caching
   - Batch process records
   - Monitor memory usage
   - Profile rule execution

## Performance Considerations

1. **Caching Strategy**
   - Configure cache sizes
   - Monitor hit rates
   - Clear stale entries
   - Balance memory usage

2. **Batch Processing**
   - Use DataFrame operations
   - Process in chunks
   - Parallel execution
   - Memory efficiency

3. **Rule Optimization**
   - Prioritize rule order
   - Simplify complex rules
   - Cache intermediate results
   - Profile performance

## Common Use Cases

1. **Customer Data Quality**
   ```python
   config = TrustConfig(
       sources={
           "CRM": {"reliability_score": 0.9},
           "Web": {"reliability_score": 0.7}
       },
       quality={
           "completeness_rules": {
               "email": 1.0,
               "phone": 0.8,
               "address": 0.6
           }
       }
   )
   
   framework = TrustFramework(config)
   quality_scores = framework.process_records(customer_records, "CRM")
   ```

2. **Product Data Validation**
   ```python
   config = TrustConfig(
       quality={
           "validity_rules": {
               "sku": {"pattern": r"^[A-Z]{2}\d{6}$"},
               "price": {"min": 0, "max": 1000000},
               "stock": {"min": 0}
           }
       }
   )
   
   framework = TrustFramework(config)
   valid_products = framework.process_records(product_records, "ERP")
   ```

3. **Address Verification**
   ```python
   def verify_address(value):
       # Custom address verification logic
       return is_valid_address(value)
   
   config = TrustConfig(
       quality={
           "validity_rules": {
               "address": {
                   "custom_validator": verify_address
               }
           }
       }
   )
   
   framework = TrustFramework(config)
   verified_addresses = framework.process_records(address_records, "CRM")
   ``` 