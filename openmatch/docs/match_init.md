# Match Module Public Interface

The match module provides a comprehensive set of classes and utilities for entity matching and record linkage. This document outlines the public interface exposed by the module.

## Exported Components

### 1. Core Classes

```python
from openmatch.match import (
    MatchEngine,    # Main matching engine
    MatchRules,     # Implementation of matching rules
)
```

### 2. Configuration Classes

```python
from openmatch.match import (
    MatchConfig,        # Main matching configuration
    FieldMatchConfig,   # Field-level matching configuration
    BlockingConfig,     # Blocking strategy configuration
    CustomMatchConfig,  # Custom matching function configuration
    SegmentConfig,      # Segmented matching configuration
    ConditionalRule,    # Conditional matching rules
    NullHandling       # Null value handling configuration
)
```

### 3. Enums

```python
from openmatch.match import (
    MatchType,           # Types of matching algorithms
    ComparisonOperator   # Operators for conditional rules
)
```

## Match Types

The `MatchType` enum defines supported matching algorithms:

```python
class MatchType(Enum):
    EXACT = "exact"           # Exact string matching
    FUZZY = "fuzzy"          # Fuzzy string matching
    PHONETIC = "phonetic"    # Phonetic matching
    NUMERIC = "numeric"      # Numeric comparison
    DATE = "date"           # Date comparison
    ADDRESS = "address"     # Address comparison
    CONDITIONAL = "conditional"  # Conditional matching
    SEGMENTED = "segmented"    # Segmented matching
    CUSTOM = "custom"        # Custom matching functions
```

## Comparison Operators

The `ComparisonOperator` enum defines operators for conditional rules:

```python
class ComparisonOperator(Enum):
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IN = "in"
    NOT_IN = "not_in"
```

## Basic Usage

### 1. Simple Record Matching

```python
from openmatch.match import (
    MatchEngine,
    MatchConfig,
    FieldMatchConfig,
    MatchType
)

# Configure matching
config = MatchConfig(
    field_configs={
        "name": FieldMatchConfig(
            match_type=MatchType.FUZZY,
            weight=1.0
        )
    }
)

# Create engine
engine = MatchEngine(config)

# Match records
score, field_scores = engine.match_records(
    {"name": "John Smith"},
    {"name": "Jon Smith"}
)
```

### 2. Batch Record Matching

```python
# Find matches in collections
matches = engine.find_matches(
    records=[
        {"name": "John Smith", "id": "1"},
        {"name": "Jane Doe", "id": "2"}
    ],
    comparison_records=[
        {"name": "Jon Smith", "id": "A"},
        {"name": "Jane Doe", "id": "B"}
    ]
)
```

### 3. DataFrame Matching

```python
# Match pandas DataFrames
results_df = engine.match_dataframe(
    df1,
    df2,
    id_field="record_id"
)
```

## Configuration Examples

### 1. Field Configuration

```python
field_config = FieldMatchConfig(
    match_type=MatchType.FUZZY,
    weight=1.0,
    threshold=0.8,
    fuzzy_params={
        "method": "levenshtein",
        "threshold": 0.8
    },
    preprocessors=["lower", "strip"]
)
```

### 2. Blocking Configuration

```python
blocking_config = BlockingConfig(
    blocking_keys=["zip_code"],
    method="sorted_neighborhood",
    parameters={"window_size": 3}
)
```

### 3. Null Handling

```python
null_config = NullHandling(
    match_nulls=False,
    null_equality_score=0.0,
    require_both_non_null=True,
    null_field_score=0.0
)
```

## Best Practices

1. **Configuration**
   - Define field configurations based on data types
   - Set appropriate weights and thresholds
   - Configure blocking for large datasets

2. **Performance**
   - Use blocking for large datasets
   - Enable caching for repeated comparisons
   - Configure parallel processing appropriately

3. **Error Handling**
   - Validate configurations before use
   - Handle null values appropriately
   - Monitor matching performance

4. **Extensibility**
   - Implement custom match types as needed
   - Create domain-specific preprocessors
   - Define custom comparison rules

## Module Dependencies

The match module depends on the following Python packages:

- **jellyfish**: For string similarity metrics
- **editdistance**: For Levenshtein distance calculation
- **usaddress**: For address parsing
- **phonenumbers**: For phone number formatting
- **python-dateutil**: For date parsing
- **pandas**: For DataFrame operations (optional)

## Version Compatibility

The match module is compatible with:

- Python 3.7+
- pandas 1.0+ (for DataFrame operations)
- numpy 1.17+ 