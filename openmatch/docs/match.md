# Match Module Documentation

The match module is a core component of OpenMatch that provides sophisticated entity matching capabilities. It implements intelligent record matching using various comparison strategies, blocking techniques, and configurable matching rules.

## Core Components

### 1. MatchEngine

The `MatchEngine` class is the primary interface for performing record matching operations. It supports both one-to-one and many-to-many record comparisons with configurable matching strategies.

```python
from openmatch.match import MatchEngine, MatchConfig

# Initialize engine with configuration
engine = MatchEngine(config=match_config)

# Match individual records
score, field_scores = engine.match_records(record1, record2)

# Find matches in collections
matches = engine.find_matches(records, comparison_records)

# Match pandas DataFrames
match_results = engine.match_dataframe(df1, df2, id_field="id")
```

#### Key Features:
- Configurable matching rules per field
- Blocking strategies for performance optimization
- Parallel processing support
- Result caching for improved performance
- Comprehensive match scoring

### 2. Configuration Classes

#### MatchConfig
The main configuration class that controls the overall matching behavior:

```python
config = MatchConfig(
    field_configs={
        "name": FieldMatchConfig(
            match_type=MatchType.FUZZY,
            weight=2.0,
            threshold=0.8
        ),
        "address": FieldMatchConfig(
            match_type=MatchType.ADDRESS,
            weight=1.5
        )
    },
    blocking=BlockingConfig(
        blocking_keys=["zip_code"],
        method="standard"
    ),
    min_overall_score=0.85,
    score_aggregation="weighted_average"
)
```

#### FieldMatchConfig
Configures how individual fields are matched:

- **match_type**: Type of matching algorithm (EXACT, FUZZY, PHONETIC, etc.)
- **weight**: Relative importance in overall match score
- **threshold**: Minimum score for field-level match
- **null_handling**: How to handle missing values
- **preprocessors**: Data cleaning/standardization steps

#### BlockingConfig
Controls record blocking for performance optimization:

- **blocking_keys**: Fields used for blocking
- **method**: Blocking strategy (standard, LSH, sorted_neighborhood)
- **parameters**: Method-specific parameters

### 3. Match Types

The module supports various matching strategies:

1. **Exact Match**
   - Case-insensitive exact string comparison
   - Useful for unique identifiers

2. **Fuzzy Match**
   - Levenshtein distance-based comparison
   - Configurable similarity threshold
   - Handles typos and minor variations

3. **Phonetic Match**
   - Soundex and Metaphone algorithms
   - Matches similar-sounding values
   - Useful for names and words

4. **Numeric Match**
   - Tolerance-based number comparison
   - Handles different numeric formats

5. **Date Match**
   - Format-aware date comparison
   - Handles different date representations

6. **Address Match**
   - Component-wise address comparison
   - Standardization and normalization
   - Geocoding support (optional)

7. **Conditional Match**
   - Context-dependent matching rules
   - Different strategies based on field values

8. **Segmented Match**
   - Segment-specific matching rules
   - Weighted scoring per segment

### 4. Null Handling

Configurable strategies for handling missing values:

```python
null_config = NullHandling(
    match_nulls=False,              # Whether nulls match each other
    null_equality_score=0.0,        # Score when both values are null
    require_both_non_null=True,     # Require both values to be non-null
    null_field_score=0.0            # Score when one value is null
)
```

### 5. Performance Optimization

The module includes several performance optimization features:

1. **Blocking**
   - Reduces comparison space
   - Multiple blocking strategies
   - Configurable blocking keys

2. **Caching**
   - LRU cache for field comparisons
   - Configurable cache size
   - Automatic cache management

3. **Parallel Processing**
   - Multi-threaded comparison
   - Configurable worker count
   - Automatic workload distribution

## Usage Examples

### Basic Record Matching

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
            weight=1.0,
            threshold=0.8
        )
    },
    min_overall_score=0.8
)

# Create engine
engine = MatchEngine(config)

# Match records
record1 = {"name": "John Smith"}
record2 = {"name": "Jon Smith"}

score, field_scores = engine.match_records(record1, record2)
```

### Batch Matching with Blocking

```python
from openmatch.match import BlockingConfig

# Configure blocking
config = MatchConfig(
    field_configs={...},
    blocking=BlockingConfig(
        blocking_keys=["zip_code"],
        method="sorted_neighborhood",
        parameters={"window_size": 3}
    )
)

# Find matches in collections
matches = engine.find_matches(records, comparison_records)
```

### DataFrame Matching

```python
# Match two dataframes
results_df = engine.match_dataframe(
    df1,
    df2,
    id_field="record_id"
)
```

## Best Practices

1. **Field Weights**
   - Assign higher weights to more reliable fields
   - Ensure weights sum to a meaningful total
   - Consider field completeness in weighting

2. **Thresholds**
   - Set field-level thresholds based on data quality
   - Adjust overall threshold for precision/recall balance
   - Monitor and tune thresholds based on results

3. **Blocking**
   - Choose blocking keys with good selectivity
   - Use multiple blocking passes if needed
   - Monitor block sizes for performance

4. **Performance**
   - Enable caching for repeated comparisons
   - Use parallel processing for large datasets
   - Implement appropriate blocking strategies

5. **Data Preparation**
   - Standardize field values before matching
   - Handle missing values appropriately
   - Clean and normalize data when possible

## Error Handling

The module includes comprehensive error checking:

1. **Configuration Validation**
   - Field config completeness
   - Weight and threshold ranges
   - Blocking configuration

2. **Runtime Validation**
   - Data type checking
   - Null value handling
   - Algorithm compatibility

3. **Performance Monitoring**
   - Block size warnings
   - Memory usage tracking
   - Processing time logging

## Extending the Module

The match module can be extended in several ways:

1. **Custom Match Types**
   - Implement new ComparisonRule classes
   - Add new MatchType enum values
   - Register custom comparison functions

2. **Custom Blocking Strategies**
   - Implement new blocking methods
   - Customize blocking key generation
   - Add new blocking parameters

3. **Custom Preprocessing**
   - Add new data standardization functions
   - Implement custom cleaners
   - Create domain-specific normalizers 