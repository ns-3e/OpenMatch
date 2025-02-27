# Match Rules Documentation

The `MatchRules` class implements the core matching logic for comparing field values using various matching strategies. This document details the available matching rules and their configurations.

## Core Matching Rules

### 1. Exact Match
Performs case-sensitive or case-insensitive exact string comparison.

```python
from openmatch.match import MatchRules

# Case-insensitive match
score = MatchRules.exact_match("John", "JOHN", case_sensitive=False)  # Returns 1.0

# Case-sensitive match
score = MatchRules.exact_match("John", "JOHN", case_sensitive=True)   # Returns 0.0
```

### 2. Fuzzy Match
Implements various fuzzy string matching algorithms with configurable thresholds.

```python
# Available methods: levenshtein, jaro, jaro_winkler
score = MatchRules.fuzzy_match(
    "John Smith",
    "Jon Smyth",
    method="levenshtein",
    threshold=0.8
)
```

#### Supported Algorithms:
- **Levenshtein**: Edit distance-based similarity
- **Jaro**: Basic Jaro similarity
- **Jaro-Winkler**: Modified Jaro with prefix emphasis

### 3. Phonetic Match
Matches strings based on their phonetic representation.

```python
# Available algorithms: soundex, metaphone, nysiis
score = MatchRules.phonetic_match(
    "Smith",
    "Smythe",
    algorithm="soundex"
)
```

#### Supported Algorithms:
- **Soundex**: Basic phonetic encoding
- **Metaphone**: More accurate English phonetic algorithm
- **NYSIIS**: New York State Identification Intelligence System

### 4. Numeric Match
Compares numeric values with optional tolerance.

```python
score = MatchRules.numeric_match(
    100.0,
    100.5,
    tolerance=1.0  # Allows difference up to 1.0
)
```

### 5. Date Match
Compares dates with optional format specification.

```python
# With specific format
score = MatchRules.date_match(
    "2024-02-25",
    "2024-02-25",
    format="%Y-%m-%d"
)

# Automatic format detection
score = MatchRules.date_match(
    "Feb 25, 2024",
    "2024-02-25"
)
```

### 6. Address Match
Component-wise address comparison with weighted scoring.

```python
score = MatchRules.address_match(
    "123 Main St, New York, NY 10001",
    "123 Main Street, New York, NY 10001"
)
```

#### Component Weights:
- Address Number: 25%
- Street Name: 35%
- Street Type: 15%
- City: 15%
- State: 5%
- ZIP Code: 5%

## Advanced Features

### 1. Null Handling
Configurable strategies for handling null values in comparisons.

```python
from openmatch.match import NullHandling

null_config = NullHandling(
    match_nulls=False,              # Whether nulls match each other
    null_equality_score=0.0,        # Score when both values are null
    require_both_non_null=True,     # Require both values to be non-null
    null_field_score=0.0            # Score when one value is null
)
```

### 2. Conditional Matching
Apply different matching rules based on field values.

```python
from openmatch.match import ConditionalRule, ComparisonOperator

rule = ConditionalRule(
    condition_field="type",
    operator=ComparisonOperator.EQUALS,
    value="business",
    match_config=business_match_config
)
```

#### Supported Operators:
- EQUALS
- NOT_EQUALS
- GREATER_THAN
- LESS_THAN
- CONTAINS
- STARTS_WITH
- ENDS_WITH
- REGEX
- IN
- NOT_IN

### 3. Segmented Matching
Apply different matching strategies based on data segments.

```python
from openmatch.match import SegmentConfig

segment_config = SegmentConfig(
    segment_field="customer_type",
    segment_values={
        "retail": 1.0,
        "business": 1.5,
        "enterprise": 2.0
    },
    default_weight=1.0
)
```

### 4. Value Preprocessing
Built-in and custom preprocessing functions for field values.

```python
field_config = FieldMatchConfig(
    match_type=MatchType.FUZZY,
    preprocessors=[
        "lower",           # Convert to lowercase
        "strip",           # Remove whitespace
        "normalize_phone", # Standardize phone numbers
        custom_cleaner    # Custom function
    ]
)
```

#### Built-in Preprocessors:
- **lower**: Convert to lowercase
- **strip**: Remove leading/trailing whitespace
- **normalize_phone**: Standardize phone numbers to E.164 format

## Best Practices

### 1. Choosing Match Types
- Use **exact match** for unique identifiers
- Use **fuzzy match** for names and descriptions
- Use **phonetic match** for names with varied spellings
- Use **numeric match** for quantities and measurements
- Use **date match** for temporal comparisons
- Use **address match** for location data

### 2. Configuring Thresholds
- Start with conservative thresholds (0.8-0.9)
- Adjust based on data quality and requirements
- Consider field-specific threshold tuning
- Monitor false positive/negative rates

### 3. Preprocessing
- Always normalize case for string comparisons
- Remove irrelevant characters and whitespace
- Standardize formats (dates, phones, etc.)
- Consider domain-specific cleaning

### 4. Performance Optimization
- Use appropriate match types for data
- Implement preprocessing efficiently
- Cache preprocessed values when possible
- Monitor matching performance

## Error Handling

The matching rules include robust error handling:

1. **Type Validation**
   - Checks for appropriate value types
   - Converts types when possible
   - Returns 0.0 score on type mismatch

2. **Null Handling**
   - Configurable null value treatment
   - Explicit null comparison rules
   - Default scores for null cases

3. **Format Validation**
   - Date format verification
   - Phone number validation
   - Address parsing validation

## Extending Match Rules

### 1. Custom Match Types
```python
class CustomMatchRule:
    @staticmethod
    def match(value1: Any, value2: Any, **params) -> float:
        # Custom matching logic
        return score
```

### 2. Custom Preprocessors
```python
def custom_preprocessor(value: Any) -> Any:
    # Custom preprocessing logic
    return processed_value
```

### 3. Custom Comparison Rules
```python
class CustomComparisonRule:
    def compare(self, record1: Dict, record2: Dict) -> float:
        # Custom comparison logic
        return score
``` 