# OpenMatch Quickstart Guide

This guide will help you get started with OpenMatch, an enterprise-grade Master Data Management (MDM) library.

## Installation

```bash
# Basic installation
pip install openmatch

# Full installation with all features
pip install openmatch[all]
```

## Basic Usage

### 1. Initialize the MDM Pipeline

First, create an MDM pipeline with basic configurations for matching and survivorship:

```python
from openmatch import MDMPipeline
from openmatch.config import TrustConfig, SurvivorshipRules

pipeline = MDMPipeline(
    trust_config=TrustConfig(
        source_reliability={
            "CRM": 0.9,
            "ERP": 0.8
        }
    ),
    survivorship_rules=SurvivorshipRules(
        priority_fields={
            "name": ["CRM", "ERP"],
            "email": ["CRM", "ERP"]
        }
    )
)
```

### 2. Process Records

Process records from different sources to identify matches and create golden records:

```python
records = [
    {
        "id": "CRM_001",
        "source": "CRM",
        "name": "Acme Corporation",
        "email": "contact@acme.com",
        "phone": "555-0101"
    },
    {
        "id": "ERP_101",
        "source": "ERP",
        "name": "ACME Corp.",
        "email": "contact@acme.com",
        "phone": "5550101"
    }
]

result = pipeline.process_records(records)
```

### 3. Advanced Matching Configuration

Configure detailed matching rules for better accuracy:

```python
from openmatch.match import MatchConfig, MatchEngine

match_config = MatchConfig(
    blocking_keys=["postal_code"],
    comparison_fields=[
        ("name", "fuzzy", 0.8),
        ("email", "exact", 1.0),
        ("phone", "phonetic", 0.7)
    ],
    min_overall_score=0.85
)

pipeline.set_match_config(match_config)
```

### 4. Working with Trust Scores

Implement trust scoring for your data sources:

```python
from openmatch.trust import TrustFramework

trust_rules = {
    "completeness_weight": 0.3,
    "timeliness_weight": 0.4,
    "source_weight": 0.3
}

trust_engine = TrustFramework(trust_rules)
pipeline.set_trust_framework(trust_engine)
```

### 5. Tracking Data Lineage

Monitor the history and origin of your master data:

```python
from openmatch.lineage import LineageTracker

lineage = LineageTracker()
lineage.track_merge(source_records, golden_record)
history = lineage.get_record_history("GOLDEN_001")
```

## Common Use Cases

### 1. Customer Data Integration

```python
# Configure customer-specific matching rules
customer_match_config = MatchConfig(
    blocking_keys=["postal_code", "last_name_prefix"],
    comparison_fields=[
        ("email", "exact", 1.0),
        ("phone", "phonetic", 0.8),
        ("name", "fuzzy", 0.7),
        ("address", "address_similarity", 0.6)
    ],
    min_overall_score=0.8
)

# Process customer records
customer_pipeline = MDMPipeline(match_config=customer_match_config)
golden_customers = customer_pipeline.process_records(customer_records)
```

### 2. Product Data Management

```python
# Configure product-specific matching rules
product_match_config = MatchConfig(
    blocking_keys=["category", "brand"],
    comparison_fields=[
        ("sku", "exact", 1.0),
        ("name", "fuzzy", 0.8),
        ("description", "token_sort_ratio", 0.6)
    ],
    min_overall_score=0.9
)

# Process product records
product_pipeline = MDMPipeline(match_config=product_match_config)
golden_products = product_pipeline.process_records(product_records)
```

## Best Practices

1. **Blocking Strategy**: Choose blocking keys that balance performance and accuracy
2. **Match Thresholds**: Start conservative (high thresholds) and adjust based on results
3. **Trust Rules**: Configure source reliability based on historical data quality
4. **Survivorship**: Define clear rules for selecting winning attributes
5. **Monitoring**: Regularly review match results and adjust configurations

## Next Steps

- Explore advanced features in the [full documentation](docs/README.md)
- Check out example implementations in the [examples](examples/) directory
- Join our [community forum](https://github.com/ns-3e/OpenMatch/discussions)
- Report issues on [GitHub](https://github.com/ns-3e/OpenMatch/issues)

For more detailed information, refer to our [API Documentation](docs/api/README.md). 