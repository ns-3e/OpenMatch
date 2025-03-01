# OpenMatch - Enterprise-Grade Master Data Management Library 🚀

<p align="center" style="background: white;">
<img src="openmatch/docs/assets/OpenMatch-logo.png" alt="OpenMatch Logo" width="400">
</p>

NOTE: This is a work in progress and not all functionality is available and/or stable yet.

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Contributions](https://img.shields.io/badge/Contributions-Welcome-brightgreen.svg)](#-contributing)
[![Author](https://img.shields.io/badge/Author-Nick%20Smith-red.svg)](https://github.com/ns-3e)

**OpenMatch** is an **enterprise-grade Python library** for comprehensive **Master Data Management (MDM)** solutions. It provides a complete suite of tools for entity resolution, data governance, and master data lifecycle management using cutting-edge AI and scalable architecture.

## 🎯 Core Capabilities

### 1. 🔄 Match Engine (`openmatch.match`)
- ⚡ **Advanced Matching Engine**
  - Configurable blocking strategies for performance optimization
  - Multi-attribute fuzzy matching with weighted scoring
  - Incremental matching support
  - Match result persistence and metadata tracking
  - Comprehensive match statistics and performance metrics
  - Caching support for improved performance

### 2. 🎯 Merge Processing (`openmatch.merge`)
- 🔄 **Intelligent Merge Processing**
  - Flexible merge strategy framework
  - Golden record generation and management
  - Cross-reference (xref) tracking
  - Source record lineage
  - Merge operation rollback support
  - Detailed merge audit trails

### 3. 📊 Data Model Management (`openmatch.model`)
- 🏗️ **Robust Model Framework**
  - Entity and field configuration
  - Physical model generation
  - Schema validation
  - Source system integration
  - Field mapping and transformation
  - Custom validation rules

### 4. 📜 Lineage Tracking (`openmatch.lineage`)
- 🔍 **Comprehensive Lineage**
  - Cross-reference management
  - Change history tracking
  - Source system mapping
  - Temporal data support
  - Full audit capabilities

### 5. 🔌 Enterprise Connectors (`openmatch.connectors`)
- 🌐 **Rich Connector Framework**
  - AWS integration
  - Azure support
  - Databricks connectivity
  - JDBC/ODBC support
  - REST API integration
  - Snowflake native support
  - Flat file processing

### 6. ⚙️ Management Tools (`openmatch.management`)
- 🛠️ **Administrative Capabilities**
  - Command-line interface
  - Configuration management
  - Deployment utilities
  - Monitoring tools

### 7. 🛡️ Trust Framework (`openmatch.trust`)
- ✅ **Data Quality Management**
  - Configurable trust rules
  - Scoring framework
  - Quality metrics
  - Trust-based survivorship
  - Framework configuration

## 📦 Installation

```bash
pip install openmatch
```

## 🚀 Quick Start

```python
from openmatch import MDMPipeline
from openmatch.config import TrustConfig, SurvivorshipRules

# Initialize pipeline with configurations
pipeline = MDMPipeline(
    trust_config=TrustConfig(
        source_reliability={
            "CRM": 0.9,
            "ERP": 0.8,
            "LEGACY": 0.6
        }
    ),
    survivorship_rules=SurvivorshipRules(
        priority_fields={
            "name": ["CRM", "ERP", "LEGACY"],
            "address": ["ERP", "CRM", "LEGACY"]
        }
    )
)

# Sample records from different sources
records = [
    {
        "id": "CRM_001",
        "source": "CRM",
        "name": "Acme Corp",
        "address": "123 Main St",
        "phone": "555-0101",
        "last_updated": "2024-02-25"
    },
    {
        "id": "ERP_101",
        "source": "ERP",
        "name": "ACME Corporation",
        "address": "123 Main Street",
        "phone": "555-0101",
        "last_updated": "2024-02-24"
    }
]

# Process records
result = pipeline.process_records(records)

# Access results
print("Golden Records:", result.golden_records)
print("Cross References:", result.xrefs)
print("Lineage:", result.lineage)
print("Trust Scores:", result.trust_scores)
```

## 📊 Performance & Scalability

- 🚀 Optimized blocking strategies for efficient matching
- 📈 Configurable caching for improved performance
- 🌐 Support for incremental processing
- 💾 Efficient metadata management
- ⚡ Comprehensive performance metrics

## 🗺️ Roadmap

### Immediate Term
- ✅ Core matching engine
- ✅ Basic survivorship rules
- ✅ Lineage tracking
- 🔄 REST API endpoints

### Short Term (3-6 months)
- 🔄 Machine learning-based matching
- 🔄 Advanced survivorship rules
- 🔄 Real-time matching API
- 🔄 Enhanced visualization tools

### Long Term
- 🎯 Graph-based relationships
- 🎯 AI-powered data stewardship
- 🎯 Automated rule learning
- 🎯 Blockchain integration for lineage

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup
```bash
# Clone repository
git clone https://github.com/ns-3e/OpenMatch.git
cd OpenMatch

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

🚀 **Ready to master your data? Get started with OpenMatch today!**
