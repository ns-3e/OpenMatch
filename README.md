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

# OpenMatch MDM System

OpenMatch is a powerful Master Data Management (MDM) system that uses advanced vector similarity search and machine learning for accurate record matching and deduplication.

## Features

- Vector-based similarity search using pgvector
- Configurable matching rules and thresholds
- Automatic schema and vector index management
- Batch processing with optimized performance
- Support for multiple source systems
- Real-time and batch matching capabilities
- Materialized views for performance optimization
- Comprehensive logging and monitoring

## Quick Start Guide

### 1. Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/openmatch.git
cd openmatch

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

### 2. Database Setup

1. Install PostgreSQL 14+ and the pgvector extension
```bash
# On Ubuntu/Debian
sudo apt-get install postgresql-14 postgresql-14-pgvector

# On macOS with Homebrew
brew install postgresql@14
brew install pgvector
```

2. Create the MDM database:
```sql
CREATE DATABASE mdm;
```

### 3. Configuration

1. Configure your database settings in `openmatch/match/local_settings.py`:

```python
# local_settings.py
MDM_DB = {
    'ENGINE': 'postgresql',
    'HOST': 'localhost',
    'PORT': 5432,
    'NAME': 'mdm',
    'USER': 'your_user',
    'PASSWORD': 'your_password',
    'SCHEMA': 'mdm',
}

# Configure your source systems
SOURCE_SYSTEMS = {
    'source1': {
        'ENGINE': 'postgresql',
        'HOST': 'localhost',
        'PORT': 5432,
        'NAME': 'source1',
        'USER': 'source1_user',
        'PASSWORD': 'source1_password',
        'ENTITY_TYPE': 'person',
        'QUERY': 'SELECT * FROM persons WHERE updated_at > :last_sync',
        'FIELD_MAPPINGS': {
            'first_name': 'given_name',
            'last_name': 'family_name',
        }
    }
}
```

2. Adjust matching settings if needed:
```python
MATCH_SETTINGS = {
    'BLOCKING_KEYS': ['first_name', 'last_name', 'birth_date'],
    'MATCH_THRESHOLD': 0.8,
    'MERGE_THRESHOLD': 0.9,
}
```

### 4. Initialize the System

```bash
# Initialize database schema and extensions
python manage.py init_db
```

### 5. Process Matches

```bash
# Process matches with default batch size
python manage.py process_matches

# Process matches with custom batch size
python manage.py process_matches --batch_size 5000
```

### 6. Maintenance

```bash
# Refresh materialized views
python manage.py refresh_views
```

## Project Structure

```
openmatch/
├── match/
│   ├── __init__.py
│   ├── settings.py      # Main settings file
│   ├── local_settings.py  # Local overrides (create this)
│   ├── engine.py        # Core matching engine
│   ├── db_ops.py        # Database operations
│   ├── rules.py         # Matching rules
│   └── matchers.py      # Matching algorithms
├── manage.py            # Management script
├── requirements.txt
└── README.md
```

## Configuration Options

### Vector Search Settings

```python
VECTOR_SETTINGS = {
    'BACKEND': VectorBackend.PGVECTOR,
    'DIMENSION': 768,
    'INDEX_TYPE': 'ivfflat',  # or 'hnsw'
    'IVF_LISTS': 100,
    'PROBES': 10,
    'SIMILARITY_THRESHOLD': 0.8,
}
```

### Model Settings

```python
MODEL_SETTINGS = {
    'EMBEDDING_MODEL': 'sentence-transformers/all-MiniLM-L6-v2',
    'USE_GPU': False,
    'BATCH_SIZE': 128,
}
```

### Processing Settings

```python
PROCESSING = {
    'BATCH_SIZE': 10000,
    'MAX_WORKERS': None,  # None = 2 * CPU cores
    'USE_PROCESSES': False,
}
```

## Using the API

```python
from openmatch.match.engine import MatchEngine
from openmatch.match.db_ops import DatabaseOptimizer, BatchProcessor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Create database session
engine = create_engine("postgresql://user:password@localhost/mdm")
Session = sessionmaker(bind=engine)
session = Session()

# Initialize match engine
match_engine = MatchEngine(session)

# Process matches
processor = BatchProcessor(session, match_engine)
processor.process_matches()

# Find matches for a specific record
matches = match_engine.find_candidates({
    'first_name': 'John',
    'last_name': 'Doe',
    'birth_date': '1990-01-01'
})

# Get match details
for match_id, similarity in matches:
    print(f"Match ID: {match_id}, Similarity: {similarity}")
```

## Best Practices

1. **Environment Variables**: Store sensitive information in environment variables:
```bash
export MDM_DB_PASSWORD=your_secure_password
export SOURCE1_DB_PASSWORD=another_secure_password
```

2. **Batch Size**: Adjust batch sizes based on your system's memory:
   - For systems with 8GB RAM: 5,000-10,000 records
   - For systems with 16GB RAM: 10,000-20,000 records
   - For systems with 32GB+ RAM: 20,000-50,000 records

3. **Vector Index**: Choose the appropriate vector index:
   - `ivfflat`: Better for frequent updates, good balance of speed/accuracy
   - `hnsw`: Better for read-heavy workloads, highest accuracy

4. **Monitoring**: Monitor the system using materialized views:
```sql
SELECT * FROM mdm.match_statistics;
SELECT * FROM mdm.blocking_statistics;
```

## Troubleshooting

1. **Vector Extension Issues**:
```sql
-- Check if pgvector is installed
SELECT * FROM pg_extension WHERE extname = 'vector';

-- Manually install if needed
CREATE EXTENSION vector;
```

2. **Performance Issues**:
```sql
-- Check index usage
SELECT * FROM pg_stat_user_indexes 
WHERE schemaname = 'mdm' 
AND indexrelname LIKE '%vector%';

-- Analyze tables
ANALYZE mdm.record_embeddings;
```

3. **Memory Issues**:
- Reduce batch size in PROCESSING settings
- Increase PostgreSQL work_mem for vector operations
- Monitor system memory usage during processing

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

🚀 **Ready to master your data? Get started with OpenMatch today!**
