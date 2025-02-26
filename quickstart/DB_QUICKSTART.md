# OpenMatch Database Integration Quickstart

This guide demonstrates how to use OpenMatch with a PostgreSQL database for storing and managing master data, including test data generation.

## Prerequisites

1. PostgreSQL installed and running locally
2. Python 3.7+
3. OpenMatch installed with database support: `pip install openmatch[db]`

## Quick Setup

The fastest way to get started is to use our setup script:

```bash
# Clone the repository if you haven't already
git clone https://github.com/your-org/OpenMatch.git
cd OpenMatch

# Make the setup script executable and run it
chmod +x scripts/setup_db.sh
./scripts/setup_db.sh
```

This script will:
1. Create a Python virtual environment
2. Install all dependencies
3. Create the PostgreSQL database
4. Generate and load 100,000 test records
5. Set up the OpenMatch schema

## Manual Setup Steps

If you prefer to run the commands manually, follow these steps:

### 1. Environment Setup

Create a Python virtual environment and install dependencies:

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install dependencies
pip install faker==19.13.0 typer==0.9.0 pandas==2.1.4 numpy==1.26.3 psycopg2-binary==2.9.9
pip install openmatch[db]
```

### 2. Database Setup

```bash
# Create the database
createdb openmatch_test

# Set up environment variables
cat << EOF > .env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=openmatch_test
DB_USER=$(whoami)
DB_PASSWORD=postgres
EOF
```

### 3. Generate Test Data

The test data generator creates realistic person records with fuzzy duplicates:

```bash
# Generate 100,000 records
python test_data/generate_test_data.py \
    --num-records 100000 \
    --format postgres \
    --db-name openmatch_test \
    --db-user $(whoami) \
    --db-password postgres \
    --db-host localhost
```

This will create:
- ~70,000 unique person records
- ~42,000 fuzzy duplicate records
- ~146,000 email records (1:M relationship)
- ~145,000 phone records (1:M relationship)
- ~135,000 address records (1:M relationship)

### 4. Verify the Data

Connect to the database and check the records:

```bash
# Connect to the database
psql openmatch_test

# Check record counts
SELECT 'Persons' as table_name, COUNT(*) as count FROM persons
UNION ALL
SELECT 'Emails', COUNT(*) FROM emails
UNION ALL
SELECT 'Phones', COUNT(*) FROM phones
UNION ALL
SELECT 'Addresses', COUNT(*) FROM addresses
ORDER BY table_name;

# Sample some duplicate records
WITH duplicates AS (
    SELECT p1.first_name, p1.last_name, p1.source as source1, p2.source as source2
    FROM persons p1
    JOIN persons p2 ON 
        (p1.first_name SIMILAR TO p2.first_name || '%' OR p2.first_name SIMILAR TO p1.first_name || '%')
        AND p1.last_name = p2.last_name
        AND p1.id != p2.id
    LIMIT 5
)
SELECT * FROM duplicates;
```

## Database Schema

The test data is organized in the following schema:

### Core Tables

1. `persons` (root table):
   ```sql
   CREATE TABLE persons (
       id VARCHAR(50) PRIMARY KEY,
       source VARCHAR(20) NOT NULL,
       first_name VARCHAR(100),
       last_name VARCHAR(100),
       birth_date DATE,
       ssn VARCHAR(20),
       gender VARCHAR(1),
       created_at TIMESTAMP WITH TIME ZONE
   );
   ```

2. `emails` (1:M relationship):
   ```sql
   CREATE TABLE emails (
       id SERIAL PRIMARY KEY,
       person_id VARCHAR(50) REFERENCES persons(id),
       email VARCHAR(255) NOT NULL,
       is_primary BOOLEAN DEFAULT false,
       created_at TIMESTAMP WITH TIME ZONE,
       CONSTRAINT unique_email UNIQUE (email)
   );
   ```

3. `phones` (1:M relationship):
   ```sql
   CREATE TABLE phones (
       id SERIAL PRIMARY KEY,
       person_id VARCHAR(50) REFERENCES persons(id),
       phone_number VARCHAR(50) NOT NULL,
       type VARCHAR(20),
       is_primary BOOLEAN DEFAULT false,
       created_at TIMESTAMP WITH TIME ZONE
   );
   ```

4. `addresses` (1:M relationship):
   ```sql
   CREATE TABLE addresses (
       id SERIAL PRIMARY KEY,
       person_id VARCHAR(50) REFERENCES persons(id),
       street VARCHAR(255),
       city VARCHAR(100),
       state VARCHAR(50),
       postal_code VARCHAR(20),
       country VARCHAR(50),
       type VARCHAR(20),
       is_primary BOOLEAN DEFAULT false,
       created_at TIMESTAMP WITH TIME ZONE
   );
   ```

### Indexes

The following indexes are automatically created:
```sql
CREATE INDEX idx_persons_source ON persons(source);
CREATE INDEX idx_persons_names ON persons(first_name, last_name);
CREATE INDEX idx_emails_person ON emails(person_id);
CREATE INDEX idx_emails_email ON emails(email);
CREATE INDEX idx_phones_person ON phones(person_id);
CREATE INDEX idx_phones_number ON phones(phone_number);
CREATE INDEX idx_addresses_person ON addresses(person_id);
CREATE INDEX idx_addresses_postal ON addresses(postal_code);
```

## OpenMatch Integration

After setting up the test data, you can use OpenMatch's database integration as described in the Database Integration Example section below.

## Database Integration Example

```python
from openmatch.db import DatabaseConfig, PostgresConnector
from openmatch import MDMPipeline
from openmatch.config import TrustConfig, SurvivorshipRules

# Initialize database connection
db_config = DatabaseConfig.from_env()  # Reads from .env file
db = PostgresConnector(db_config)

# Initialize pipeline with database storage
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
    ),
    storage=db
)

# Process records (they will be automatically stored in the database)
results = pipeline.process_records(records)

# Query golden records from database
golden_records = db.query_golden_records(
    filters={"source": "CRM"},
    limit=100
)

# Get record lineage
lineage = db.get_record_lineage("GOLDEN_001")

# Get cross-references
xrefs = db.get_cross_references("CRM_001")
```

## Database Schema

OpenMatch automatically creates the following tables:

1. `golden_records`: Stores the master/golden records
2. `source_records`: Stores the original source records
3. `cross_references`: Maps source records to golden records
4. `lineage`: Tracks record history and changes
5. `trust_scores`: Stores calculated trust scores

### Schema Details

```sql
-- Golden Records
CREATE TABLE golden_records (
    id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Source Records
CREATE TABLE source_records (
    id VARCHAR(255) PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    trust_score FLOAT
);

-- Cross References
CREATE TABLE cross_references (
    id SERIAL PRIMARY KEY,
    golden_record_id VARCHAR(255) REFERENCES golden_records(id),
    source_record_id VARCHAR(255) REFERENCES source_records(id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(golden_record_id, source_record_id)
);

-- Lineage
CREATE TABLE lineage (
    id SERIAL PRIMARY KEY,
    golden_record_id VARCHAR(255) REFERENCES golden_records(id),
    action VARCHAR(50) NOT NULL,
    details JSONB,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Trust Scores
CREATE TABLE trust_scores (
    id SERIAL PRIMARY KEY,
    record_id VARCHAR(255) NOT NULL,
    score FLOAT NOT NULL,
    details JSONB,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

## Performance Optimization

1. **Indexes**:
   ```sql
   -- Add indexes for common queries
   CREATE INDEX idx_source_records_source ON source_records(source);
   CREATE INDEX idx_cross_references_golden_id ON cross_references(golden_record_id);
   CREATE INDEX idx_cross_references_source_id ON cross_references(source_record_id);
   CREATE INDEX idx_lineage_golden_id ON lineage(golden_record_id);
   ```

2. **Partitioning**:
   For large datasets, consider partitioning tables by date or source:
   ```sql
   -- Example: Partition source_records by source
   CREATE TABLE source_records (
       id VARCHAR(255),
       source VARCHAR(50),
       data JSONB,
       created_at TIMESTAMP
   ) PARTITION BY LIST (source);

   CREATE TABLE source_records_crm 
       PARTITION OF source_records FOR VALUES IN ('CRM');
   CREATE TABLE source_records_erp 
       PARTITION OF source_records FOR VALUES IN ('ERP');
   ```

## Batch Processing

For large datasets, use batch processing:

```python
from openmatch.db import batch_processor

# Process records in batches
with batch_processor(pipeline, batch_size=1000) as processor:
    for batch in record_generator:
        processor.process_batch(batch)

# Or use the async version for better performance
import asyncio
from openmatch.db import async_batch_processor

async def process_large_dataset(records):
    async with async_batch_processor(pipeline, batch_size=1000) as processor:
        for batch in records:
            await processor.process_batch(batch)

asyncio.run(process_large_dataset(records))
```

## Monitoring and Maintenance

1. **Monitor table sizes**:
   ```sql
   SELECT 
       relname as table_name,
       pg_size_pretty(pg_total_relation_size(relid)) as total_size
   FROM pg_catalog.pg_statio_user_tables
   ORDER BY pg_total_relation_size(relid) DESC;
   ```

2. **Clean up old records**:
   ```sql
   -- Remove old lineage records
   DELETE FROM lineage 
   WHERE timestamp < NOW() - INTERVAL '1 year';

   -- Archive old source records
   INSERT INTO source_records_archive 
   SELECT * FROM source_records 
   WHERE created_at < NOW() - INTERVAL '2 years';
   ```

## Best Practices

1. **Connection Management**:
   - Use connection pooling (pgbouncer)
   - Implement retry logic for failed connections
   - Set appropriate timeouts

2. **Data Consistency**:
   - Use transactions for related operations
   - Implement proper error handling
   - Regular database backups

3. **Performance**:
   - Index frequently queried fields
   - Partition large tables
   - Regular VACUUM and ANALYZE
   - Monitor query performance

4. **Security**:
   - Use SSL for connections
   - Implement row-level security if needed
   - Regular security audits 