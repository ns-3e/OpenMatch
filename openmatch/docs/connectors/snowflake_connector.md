# Snowflake Connector Documentation

The Snowflake Connector provides seamless integration between OpenMatch and Snowflake Data Warehouse, enabling efficient data operations at scale. This document details the connector's features and best practices.

## Overview

The `SnowflakeConnector` class implements the standard OpenMatch connector interface for Snowflake, providing:

- Native Snowflake connection management
- SQL query execution and parameterization
- Batch data operations
- Transaction management
- Error handling and retries
- Resource cleanup

## Configuration

### Basic Setup

```python
from openmatch.connectors import SnowflakeConnector

connector = SnowflakeConnector(
    account="your_account",      # Snowflake account identifier
    user="your_username",        # Snowflake username
    password="your_password",    # User password
    warehouse="your_warehouse",  # Compute warehouse
    database="your_database",    # Target database
    schema="your_schema",       # Target schema
    role="optional_role"        # Optional role assignment
)

# Establish connection
success = connector.connect()
```

### Connection Parameters

- **account**: Your Snowflake account identifier (e.g., "xy12345.us-east-1")
- **user**: Authentication username
- **password**: User password
- **warehouse**: Compute warehouse for query execution
- **database**: Default database context
- **schema**: Default schema context
- **role**: Optional role to assume after connection

### Authentication Options

#### Basic Authentication
```python
connector = SnowflakeConnector(
    account="xy12345.us-east-1",
    user="your_username",
    password="your_password",
    warehouse="COMPUTE_WH"
)
```

#### Key Pair Authentication
```python
connector = SnowflakeConnector(
    account="xy12345.us-east-1",
    user="your_username",
    private_key_path="/path/to/rsa_key.p8",
    warehouse="COMPUTE_WH"
)
```

#### SSO Authentication
```python
connector = SnowflakeConnector(
    account="xy12345.us-east-1",
    user="your_username",
    authenticator="externalbrowser",
    warehouse="COMPUTE_WH"
)
```

## Operations

### Reading Records

The `read_records` method executes SQL queries and returns results:

```python
# Simple query
records = connector.read_records(
    "SELECT * FROM customers"
)

# Parameterized query
records = connector.read_records(
    """
    SELECT * 
    FROM customers 
    WHERE region = %(region)s 
    AND status = %(status)s
    """,
    params={
        "region": "WEST",
        "status": "ACTIVE"
    }
)
```

### Writing Records

The `write_records` method supports bulk inserts:

```python
# Insert records
count = connector.write_records(
    records=[
        {"id": "1", "name": "John Smith", "region": "WEST"},
        {"id": "2", "name": "Jane Doe", "region": "EAST"}
    ],
    target="customers"
)
```

### Updating Records

The `update_records` method uses Snowflake's MERGE capability:

```python
# Update existing records
count = connector.update_records(
    records=[
        {
            "id": "1",
            "name": "John Smith",
            "email": "john@example.com"
        }
    ],
    target="customers"
)
```

### Deleting Records

The `delete_records` method removes records by ID:

```python
# Delete records
count = connector.delete_records(
    record_ids=["1", "2", "3"],
    target="customers"
)
```

## Error Handling

The connector implements comprehensive error handling:

```python
try:
    records = connector.read_records(query)
except snowflake.connector.errors.ProgrammingError as e:
    logger.error(f"SQL Error: {str(e)}")
except snowflake.connector.errors.DatabaseError as e:
    logger.error(f"Database Error: {str(e)}")
except Exception as e:
    logger.error(f"Operation failed: {str(e)}")
```

### Common Errors
- Connection failures
- Authentication errors
- Query syntax errors
- Resource constraints
- Transaction conflicts

## Best Practices

### 1. Connection Management
```python
# Using context manager
with SnowflakeConnector(**config) as connector:
    records = connector.read_records(query)

# Manual cleanup
connector = SnowflakeConnector(**config)
try:
    connector.connect()
    records = connector.read_records(query)
finally:
    connector.close()
```

### 2. Batch Operations
```python
# Configure batch size for large datasets
BATCH_SIZE = 10000
for batch in chunks(records, BATCH_SIZE):
    connector.write_records(batch, "customers")
```

### 3. Query Optimization
```python
# Use parameterized queries
query = """
SELECT *
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE c.region = %(region)s
AND o.status = %(status)s
"""

records = connector.read_records(
    query,
    params={
        "region": "WEST",
        "status": "COMPLETED"
    }
)
```

### 4. Transaction Management
```python
try:
    # Begin transaction
    connector.execute("BEGIN")
    
    # Perform operations
    connector.write_records(records1, "table1")
    connector.write_records(records2, "table2")
    
    # Commit transaction
    connector.execute("COMMIT")
except Exception:
    # Rollback on error
    connector.execute("ROLLBACK")
    raise
```

## Performance Optimization

### 1. Warehouse Configuration
- Size warehouse appropriately
- Use auto-suspend/resume
- Monitor credit usage

### 2. Data Loading
- Use bulk operations
- Configure appropriate batch sizes
- Leverage COPY commands for large datasets

### 3. Query Performance
- Use clustering keys
- Leverage materialized views
- Monitor query profiles

### 4. Resource Management
- Close connections properly
- Release warehouse resources
- Monitor session usage

## Security

### 1. Authentication
```python
# Use environment variables
import os

connector = SnowflakeConnector(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD")
)
```

### 2. Network Security
- Enable SSL/TLS
- Configure IP allowlisting
- Use private endpoints

### 3. Data Protection
- Enable data encryption
- Use column-level security
- Implement row-level security

## Advanced Features

### 1. Custom SQL Execution
```python
class CustomSnowflakeConnector(SnowflakeConnector):
    def execute_procedure(self, name: str, params: Dict) -> Any:
        query = f"CALL {name}(%(param1)s, %(param2)s)"
        return self.execute(query, params)
```

### 2. Retry Logic
```python
from snowflake.connector.errors import OperationalError

class RetryingSnowflakeConnector(SnowflakeConnector):
    def execute_with_retry(self, query: str, max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                return self.execute(query)
            except OperationalError:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
```

### 3. Monitoring
```python
class MonitoredSnowflakeConnector(SnowflakeConnector):
    def execute(self, query: str, params: Optional[Dict] = None):
        start_time = time.time()
        try:
            result = super().execute(query, params)
            duration = time.time() - start_time
            logger.info(f"Query executed in {duration:.2f}s")
            return result
        except Exception as e:
            logger.error(f"Query failed after {time.time() - start_time:.2f}s")
            raise
```

## Troubleshooting

### Common Issues

1. Connection Problems
   - Verify account identifier
   - Check network connectivity
   - Validate credentials
   - Review role permissions

2. Performance Issues
   - Monitor warehouse sizing
   - Check query optimization
   - Review data clustering
   - Analyze execution plans

3. Resource Constraints
   - Monitor warehouse credits
   - Check concurrent sessions
   - Review resource limits
   - Optimize warehouse usage

### Debugging

```python
import logging

# Enable Snowflake logging
logging.getLogger("snowflake.connector").setLevel(logging.DEBUG)
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.DEBUG
)
```

## Dependencies

Required packages:
- snowflake-connector-python>=2.7.0
- cryptography (for key pair authentication)
- pyOpenSSL (for security features)

## Version Compatibility

- Python 3.7+
- Snowflake Connector Python 2.7.0+
- Snowflake Data Platform (all versions) 