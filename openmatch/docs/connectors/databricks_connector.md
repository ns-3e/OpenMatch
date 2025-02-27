# Databricks Connector Documentation

The Databricks Connector enables seamless integration between OpenMatch and Databricks, providing access to Databricks SQL Analytics and the Lakehouse platform. This document details the connector's features and usage patterns.

## Overview

The `DatabricksConnector` class implements the standard OpenMatch connector interface for Databricks, providing:

- Native Databricks SQL connectivity
- Delta Lake table operations
- Unity Catalog support
- Batch data operations
- Query execution and parameterization
- Resource management

## Configuration

### Basic Setup

```python
from openmatch.connectors import DatabricksConnector

connector = DatabricksConnector(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/warehouse-id",
    access_token="your-access-token",
    catalog="your_catalog",      # Optional Unity Catalog
    schema="your_schema"        # Optional schema
)

# Establish connection
success = connector.connect()
```

### Connection Parameters

- **server_hostname**: Databricks workspace URL
- **http_path**: SQL warehouse endpoint path
- **access_token**: Personal access token or service principal token
- **catalog**: Optional Unity Catalog name
- **schema**: Optional schema name

### Authentication Options

#### Personal Access Token (PAT)
```python
connector = DatabricksConnector(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/warehouse-id",
    access_token="dapi1234567890abcdef"
)
```

#### Azure AD Service Principal
```python
connector = DatabricksConnector(
    server_hostname="adb-xxx.azuredatabricks.net",
    http_path="/sql/1.0/warehouses/warehouse-id",
    access_token=service_principal_token
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

The `write_records` method supports batch inserts:

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

The `update_records` method uses Delta Lake MERGE capability:

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
except sql.Error as e:
    logger.error(f"Databricks SQL Error: {str(e)}")
except Exception as e:
    logger.error(f"Operation failed: {str(e)}")
```

### Common Errors
- Connection failures
- Authentication errors
- Query syntax errors
- Resource constraints
- Permission issues

## Best Practices

### 1. Connection Management
```python
# Using context manager
with DatabricksConnector(**config) as connector:
    records = connector.read_records(query)

# Manual cleanup
connector = DatabricksConnector(**config)
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
# Use Delta Lake optimizations
query = """
SELECT /*+ REPARTITION(10) */
    c.*, o.order_count
FROM customers c
JOIN (
    SELECT customer_id, COUNT(*) as order_count
    FROM orders
    WHERE date >= current_date() - INTERVAL 30 DAYS
    GROUP BY customer_id
) o ON c.id = o.customer_id
WHERE c.region = %(region)s
"""

records = connector.read_records(query, params={"region": "WEST"})
```

### 4. Unity Catalog Usage
```python
# Specify fully qualified names
connector = DatabricksConnector(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/warehouse-id",
    access_token="your-token",
    catalog="main",
    schema="default"
)

# Query across catalogs
records = connector.read_records(
    "SELECT * FROM catalog_a.schema_b.table_c"
)
```

## Performance Optimization

### 1. Warehouse Configuration
- Size SQL warehouse appropriately
- Use auto-scaling
- Configure spot instances
- Set appropriate timeout

### 2. Data Loading
- Use batch operations
- Leverage Delta Lake optimizations
- Configure partition columns
- Use Z-ordering for large tables

### 3. Query Performance
- Use Delta Lake caching
- Implement data skipping
- Optimize file layouts
- Monitor query profiles

### 4. Resource Management
- Close connections properly
- Release warehouse resources
- Monitor cluster usage
- Implement connection pooling

## Security

### 1. Authentication
```python
# Use environment variables
import os

connector = DatabricksConnector(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")
)
```

### 2. Network Security
- Enable transport encryption
- Use private endpoints
- Configure IP access lists
- Implement network isolation

### 3. Data Protection
- Enable table access control
- Use column-level security
- Implement row-level security
- Enable audit logging

## Advanced Features

### 1. Custom Query Execution
```python
class CustomDatabricksConnector(DatabricksConnector):
    def execute_with_history(self, query: str) -> Dict[str, Any]:
        result = self.read_records(query)
        history = self.read_records("DESCRIBE HISTORY delta.`table`")
        return {"data": result, "history": history}
```

### 2. Delta Lake Operations
```python
class DeltaLakeConnector(DatabricksConnector):
    def optimize_table(self, table: str) -> None:
        self._cursor.execute(f"OPTIMIZE {table}")
        
    def vacuum_table(self, table: str, retention_hours: int = 168) -> None:
        self._cursor.execute(f"VACUUM {table} RETAIN {retention_hours} HOURS")
```

### 3. Monitoring
```python
class MonitoredDatabricksConnector(DatabricksConnector):
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
   - Verify workspace URL
   - Check access token
   - Validate warehouse status
   - Review network settings

2. Performance Issues
   - Monitor warehouse load
   - Check query optimization
   - Review data distribution
   - Analyze execution plans

3. Resource Constraints
   - Monitor warehouse sizing
   - Check concurrent queries
   - Review resource limits
   - Optimize warehouse configs

### Debugging

```python
import logging

# Enable Databricks SQL logging
logging.getLogger("databricks.sql").setLevel(logging.DEBUG)
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.DEBUG
)
```

## Dependencies

Required packages:
- databricks-sql-connector>=2.0.0
- pyarrow (for optimized data transfer)
- pandas (optional, for DataFrame support)

## Version Compatibility

- Python 3.7+
- Databricks Runtime 7.0+
- Databricks SQL Connector 2.0.0+
- Delta Lake 1.0+ 