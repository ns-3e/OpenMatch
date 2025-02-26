# Azure Synapse Connector Documentation

The Azure Synapse Connector provides integration between OpenMatch and Azure Synapse Analytics, enabling enterprise-scale data operations with Microsoft's cloud data warehouse. This document details the connector's features and usage patterns.

## Overview

The `AzureConnector` class implements the standard OpenMatch connector interface for Azure Synapse Analytics, providing:

- Native Azure Synapse connectivity
- SQL query execution and parameterization
- Batch data operations
- Transaction management
- Error handling and retries
- Resource cleanup

## Configuration

### Basic Setup

```python
from openmatch.connectors import AzureConnector

connector = AzureConnector(
    server="your-server.database.windows.net",
    database="your_database",
    username="your_username",
    password="your_password",
    port=1433  # Optional, defaults to 1433
)

# Establish connection
success = connector.connect()
```

### Connection Parameters

- **server**: Azure Synapse server hostname
- **database**: Target database name
- **username**: Authentication username
- **password**: User password
- **port**: Optional server port (default: 1433)

### Authentication Options

#### SQL Authentication
```python
connector = AzureConnector(
    server="your-server.database.windows.net",
    database="your_database",
    username="your_username",
    password="your_password"
)
```

#### Azure AD Authentication
```python
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
token = credential.get_token("https://database.windows.net/")

connector = AzureConnector(
    server="your-server.database.windows.net",
    database="your_database",
    username="your_username@your_tenant.onmicrosoft.com",
    password=token.token
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
    WHERE region = ? 
    AND status = ?
    """,
    params={"region": "WEST", "status": "ACTIVE"}
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

The `update_records` method handles record updates:

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
except pyodbc.Error as e:
    logger.error(f"Database Error: {str(e)}")
except Exception as e:
    logger.error(f"Operation failed: {str(e)}")
```

### Common Errors
- Connection failures
- Authentication errors
- Query syntax errors
- Resource constraints
- Transaction deadlocks

## Best Practices

### 1. Connection Management
```python
# Using context manager
with AzureConnector(**config) as connector:
    records = connector.read_records(query)

# Manual cleanup
connector = AzureConnector(**config)
try:
    connector.connect()
    records = connector.read_records(query)
finally:
    connector.close()
```

### 2. Batch Operations
```python
# Configure batch size for large datasets
BATCH_SIZE = 1000
for batch in chunks(records, BATCH_SIZE):
    connector.write_records(batch, "customers")
```

### 3. Query Optimization
```python
# Use efficient queries
query = """
SELECT c.*, o.order_count
FROM customers c
WITH (NOLOCK)  -- For read operations
JOIN (
    SELECT customer_id, COUNT(*) as order_count
    FROM orders WITH (NOLOCK)
    WHERE date >= DATEADD(day, -30, GETDATE())
    GROUP BY customer_id
) o ON c.id = o.customer_id
WHERE c.region = ?
"""

records = connector.read_records(query, params={"region": "WEST"})
```

### 4. Transaction Management
```python
try:
    # Begin transaction
    connector._conn.autocommit = False
    
    # Perform operations
    connector.write_records(records1, "table1")
    connector.write_records(records2, "table2")
    
    # Commit transaction
    connector._conn.commit()
except Exception:
    # Rollback on error
    connector._conn.rollback()
    raise
finally:
    connector._conn.autocommit = True
```

## Performance Optimization

### 1. Resource Configuration
- Configure appropriate DWU
- Use columnstore indexes
- Implement table partitioning
- Optimize resource classes

### 2. Data Loading
- Use bulk operations
- Configure appropriate batch sizes
- Use staging tables
- Implement partition switching

### 3. Query Performance
- Use appropriate indexes
- Implement statistics
- Optimize distribution keys
- Monitor query plans

### 4. Resource Management
- Close connections properly
- Use connection pooling
- Monitor resource usage
- Implement retry logic

## Security

### 1. Authentication
```python
# Use environment variables
import os

connector = AzureConnector(
    server=os.getenv("AZURE_SERVER"),
    database=os.getenv("AZURE_DATABASE"),
    username=os.getenv("AZURE_USERNAME"),
    password=os.getenv("AZURE_PASSWORD")
)
```

### 2. Network Security
- Enable encryption
- Use virtual networks
- Configure firewalls
- Implement private endpoints

### 3. Data Protection
- Enable TDE
- Use column encryption
- Implement row-level security
- Enable auditing

## Advanced Features

### 1. Custom Query Execution
```python
class CustomAzureConnector(AzureConnector):
    def execute_stored_procedure(
        self,
        name: str,
        params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        param_string = ",".join(["?" for _ in params])
        query = f"EXEC {name} {param_string}"
        return self.read_records(query, list(params.values()))
```

### 2. Monitoring
```python
class MonitoredAzureConnector(AzureConnector):
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

### 3. Retry Logic
```python
from tenacity import retry, stop_after_attempt, wait_exponential

class RetryingAzureConnector(AzureConnector):
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def execute_with_retry(self, query: str, params: Optional[Dict] = None):
        return self.read_records(query, params)
```

## Troubleshooting

### Common Issues

1. Connection Problems
   - Verify server name
   - Check credentials
   - Review firewall rules
   - Validate network access

2. Performance Issues
   - Monitor DWU usage
   - Check query plans
   - Review statistics
   - Analyze resource usage

3. Resource Constraints
   - Monitor concurrent queries
   - Check resource class settings
   - Review tempdb usage
   - Optimize workload groups

### Debugging

```python
import logging

# Enable ODBC logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("pyodbc")
logger.setLevel(logging.DEBUG)

# Enable connection tracing
connector._conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
connector._conn.setencoding(encoding='utf-8')
```

## Dependencies

Required packages:
- pyodbc>=4.0.30
- azure-identity (for Azure AD authentication)
- tenacity (for retry logic)

## Version Compatibility

- Python 3.7+
- pyodbc 4.0.30+
- Azure Synapse SQL Pool (all versions)
- ODBC Driver 17+ for SQL Server 