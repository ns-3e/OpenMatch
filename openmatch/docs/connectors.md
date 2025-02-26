# Connectors Module Documentation

The connectors module provides a standardized interface for integrating OpenMatch with various data sources and platforms. It implements a flexible connector framework that supports both batch and real-time data operations.

## Core Components

### 1. Base Connector Interface

The `Connector` abstract base class defines the standard interface that all connectors must implement:

```python
from openmatch.connectors import Connector

class CustomConnector(Connector):
    def connect(self) -> bool:
        """Establish connection to data source"""
        pass
        
    def read_records(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Read records from data source"""
        pass
        
    def write_records(self, records: List[Dict[str, Any]], target: str) -> int:
        """Write records to data source"""
        pass
        
    def update_records(self, records: List[Dict[str, Any]], target: str) -> int:
        """Update existing records"""
        pass
        
    def delete_records(self, record_ids: List[str], target: str) -> int:
        """Delete records from data source"""
        pass
        
    def close(self) -> None:
        """Close the connection"""
        pass
```

### 2. Built-in Connectors

#### Flat File Connector
Handles CSV, Excel, XML, and JSON file formats with automatic type detection.

```python
from openmatch.connectors import FlatFileConnector

connector = FlatFileConnector(
    base_path="/path/to/data",
    file_type="auto",  # auto, csv, excel, xml, json
    encoding="utf-8",
    csv_separator=",",
    excel_sheet="Sheet1"  # Optional
)

# Read from any supported format
records = connector.read_records("customers.csv")
records = connector.read_records("sales.xlsx")
records = connector.read_records("products.xml")
records = connector.read_records("orders.json")

# Write records
count = connector.write_records(
    records=[{"id": "1", "name": "John"}],
    target="output/customers.csv"
)
```

For detailed documentation on the Flat File Connector, see [Flat File Connector Documentation](flat_file_connector.md).

#### Snowflake Connector
Connects to Snowflake Data Warehouse for data operations.

```python
from openmatch.connectors import SnowflakeConnector

connector = SnowflakeConnector(
    account="your_account",
    user="your_user",
    password="your_password",
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema",
    role="optional_role"
)

# Connect to Snowflake
connector.connect()

# Read records
records = connector.read_records(
    "SELECT * FROM customers WHERE region = %(region)s",
    params={"region": "WEST"}
)

# Write records
count = connector.write_records(
    records=[{"id": "1", "name": "John"}],
    target="customers"
)
```

#### Databricks Connector
Integrates with Databricks for distributed data processing.

```python
from openmatch.connectors import DatabricksConnector

connector = DatabricksConnector(
    host="your_workspace_url",
    token="your_access_token",
    cluster_id="your_cluster_id"
)

# Execute Spark SQL
results = connector.read_records(
    "SELECT * FROM customer_data"
)
```

#### Azure Synapse Connector
Connects to Azure Synapse Analytics for enterprise-scale operations.

```python
from openmatch.connectors import AzureSynapseConnector

connector = AzureSynapseConnector(
    server="your_server",
    database="your_database",
    username="your_username",
    password="your_password"
)
```

#### REST API Connector
Enables integration with HTTP/REST endpoints.

```python
from openmatch.connectors import RESTConnector

connector = RESTConnector(
    base_url="https://api.example.com",
    auth_token="your_token",
    headers={
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
)

# Read from API
records = connector.read_records(
    "customers",
    params={"status": "active"}
)
```

## Common Features

### 1. Connection Management
- Automatic connection establishment
- Connection pooling (where supported)
- Graceful error handling
- Resource cleanup

### 2. Data Operations
- Batch record reading
- Bulk inserts/updates
- Transaction support
- Parameterized queries

### 3. Error Handling
- Connection error recovery
- Transaction rollback
- Detailed error reporting
- Retry mechanisms

### 4. Security
- Secure credential management
- Role-based access
- SSL/TLS support
- Token-based authentication

## Best Practices

### 1. Connection Management
```python
# Recommended pattern using context manager
with connector:
    records = connector.read_records(query)
    connector.write_records(records, target)
```

### 2. Batch Processing
```python
# Process records in batches
BATCH_SIZE = 1000
for batch in chunks(records, BATCH_SIZE):
    connector.write_records(batch, target)
```

### 3. Error Handling
```python
try:
    connector.connect()
    connector.write_records(records, target)
except Exception as e:
    logger.error(f"Operation failed: {str(e)}")
finally:
    connector.close()
```

### 4. Query Parameters
```python
# Use parameterized queries for security
records = connector.read_records(
    "SELECT * FROM users WHERE status = %(status)s",
    params={"status": "active"}
)
```

## Performance Optimization

### 1. Batch Operations
- Use bulk operations when possible
- Configure appropriate batch sizes
- Monitor memory usage

### 2. Connection Pooling
- Reuse connections when possible
- Configure pool size appropriately
- Monitor pool utilization

### 3. Query Optimization
- Use efficient queries
- Leverage database indexes
- Monitor query performance

### 4. Resource Management
- Close connections properly
- Release resources timely
- Monitor resource usage

## Security Considerations

### 1. Credential Management
- Use environment variables
- Support secret management services
- Encrypt sensitive data

### 2. Access Control
- Implement role-based access
- Use minimum required privileges
- Audit access patterns

### 3. Data Protection
- Enable SSL/TLS
- Encrypt sensitive data
- Implement data masking

## Extending the Module

### 1. Custom Connectors
```python
from openmatch.connectors import Connector

class CustomConnector(Connector):
    def __init__(self, **config):
        self.config = config
        self._connection = None
        
    def connect(self) -> bool:
        # Implement connection logic
        pass
        
    def read_records(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        # Implement read logic
        pass
```

### 2. Connection Adapters
```python
class ConnectionAdapter:
    def __init__(self, connection):
        self._conn = connection
        
    def execute(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        # Implement query execution
        pass
```

### 3. Custom Authentication
```python
class CustomAuthProvider:
    def __init__(self, **config):
        self.config = config
        
    def get_credentials(self) -> Dict[str, str]:
        # Implement credential retrieval
        pass
```

## Troubleshooting

### 1. Connection Issues
- Verify network connectivity
- Check credentials
- Validate configuration
- Review firewall settings

### 2. Performance Problems
- Monitor query execution time
- Check resource utilization
- Review batch sizes
- Analyze query plans

### 3. Data Issues
- Validate data types
- Check character encodings
- Verify data integrity
- Monitor error logs

## Dependencies

Each connector may have specific dependencies:

- **Snowflake**: snowflake-connector-python
- **Databricks**: databricks-connect
- **Azure**: azure-synapse-connector
- **REST**: requests

## Version Compatibility

- Python 3.7+
- Database-specific client versions
- Platform-specific SDK versions 