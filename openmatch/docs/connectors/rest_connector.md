# REST API Connector Documentation

The REST API Connector provides integration with HTTP/REST endpoints, enabling OpenMatch to interact with web services and APIs. This document details the connector's features and usage patterns.

## Overview

The `RestConnector` class implements the standard OpenMatch connector interface for REST APIs, providing:

- HTTP method support (GET, POST, PUT, DELETE)
- Authentication handling (Basic Auth, Bearer Token)
- Custom header support
- Automatic response parsing
- Error handling and retries
- Session management

## Configuration

### Basic Setup

```python
from openmatch.connectors import RestConnector

connector = RestConnector(
    base_url="https://api.example.com/v1",
    headers={
        "Content-Type": "application/json",
        "Accept": "application/json"
    },
    timeout=30  # seconds
)
```

### Authentication Options

#### Bearer Token
```python
connector = RestConnector(
    base_url="https://api.example.com/v1",
    auth={"token": "your_bearer_token"}
)
```

#### Basic Auth
```python
connector = RestConnector(
    base_url="https://api.example.com/v1",
    auth={
        "username": "your_username",
        "password": "your_password"
    }
)
```

## Operations

### Reading Records

The `read_records` method fetches data from REST endpoints:

```python
# Simple GET request
records = connector.read_records("customers")

# GET with query parameters
records = connector.read_records(
    "customers",
    params={
        "status": "active",
        "limit": 100
    }
)
```

#### Response Handling
The connector automatically handles common API response formats:

```json
// Array response
[
    {"id": "1", "name": "John"},
    {"id": "2", "name": "Jane"}
]

// Object response with data array
{
    "data": [
        {"id": "1", "name": "John"},
        {"id": "2", "name": "Jane"}
    ],
    "metadata": {
        "total": 2
    }
}
```

### Writing Records

The `write_records` method supports both single and batch record creation:

```python
# Single record
count = connector.write_records(
    [{"name": "John Smith"}],
    target="customers"
)

# Batch records
count = connector.write_records(
    [
        {"name": "John Smith"},
        {"name": "Jane Doe"}
    ],
    target="customers"
)
```

### Updating Records

The `update_records` method handles record updates via PUT requests:

```python
count = connector.update_records(
    [
        {
            "id": "123",
            "name": "John Smith",
            "email": "john@example.com"
        }
    ],
    target="customers"
)
```

### Deleting Records

The `delete_records` method removes records via DELETE requests:

```python
count = connector.delete_records(
    ["123", "456"],
    target="customers"
)
```

## Error Handling

The connector implements comprehensive error handling:

```python
try:
    records = connector.read_records("customers")
except requests.exceptions.RequestException as e:
    logger.error(f"API request failed: {str(e)}")
except ValueError as e:
    logger.error(f"Response parsing failed: {str(e)}")
```

### HTTP Status Codes
- 2xx: Successful operations
- 4xx: Client errors (invalid requests)
- 5xx: Server errors (API issues)

### Timeout Handling
```python
# Configure timeout
connector = RestConnector(
    base_url="https://api.example.com",
    timeout=60  # 60 second timeout
)
```

## Best Practices

### 1. URL Management
```python
# Good: Clean URL handling
connector = RestConnector("https://api.example.com/v1")
records = connector.read_records("customers")  # Calls /v1/customers

# Avoid: Duplicate slashes
connector = RestConnector("https://api.example.com/v1/")
records = connector.read_records("/customers")  # Still works, but less clean
```

### 2. Batch Operations
```python
# Good: Batch processing
records = [
    {"name": "John"},
    {"name": "Jane"}
]
connector.write_records(records, "customers")

# Avoid: Individual requests
for record in records:
    connector.write_records([record], "customers")
```

### 3. Resource Management
```python
# Good: Using context manager
with RestConnector(base_url) as connector:
    records = connector.read_records("customers")

# Alternative: Manual cleanup
connector = RestConnector(base_url)
try:
    records = connector.read_records("customers")
finally:
    connector.close()
```

### 4. Error Handling
```python
# Good: Comprehensive error handling
try:
    records = connector.read_records("customers")
    if not records:
        logger.warning("No records found")
except Exception as e:
    logger.error(f"Operation failed: {str(e)}")
    raise
```

## Advanced Features

### 1. Custom Response Parsing
```python
class CustomRestConnector(RestConnector):
    def _parse_response(self, response):
        data = response.json()
        return data.get("custom_data_field", [])
```

### 2. Request Middleware
```python
class LoggingRestConnector(RestConnector):
    def _before_request(self, method, url, **kwargs):
        logger.info(f"Making {method} request to {url}")
        return kwargs
```

### 3. Retry Logic
```python
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class RetryingRestConnector(RestConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
```

## Performance Considerations

### 1. Connection Pooling
The connector uses `requests.Session()` for connection pooling:
- Reuses TCP connections
- Maintains cookie state
- Improves performance

### 2. Batch Operations
- Use batch endpoints when available
- Configure appropriate batch sizes
- Monitor response times

### 3. Timeouts
- Set appropriate timeouts
- Consider operation complexity
- Handle timeout exceptions

## Security

### 1. SSL/TLS
```python
# Verify SSL certificates
connector = RestConnector(
    base_url="https://api.example.com",
    verify=True  # Verify SSL certificates
)
```

### 2. Authentication
- Use environment variables for credentials
- Support token refresh
- Implement rate limiting

### 3. Data Protection
- Use HTTPS only
- Encrypt sensitive data
- Implement request signing

## Troubleshooting

### Common Issues

1. Connection Errors
   - Check network connectivity
   - Verify base URL
   - Check SSL/TLS settings

2. Authentication Failures
   - Verify credentials
   - Check token expiration
   - Review request headers

3. Timeout Issues
   - Adjust timeout settings
   - Check API responsiveness
   - Monitor network latency

### Debugging

```python
import logging
import http.client

# Enable HTTP debugging
http.client.HTTPConnection.debuglevel = 1
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True
```

## Dependencies

- requests>=2.25.0
- urllib3>=1.26.0 