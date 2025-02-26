# Utility Components

OpenMatch provides a comprehensive set of utility components through its `utils` module. These utilities provide essential functionality for configuration management, validation, and logging across the library.

## Core Components

### 1. Validation (`validation.py`)

The validation component provides a robust set of validation functions and decorators.

#### Key Features:
- Type validation
- Value constraints
- Pattern matching
- Complex data structures
- Custom validators

#### Usage Example:
```python
from openmatch.utils.validation import (
    validate_dict,
    validate_email,
    validate_phone,
    validator
)

# Validate dictionary against schema
schema = {
    "email": {
        "type": str,
        "required": True,
        "email": True
    },
    "phone": {
        "type": str,
        "phone": {"region": "US"}
    }
}

data = {
    "email": "user@example.com",
    "phone": "+1-555-0123"
}

validate_dict(data, schema)

# Use validator decorator
@validator(
    ("email", validate_email),
    ("phone", lambda x: validate_phone(x, "US"))
)
def process_contact(email: str, phone: str):
    # Process validated data
    pass
```

### 2. Configuration (`config.py`)

The configuration component manages configuration loading, validation, and merging.

#### Key Features:
- YAML configuration files
- Environment variables
- Configuration validation
- Config inheritance
- Caching support

#### Usage Example:
```python
from openmatch.utils.config import ConfigManager
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    host: str
    port: int
    username: str
    password: str

# Initialize config manager
config_mgr = ConfigManager("config")

# Load and validate configuration
db_config = config_mgr.get_config("database", DatabaseConfig)

# Merge configurations
base_config = config_mgr.get_config("base")
env_config = config_mgr.get_config("production")
merged = config_mgr.merge_configs(base_config, env_config)

# Save configuration
config_mgr.save_config(db_config, "database")

# Load from environment variables
env_config = config_mgr.get_env_config("APP_")
```

### 3. Logging (`logging.py`)

The logging component provides structured logging with context management.

#### Key Features:
- JSON formatting
- Context logging
- File and console output
- Extra field support
- Performance optimization

#### Usage Example:
```python
from openmatch.utils.logging import get_logger, ContextLogger

# Basic logger setup
logger = get_logger(
    "app",
    log_file="app.log",
    json_format=True,
    extra_fields={"app_version": "1.0.0"}
)

# Log with context
logger.info("Processing started", extra={"job_id": "123"})

# Use context manager
with ContextLogger(logger, operation="import", user_id="user123"):
    logger.info("Operation in progress")  # Includes context automatically
```

## Integration Features

### 1. Validation Integration
- Schema validation
- Custom validators
- Type checking
- Error collection
- Nested validation

```python
from openmatch.utils.validation import validate_dict

# Complex schema validation
schema = {
    "user": {
        "type": dict,
        "required": True,
        "schema": {
            "name": {"type": str, "required": True},
            "age": {"type": int, "range": {"min": 0, "max": 120}},
            "email": {"type": str, "email": True}
        }
    },
    "preferences": {
        "type": dict,
        "required": False,
        "schema": {
            "theme": {"type": str, "pattern": r"^(light|dark)$"},
            "notifications": {"type": bool}
        }
    }
}

validate_dict(user_data, schema)
```

### 2. Configuration Integration
- Multiple formats
- Environment overrides
- Validation rules
- Inheritance chains
- Cache management

```python
from openmatch.utils.config import ConfigManager

config_mgr = ConfigManager()

# Load configuration chain
base_config = config_mgr.get_config("base")
env_config = config_mgr.get_config("env")
user_config = config_mgr.get_config("user")

# Merge with precedence
final_config = config_mgr.merge_configs(
    config_mgr.merge_configs(base_config, env_config),
    user_config
)
```

### 3. Logging Integration
- Structured data
- Context propagation
- Performance monitoring
- Error tracking
- Audit trails

```python
from openmatch.utils.logging import get_logger, LoggerAdapter

# Create logger with context
logger = get_logger("app.processing")
context_logger = LoggerAdapter(logger, {
    "service": "data_processor",
    "version": "1.0.0"
})

# Log with additional context
context_logger.info(
    "Processing batch",
    extra={
        "batch_id": "batch123",
        "records": 1000,
        "duration_ms": 150
    }
)
```

## Best Practices

1. **Validation**
   - Define clear schemas
   - Use appropriate validators
   - Handle validation errors
   - Document constraints
   - Test edge cases

2. **Configuration**
   - Use hierarchical configs
   - Validate early
   - Cache appropriately
   - Document options
   - Handle defaults

3. **Logging**
   - Use structured logging
   - Include context
   - Set appropriate levels
   - Monitor performance
   - Rotate log files

## Performance Considerations

1. **Validation Performance**
   - Cache compiled patterns
   - Optimize complex schemas
   - Batch validations
   - Profile validators
   - Handle large datasets

2. **Configuration Performance**
   - Use caching
   - Lazy loading
   - Minimize file I/O
   - Profile access patterns
   - Memory management

3. **Logging Performance**
   - Async logging
   - Buffer management
   - Level filtering
   - Format optimization
   - Resource cleanup

## Common Use Cases

1. **API Request Validation**
   ```python
   from openmatch.utils.validation import validate_dict
   
   request_schema = {
       "id": {"type": str, "required": True},
       "data": {"type": dict, "required": True},
       "timestamp": {"type": str, "date": "%Y-%m-%dT%H:%M:%SZ"}
   }
   
   def process_request(request_data):
       validate_dict(request_data, request_schema)
       # Process validated request
   ```

2. **Multi-Environment Configuration**
   ```python
   from openmatch.utils.config import ConfigManager
   
   config_mgr = ConfigManager()
   
   # Load environment-specific config
   env = os.getenv("APP_ENV", "development")
   config = config_mgr.get_config(f"config.{env}")
   
   # Override with local settings
   local_config = config_mgr.get_config("config.local", required=False)
   if local_config:
       config = config_mgr.merge_configs(config, local_config)
   ```

3. **Audit Logging**
   ```python
   from openmatch.utils.logging import get_logger
   
   audit_logger = get_logger(
       "audit",
       log_file="audit.log",
       json_format=True,
       extra_fields={"app": "data_processor"}
   )
   
   def audit_operation(operation, user, data):
       audit_logger.info(
           f"User {user} performed {operation}",
           extra={
               "operation": operation,
               "user": user,
               "data": data,
               "timestamp": datetime.utcnow().isoformat()
           }
       )
   ``` 