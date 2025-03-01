"""
Django-style settings for OpenMatch MDM system.
All settings can be overridden by creating a local_settings.py file.
"""
import os
from enum import Enum
from typing import Dict, List
from dataclasses import dataclass

# Vector Backend Options
class VectorBackend(Enum):
    PGVECTOR = "pgvector"  # PostgreSQL with pgvector extension
    QDRANT = "qdrant"      # Qdrant vector database
    MILVUS = "milvus"      # Milvus vector database
    FAISS = "faiss"        # In-memory FAISS (fallback)

@dataclass
class DatabaseConfig:
    """Database configuration."""
    engine: str
    host: str
    port: int
    name: str
    user: str
    password: str
    schema: str = 'public'
    min_connections: int = 5
    max_connections: int = 20
    timeout: int = 30
    vector_backend: VectorBackend = VectorBackend.PGVECTOR
    vector_dimension: int = 768
    vector_index_type: str = 'ivfflat'
    vector_lists: int = 100
    vector_probes: int = 10

    @classmethod
    def from_dict(cls, config: Dict) -> 'DatabaseConfig':
        """Create DatabaseConfig from dictionary."""
        return cls(
            engine=config.get('ENGINE', 'postgresql'),
            host=config.get('HOST', 'localhost'),
            port=config.get('PORT', 5432),
            name=config.get('NAME', 'mdm'),
            user=config.get('USER', 'postgres'),
            password=config.get('PASSWORD', ''),
            schema=config.get('SCHEMA', 'public'),
            min_connections=config.get('MIN_CONNECTIONS', 5),
            max_connections=config.get('MAX_CONNECTIONS', 20),
            timeout=config.get('TIMEOUT', 30),
            vector_backend=VectorBackend(config.get('VECTOR_BACKEND', 'pgvector')),
            vector_dimension=config.get('VECTOR_DIMENSION', 768),
            vector_index_type=config.get('VECTOR_INDEX_TYPE', 'ivfflat'),
            vector_lists=config.get('VECTOR_LISTS', 100),
            vector_probes=config.get('VECTOR_PROBES', 10)
        )

# Database Settings
MDM_DB = {
    'ENGINE': 'postgresql',
    'HOST': os.getenv('MDM_DB_HOST', 'localhost'),
    'PORT': int(os.getenv('MDM_DB_PORT', 5432)),
    'NAME': os.getenv('MDM_DB_NAME', 'mdm'),
    'USER': os.getenv('MDM_DB_USER', 'mdm_user'),
    'PASSWORD': os.getenv('MDM_DB_PASSWORD', 'change_me'),
    'SCHEMA': os.getenv('MDM_DB_SCHEMA', 'mdm'),
    'MIN_CONNECTIONS': 5,
    'MAX_CONNECTIONS': 20,
    'TIMEOUT': 30,
}

# Vector Search Settings
VECTOR_SETTINGS = {
    'BACKEND': VectorBackend.PGVECTOR,
    'DIMENSION': 768,  # Default for most transformer models
    'INDEX_TYPE': 'ivfflat',  # Options: ivfflat, hnsw
    'IVF_LISTS': 100,  # Number of lists for IVF index
    'PROBES': 10,  # Number of probes for search
    'SIMILARITY_THRESHOLD': 0.8,
}

# Source Systems Configuration
SOURCE_SYSTEMS = {
    'source1': {
        'ENGINE': 'postgresql',
        'HOST': os.getenv('SOURCE1_DB_HOST', 'localhost'),
        'PORT': int(os.getenv('SOURCE1_DB_PORT', 5432)),
        'NAME': os.getenv('SOURCE1_DB_NAME', 'source1'),
        'USER': os.getenv('SOURCE1_DB_USER', 'source1_user'),
        'PASSWORD': os.getenv('SOURCE1_DB_PASSWORD', 'change_me'),
        'ENTITY_TYPE': 'person',
        'QUERY': 'SELECT * FROM persons WHERE updated_at > :last_sync',
        'BATCH_SIZE': 1000,
        'FIELD_MAPPINGS': {
            'first_name': 'given_name',
            'last_name': 'family_name',
            'birth_date': 'dob',
            'ssn': 'tax_id',
        }
    }
}

# Matching Configuration
MATCH_SETTINGS = {
    'BLOCKING_KEYS': ['first_name', 'last_name', 'birth_date'],
    'MATCH_THRESHOLD': 0.8,
    'MERGE_THRESHOLD': 0.9,
    'MAX_CLUSTER_SIZE': 100,
    'ENABLE_INCREMENTAL': True,
    'CACHE_EMBEDDINGS': True,
}

# Model Settings
MODEL_SETTINGS = {
    'EMBEDDING_MODEL': 'sentence-transformers/all-MiniLM-L6-v2',
    'USE_GPU': False,
    'BATCH_SIZE': 128,
}

# Processing Settings
PROCESSING = {
    'BATCH_SIZE': 10000,
    'MAX_WORKERS': None,  # None = 2 * CPU cores
    'USE_PROCESSES': False,
}

# Logging Configuration
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'mdm.log',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'openmatch': {
            'handlers': ['console', 'file'],
            'level': os.getenv('MDM_LOG_LEVEL', 'INFO'),
            'propagate': True,
        },
    },
}

# Import local settings if they exist
try:
    from .local_settings import *
except ImportError:
    pass

# Validate settings
def validate_settings():
    """Validate critical settings."""
    required_settings = [
        ('MDM_DB', dict),
        ('VECTOR_SETTINGS', dict),
        ('SOURCE_SYSTEMS', dict),
        ('MATCH_SETTINGS', dict),
        ('MODEL_SETTINGS', dict),
    ]
    
    for setting_name, expected_type in required_settings:
        if not globals().get(setting_name):
            raise ValueError(f"Required setting {setting_name} is missing")
        if not isinstance(globals().get(setting_name), expected_type):
            raise TypeError(f"Setting {setting_name} must be a {expected_type.__name__}")

validate_settings() 