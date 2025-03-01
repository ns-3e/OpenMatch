"""
OpenMatch Example Configuration File
This file contains all the necessary settings to run the MDM operations.
"""

import os
from openmatch.model.config import DataType, RelationType, FieldConfig, RelationshipConfig, EntityConfig, DataModelConfig
from openmatch.match.config import MatchType, MatchRuleConfig, FieldConfig as MatchFieldConfig

# Database Configuration
MDM_DB = {
    'NAME': 'mdm_db',
    'USER': 'postgres',
    'PASSWORD': 'postgres',
    'HOST': 'localhost',
    'PORT': 5432,
    'SCHEMA': 'mdm',
}

# Vector Search Settings
VECTOR_SETTINGS = {
    'BLOCKING_KEYS': ['first_name', 'last_name', 'email'],
    'BLOCK_SIZE_LIMIT': 1000,
    'SIMILARITY_THRESHOLD': 0.8,
    'USE_FAISS_FALLBACK': True,  # Use FAISS if pgvector is not available
    'INDEX_TYPE': 'IVF100,Flat',  # FAISS index type
    'METRIC': 'cosine'
}

# Embedding Model Settings
MODEL_SETTINGS = {
    'USE_GPU': False,
    'BATCH_SIZE': 32,
    'MAX_LENGTH': 128,
    'MODEL_NAME': 'paraphrase-MiniLM-L3-v2',  # Default lightweight model
    'CACHE_DIR': '.cache/models'
}

# Processing Settings
PROCESSING = {
    'BATCH_SIZE': 1000,
    'MAX_WORKERS': 4,
    'TIMEOUT': 3600,  # 1 hour
    'RETRY_COUNT': 3
}

# MDM Data Model Configuration
MDM_MODEL = DataModelConfig(
    entities=[
        EntityConfig(
            name='person',
            fields=[
                'first_name',
                'last_name',
                'email',
                'birth_date'
            ],
            required_fields=['first_name', 'last_name'],
            unique_fields=['email']
        ),
        EntityConfig(
            name='address',
            fields=[
                'type',
                'street_1',
                'street_2',
                'city',
                'state',
                'postal_code',
                'country'
            ],
            required_fields=['street_1', 'city', 'country']
        ),
        EntityConfig(
            name='phone',
            fields=[
                'type',
                'phone_number',
                'country_code'
            ],
            required_fields=['phone_number']
        )
    ],
    relationships=[
        RelationshipConfig(
            name='person_addresses',
            from_entity='person',
            to_entity='address',
            type='one_to_many'
        ),
        RelationshipConfig(
            name='person_phones',
            from_entity='person',
            to_entity='phone',
            type='one_to_many'
        )
    ]
)

# Source System Configuration with Field Mappings
SOURCE_SYSTEMS = {
    'EXAMPLE_SYSTEM': {
        'name': 'Example System',
        'type': 'json',
        'batch_size': 1000,
        'enabled': True
    }
}

# Match Settings
MATCH_SETTINGS = {
    'RULES': [
        MatchRuleConfig(
            id='exact_match',
            name='Exact Match Rule',
            fields=[
                FieldConfig(name='email', weight=1.0, match_type=MatchType.EXACT)
            ],
            threshold=1.0,
            priority=1
        ),
        MatchRuleConfig(
            id='fuzzy_name_match',
            name='Fuzzy Name Match Rule',
            fields=[
                FieldConfig(name='first_name', weight=0.4, match_type=MatchType.FUZZY),
                FieldConfig(name='last_name', weight=0.4, match_type=MatchType.FUZZY),
                FieldConfig(name='birth_date', weight=0.2, match_type=MatchType.EXACT)
            ],
            threshold=0.8,
            priority=2
        )
    ]
}

# Logging Configuration
import logging

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'DEBUG',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'mdm.log',
            'formatter': 'standard',
            'level': 'INFO',
        }
    },
    'loggers': {
        'openmatch': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': True
        }
    }
} 