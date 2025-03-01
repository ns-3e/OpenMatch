"""
OpenMatch Example Configuration File
This file contains all the necessary settings to run the MDM operations.
"""

import os

# Database Configuration
MDM_DB = {
    'ENGINE': 'postgresql',
    'HOST': 'localhost',
    'PORT': 5432,
    'NAME': 'mdm',
    'USER': os.getenv('MDM_DB_USER', 'postgres'),
    'PASSWORD': os.getenv('MDM_DB_PASSWORD', ''),
    'SCHEMA': 'mdm',
}

# Vector Search Settings
VECTOR_SETTINGS = {
    'DIMENSION': 768,
    'INDEX_TYPE': 'ivfflat',
    'IVF_LISTS': 100,
    'PROBES': 10,
    'SIMILARITY_THRESHOLD': 0.8,
}

# Model Settings
MODEL_SETTINGS = {
    'EMBEDDING_MODEL': 'sentence-transformers/all-MiniLM-L6-v2',
    'USE_GPU': False,
    'BATCH_SIZE': 128,
}

# Processing Settings
PROCESSING = {
    'BATCH_SIZE': 5000,  # Adjust based on your system's memory
    'MAX_WORKERS': None,  # None = 2 * CPU cores
    'USE_PROCESSES': False,
}

# Example source system configuration
SOURCE_SYSTEMS = {
    'sample_source': {
        'ENGINE': 'postgresql',
        'HOST': 'localhost',
        'PORT': 5432,
        'NAME': 'source_db',
        'USER': os.getenv('SOURCE_DB_USER', 'postgres'),
        'PASSWORD': os.getenv('SOURCE_DB_PASSWORD', ''),
        'ENTITY_TYPE': 'person',
        'QUERY': '''
            SELECT 
                id,
                first_name,
                last_name,
                email,
                birth_date,
                updated_at
            FROM persons 
            WHERE updated_at > :last_sync
        ''',
        'FIELD_MAPPINGS': {
            'first_name': 'given_name',
            'last_name': 'family_name',
            'email': 'email_address',
            'birth_date': 'date_of_birth'
        }
    }
}

# Match Settings
MATCH_SETTINGS = {
    'BLOCKING_KEYS': ['first_name', 'last_name', 'birth_date'],
    'MATCH_THRESHOLD': 0.8,
    'MERGE_THRESHOLD': 0.9,
    'WEIGHTS': {
        'first_name': 0.3,
        'last_name': 0.3,
        'email': 0.25,
        'birth_date': 0.15
    }
} 