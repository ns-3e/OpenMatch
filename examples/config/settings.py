"""
OpenMatch Example Configuration File
This file contains all the necessary settings to run the MDM operations.
"""

import os
from openmatch.model.config import DataType, RelationType, FieldConfig, RelationshipConfig, EntityConfig

# Database Configuration
MDM_DB = {
    'ENGINE': 'postgresql',
    'HOST': 'localhost',
    'PORT': 5432,
    'NAME': 'mdm',
    'USER': os.getenv('MDM_DB_USER', 'postgres'),
    'PASSWORD': os.getenv('MDM_DB_PASSWORD', 'postgres'),
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

# Embedding Model Settings
MODEL_SETTINGS = {
    'EMBEDDING_MODEL': 'sentence-transformers/all-MiniLM-L6-v2',
    'USE_GPU': False,
    'BATCH_SIZE': 128,
}

# Processing Settings
PROCESSING = {
    'BATCH_SIZE': 10000,  # Adjust based on your system's memory
    'MAX_WORKERS': None,  # None = 2 * CPU cores
    'USE_PROCESSES': False,
}

# MDM Data Model Configuration
MDM_MODEL = {
    'entities': {
        'person': EntityConfig(
            name='person',
            description='Master person record',
            fields=[
                FieldConfig(name='id', data_type=DataType.STRING, primary_key=True),
                FieldConfig(name='first_name', data_type=DataType.STRING, required=True),
                FieldConfig(name='last_name', data_type=DataType.STRING, required=True),
                FieldConfig(name='birth_date', data_type=DataType.DATE),
                FieldConfig(name='email', data_type=DataType.STRING),
                FieldConfig(name='created_at', data_type=DataType.DATETIME, required=True),
                FieldConfig(name='updated_at', data_type=DataType.DATETIME, required=True)
            ],
            relationships=[
                RelationshipConfig(
                    name='addresses',
                    source_entity='person',
                    target_entity='address',
                    relation_type=RelationType.ONE_TO_MANY,
                    source_field='id',
                    target_field='person_id'
                ),
                RelationshipConfig(
                    name='phone_numbers',
                    source_entity='person',
                    target_entity='phone',
                    relation_type=RelationType.ONE_TO_MANY,
                    source_field='id',
                    target_field='person_id'
                ),
                RelationshipConfig(
                    name='email_addresses',
                    source_entity='person',
                    target_entity='email',
                    relation_type=RelationType.ONE_TO_MANY,
                    source_field='id',
                    target_field='person_id'
                ),
                
            ]
        ),
        'address': EntityConfig(
            name='address',
            description='Physical address information',
            fields=[
                FieldConfig(name='id', data_type=DataType.STRING, primary_key=True),
                FieldConfig(name='person_id', data_type=DataType.STRING, required=True, foreign_key='person.id'),
                FieldConfig(name='address_type', data_type=DataType.STRING),
                FieldConfig(name='street_1', data_type=DataType.STRING, required=True),
                FieldConfig(name='street_2', data_type=DataType.STRING),
                FieldConfig(name='city', data_type=DataType.STRING, required=True),
                FieldConfig(name='state', data_type=DataType.STRING, required=True),
                FieldConfig(name='postal_code', data_type=DataType.STRING, required=True),
                FieldConfig(name='country', data_type=DataType.STRING, required=True),
                FieldConfig(name='created_at', data_type=DataType.DATETIME, required=True),
                FieldConfig(name='updated_at', data_type=DataType.DATETIME, required=True)
            ]
        ),
        'phone': EntityConfig(
            name='phone',
            description='Phone contact information',
            fields=[
                FieldConfig(name='id', data_type=DataType.STRING, primary_key=True),
                FieldConfig(name='person_id', data_type=DataType.STRING, required=True, foreign_key='person.id'),
                FieldConfig(name='phone_type', data_type=DataType.STRING),
                FieldConfig(name='phone_number', data_type=DataType.STRING, required=True),
                FieldConfig(name='country_code', data_type=DataType.STRING),
                FieldConfig(name='created_at', data_type=DataType.DATETIME, required=True),
                FieldConfig(name='updated_at', data_type=DataType.DATETIME, required=True)
            ]
        ),
        'email': EntityConfig(
            name='email',
            description='Email contact information',
            fields=[
                FieldConfig(name='id', data_type=DataType.STRING, primary_key=True),
                FieldConfig(name='person_id', data_type=DataType.STRING, required=True, foreign_key='person.id'),
                FieldConfig(name='email_type', data_type=DataType.STRING),
                FieldConfig(name='email_address', data_type=DataType.STRING, required=True),
                FieldConfig(name='created_at', data_type=DataType.DATETIME, required=True),
                FieldConfig(name='updated_at', data_type=DataType.DATETIME, required=True)
            ]
        ),
    }
}

# Source System Configuration with Field Mappings
SOURCE_SYSTEMS = {
    'crm_system': {
        'ENGINE': 'postgresql',
        'HOST': 'localhost',
        'PORT': 5432,
        'NAME': 'source_db',
        'USER': os.getenv('SOURCE_DB_USER', 'postgres'),
        'PASSWORD': os.getenv('SOURCE_DB_PASSWORD', ''),
        'ENTITY_MAPPINGS': {
            'person': {
                'table': 'customers',
                'query': '''
                    SELECT 
                        customer_id as id,
                        given_name as first_name,
                        family_name as last_name,
                        dob as birth_date,
                        email_address as email,
                        created_timestamp as created_at,
                        modified_timestamp as updated_at
                    FROM customers 
                    WHERE modified_timestamp > :last_sync
                    AND source_system = :source_system
                ''',
                'child_entities': {
                    'address': {
                        'table': 'customer_addresses',
                        'query': '''
                            SELECT 
                                address_id as id,
                                customer_id as person_id,
                                address_type,
                                address_line1 as street_1,
                                address_line2 as street_2,
                                city,
                                state_province as state,
                                postal_code,
                                country,
                                created_at,
                                updated_at
                            FROM customer_addresses
                            WHERE customer_id = :parent_id
                            AND source_system = :source_system
                        '''
                    },
                    'phone': {
                        'table': 'customer_phones',
                        'query': '''
                            SELECT 
                                phone_id as id,
                                customer_id as person_id,
                                phone_type,
                                phone_number,
                                country_code,
                                created_at,
                                updated_at
                            FROM customer_phones
                            WHERE customer_id = :parent_id
                            AND source_system = :source_system
                        '''
                    },
                    'email': {
                        'table': 'customer_emails',
                        'query': '''
                            SELECT 
                                email_id as id,
                                customer_id as person_id,
                                email_type,
                                email_address,
                                created_at,
                                updated_at
                            FROM customer_emails
                            WHERE customer_id = :parent_id
                            AND source_system = :source_system
                        '''
                    },
                }
            }
        }
    },
    'erp_system': {
        'ENGINE': 'postgresql',
        'HOST': 'localhost',
        'PORT': 5432,
        'NAME': 'erp_db',
        'USER': os.getenv('ERP_DB_USER', 'postgres'),
        'PASSWORD': os.getenv('ERP_DB_PASSWORD', ''),
        'ENTITY_MAPPINGS': {
            'person': {
                'table': 'customers',
                'query': '''
                    SELECT 
                        customer_id as id,
                        given_name as first_name,
                        family_name as last_name,
                        dob as birth_date,
                        email_address as email,
                        created_timestamp as created_at,
                        modified_timestamp as updated_at
                    FROM customers
                    WHERE modified_timestamp > :last_sync
                    AND source_system = :source_system
                ''',
                'child_entities': {
                    'address': {
                        'table': 'customer_addresses',
                        'query': '''
                            SELECT 
                                address_id as id,
                                customer_id as person_id,
                                address_type,
                                address_line1 as street_1,
                                address_line2 as street_2,
                                city,
                                state_province as state,
                                postal_code,
                                country,
                                created_at,
                                updated_at
                            FROM customer_addresses
                            WHERE customer_id = :parent_id
                            AND source_system = :source_system
                        ''' 
                    },
                    'phone': {
                        'table': 'customer_phones',
                        'query': '''
                            SELECT 
                                phone_id as id,
                                customer_id as person_id,
                                phone_type,
                                phone_number,
                                country_code,
                                created_at,
                                updated_at
                            FROM customer_phones
                            WHERE customer_id = :parent_id
                            AND source_system = :source_system
                        '''
                    },
                    'email': {
                        'table': 'customer_emails',
                        'query': '''
                            SELECT 
                                email_id as id,
                                customer_id as person_id,
                                email_type,
                                email_address,
                                created_at,
                                updated_at
                            FROM customer_emails
                            WHERE customer_id = :parent_id
                            AND source_system = :source_system
                        '''
                    }
                }
            }
        }
    }
}


# Match Settings
MATCH_SETTINGS = {
    'BLOCKING_KEYS': ['first_name', 'last_name', 'birth_date'],  # Initial blocking for performance
    'MATCH_THRESHOLD': 0.8,  # Global minimum match threshold
    'MERGE_THRESHOLD': 0.9,  # Threshold for automatic merging
    'RULES': [
        {
            'rule_id': 'exact_email_match',
            'name': 'Exact Email Match',
            'fields': [
                {
                    'name': 'email',
                    'match_type': 'EXACT',
                    'weight': 1.0,
                    'required': True
                }
            ],
            'min_confidence': 1.0
        },
        {
            'rule_id': 'fuzzy_name_exact_phone',
            'name': 'Fuzzy Name with Exact Phone',
            'fields': [
                {
                    'name': 'first_name',
                    'match_type': 'FUZZY',
                    'weight': 0.3,
                    'threshold': 0.8,
                    'fuzzy_method': 'jaro_winkler'
                },
                {
                    'name': 'last_name',
                    'match_type': 'FUZZY',
                    'weight': 0.3,
                    'threshold': 0.8,
                    'fuzzy_method': 'jaro_winkler'
                },
                {
                    'name': 'phone_number',
                    'match_type': 'EXACT',
                    'weight': 0.4,
                    'required': True
                }
            ],
            'min_confidence': 0.85
        },
        {
            'rule_id': 'name_dob_address',
            'name': 'Name, DOB and Address Match',
            'fields': [
                {
                    'name': 'first_name',
                    'match_type': 'FUZZY',
                    'weight': 0.25,
                    'threshold': 0.7,
                    'fuzzy_method': 'jaro_winkler'
                },
                {
                    'name': 'last_name',
                    'match_type': 'FUZZY',
                    'weight': 0.25,
                    'threshold': 0.7,
                    'fuzzy_method': 'jaro_winkler'
                },
                {
                    'name': 'birth_date',
                    'match_type': 'EXACT',
                    'weight': 0.3,
                    'required': True
                },
                {
                    'name': 'street_1',
                    'match_type': 'FUZZY',
                    'weight': 0.2,
                    'threshold': 0.8,
                    'fuzzy_method': 'ratio'
                }
            ],
            'min_confidence': 0.8
        },
        {
            'rule_id': 'embedding_name_address',
            'name': 'Semantic Name and Address Match',
            'fields': [
                {
                    'name': 'first_name',
                    'match_type': 'EMBEDDING',
                    'weight': 0.3,
                    'threshold': 0.8,
                    'embedding_model': 'sentence-transformers/all-MiniLM-L6-v2'
                },
                {
                    'name': 'last_name',
                    'match_type': 'EMBEDDING',
                    'weight': 0.3,
                    'threshold': 0.8,
                    'embedding_model': 'sentence-transformers/all-MiniLM-L6-v2'
                },
                {
                    'name': 'street_1',
                    'match_type': 'EMBEDDING',
                    'weight': 0.4,
                    'threshold': 0.8,
                    'embedding_model': 'sentence-transformers/all-MiniLM-L6-v2'
                }
            ],
            'min_confidence': 0.85
        }
    ]
} 