import pytest
from openmatch.match import (
    MatchConfig,
    BlockingConfig,
    MetadataConfig,
    create_exact_ssn_rule,
    create_fuzzy_name_dob_rule
)

@pytest.fixture
def sample_records():
    """Sample records for testing."""
    return [
        {
            "first_name": "John",
            "last_name": "Doe",
            "dob": "1990-01-01",
            "ssn": "123-45-6789"
        },
        {
            "first_name": "Jon",  # Slightly different first name
            "last_name": "Doe",
            "dob": "1990-01-01",
            "ssn": "987-65-4321"
        },
        {
            "first_name": "Jane",
            "last_name": "Doe",
            "dob": "1992-03-15",
            "ssn": "456-78-9012"
        }
    ]

@pytest.fixture
def basic_config():
    """Basic configuration for testing."""
    return MatchConfig(
        blocking=BlockingConfig(
            blocking_keys=["last_name"],
            block_size_limit=10,
            embedding_model="sentence-transformers/all-MiniLM-L6-v2"
        ),
        rules=[
            create_exact_ssn_rule(),
            create_fuzzy_name_dob_rule()
        ],
        metadata=MetadataConfig(),
        use_gpu=False,
        batch_size=2
    ) 