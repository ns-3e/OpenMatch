import pytest
import json
import requests
from datetime import datetime
from fastapi.testclient import TestClient

from openmatch.api import app
from openmatch.config import TrustConfig, SurvivorshipRules, MatchConfig

@pytest.fixture
def api_client():
    """Fixture providing a FastAPI test client."""
    return TestClient(app)

@pytest.fixture
def api_config():
    """Fixture providing API configuration."""
    return {
        "match_config": {
            "blocking_keys": ["name_prefix"],
            "comparison_fields": [
                ["name", "fuzzy", 0.4],
                ["address", "address_similarity", 0.4],
                ["phone", "exact", 0.2]
            ],
            "min_overall_score": 0.8
        },
        "trust_config": {
            "source_reliability": {
                "CRM": 0.9,
                "ERP": 0.8,
                "LEGACY": 0.6
            }
        },
        "survivorship_rules": {
            "priority_fields": {
                "name": ["CRM", "ERP", "LEGACY"],
                "address": ["ERP", "CRM", "LEGACY"],
                "phone": ["CRM", "ERP", "LEGACY"]
            }
        }
    }

def test_health_check(api_client):
    """Test the health check endpoint."""
    response = api_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_api_configuration(api_client, api_config):
    """Test setting and retrieving API configuration."""
    # Set configuration
    response = api_client.post(
        "/config",
        json=api_config
    )
    assert response.status_code == 200
    
    # Get configuration
    response = api_client.get("/config")
    assert response.status_code == 200
    assert response.json() == api_config

def test_process_records_endpoint(api_client, api_config, sample_records):
    """Test the record processing endpoint."""
    # Set configuration first
    api_client.post("/config", json=api_config)
    
    # Process records
    response = api_client.post(
        "/process",
        json={"records": sample_records}
    )
    
    assert response.status_code == 200
    result = response.json()
    
    assert "golden_records" in result
    assert "matches" in result
    assert "xrefs" in result
    assert len(result["golden_records"]) > 0

def test_incremental_processing_endpoint(api_client, api_config):
    """Test incremental processing through the API."""
    # Set configuration
    api_client.post("/config", json=api_config)
    
    # Initial records
    initial_records = [
        {
            "id": "CRM_001",
            "source": "CRM",
            "name": "Acme Corp",
            "address": "123 Main St",
            "phone": "555-0101",
            "last_updated": "2024-02-25"
        },
        {
            "id": "ERP_101",
            "source": "ERP",
            "name": "ACME Corporation",
            "address": "123 Main Street",
            "phone": "555-0101",
            "last_updated": "2024-02-24"
        }
    ]
    
    # Process initial records
    initial_response = api_client.post(
        "/process",
        json={"records": initial_records}
    )
    assert initial_response.status_code == 200
    initial_result = initial_response.json()
    
    # New records
    new_records = [
        {
            "id": "CRM_002",
            "source": "CRM",
            "name": "Acme Corp",
            "address": "456 New St",
            "phone": "555-0102",
            "last_updated": "2024-02-26"
        }
    ]
    
    # Process incrementally
    incremental_response = api_client.post(
        "/process/incremental",
        json={
            "records": new_records,
            "existing_golden_records": initial_result["golden_records"],
            "existing_xrefs": initial_result["xrefs"]
        }
    )
    
    assert incremental_response.status_code == 200
    incremental_result = incremental_response.json()
    
    assert len(incremental_result["golden_records"]) >= len(initial_result["golden_records"])
    assert len(incremental_result["xrefs"]) > len(initial_result["xrefs"])

def test_batch_processing_endpoint(api_client, api_config):
    """Test batch processing through the API."""
    from faker import Faker
    fake = Faker()
    
    # Generate 1000 test records
    records = [
        {
            "id": f"TEST_{i}",
            "source": "CRM" if i % 2 == 0 else "ERP",
            "name": fake.company(),
            "address": fake.address(),
            "phone": fake.phone_number(),
            "last_updated": fake.date_between(
                start_date="-1y",
                end_date="today"
            ).strftime("%Y-%m-%d")
        }
        for i in range(1000)
    ]
    
    # Set configuration
    api_client.post("/config", json=api_config)
    
    # Process in batch
    response = api_client.post(
        "/process/batch",
        json={
            "records": records,
            "batch_size": 100
        }
    )
    
    assert response.status_code == 200
    result = response.json()
    
    assert "golden_records" in result
    assert "processing_stats" in result
    assert result["processing_stats"]["total_records"] == 1000
    assert result["processing_stats"]["total_batches"] == 10

def test_error_handling(api_client, api_config):
    """Test API error handling."""
    # Set configuration
    api_client.post("/config", json=api_config)
    
    # Test with invalid records
    invalid_records = [
        {
            "id": "CRM_001",
            "source": "CRM",
            "name": "Acme Corp",
            "last_updated": "invalid-date"
        }
    ]
    
    response = api_client.post(
        "/process",
        json={"records": invalid_records}
    )
    
    assert response.status_code == 400
    assert "error" in response.json()
    
    # Test with invalid configuration
    invalid_config = {
        "match_config": {
            "blocking_keys": [],  # Invalid: empty blocking keys
            "comparison_fields": []
        }
    }
    
    response = api_client.post(
        "/config",
        json=invalid_config
    )
    
    assert response.status_code == 400
    assert "error" in response.json()

def test_api_authentication(api_client):
    """Test API authentication."""
    # Test without API key
    response = api_client.post("/process", json={"records": []})
    assert response.status_code == 401
    
    # Test with invalid API key
    headers = {"X-API-Key": "invalid-key"}
    response = api_client.post(
        "/process",
        json={"records": []},
        headers=headers
    )
    assert response.status_code == 401
    
    # Test with valid API key
    headers = {"X-API-Key": "test-api-key"}  # Configure in test environment
    response = api_client.get("/health", headers=headers)
    assert response.status_code == 200

def test_api_rate_limiting(api_client, api_config):
    """Test API rate limiting."""
    headers = {"X-API-Key": "test-api-key"}
    
    # Make multiple rapid requests
    for _ in range(10):
        response = api_client.get("/health", headers=headers)
    
    # Next request should be rate limited
    response = api_client.get("/health", headers=headers)
    assert response.status_code == 429
    assert "error" in response.json()
    assert "rate limit exceeded" in response.json()["error"].lower()

def test_api_metrics(api_client, api_config, sample_records):
    """Test API metrics endpoint."""
    # Set configuration
    api_client.post("/config", json=api_config)
    
    # Process some records
    api_client.post(
        "/process",
        json={"records": sample_records}
    )
    
    # Get metrics
    response = api_client.get("/metrics")
    assert response.status_code == 200
    metrics = response.json()
    
    assert "total_records_processed" in metrics
    assert "average_processing_time" in metrics
    assert "match_rate" in metrics
    assert "error_rate" in metrics 