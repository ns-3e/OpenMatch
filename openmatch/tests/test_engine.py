import pytest
import numpy as np
from openmatch.match import MatchEngine, MatchType

def test_engine_initialization(basic_config):
    """Test engine initialization."""
    engine = MatchEngine(basic_config)
    assert len(engine.rules) == 2  # exact_ssn and fuzzy_name_dob
    assert engine.embedding_model is not None
    assert engine.index is not None

def test_compute_embedding(basic_config):
    """Test embedding computation."""
    engine = MatchEngine(basic_config)
    
    # Test basic embedding
    text = "John Doe"
    embedding = engine.compute_embedding(text)
    assert isinstance(embedding, np.ndarray)
    assert embedding.ndim == 1
    
    # Test caching (should return same object)
    embedding2 = engine.compute_embedding(text)
    assert embedding is embedding2  # Should be same object due to caching

def test_compute_blocking_tensor(basic_config, sample_records):
    """Test blocking tensor computation."""
    engine = MatchEngine(basic_config)
    
    # Test with valid record
    tensor = engine.compute_blocking_tensor(sample_records[0])
    assert isinstance(tensor, np.ndarray)
    assert tensor.ndim == 1
    
    # Test with record missing fuzzy fields
    with pytest.raises(ValueError):
        engine.compute_blocking_tensor({"ssn": "123-45-6789"})

def test_add_and_find_candidates(basic_config, sample_records):
    """Test adding records and finding candidates."""
    engine = MatchEngine(basic_config)
    
    # Add records
    for record in sample_records:
        engine.add_record(record)
    
    # Find candidates for first record
    candidates = engine.find_candidates(sample_records[0], k=2)
    assert len(candidates) == 2
    
    # Candidates should be tuples of (index, distance)
    assert all(isinstance(c, tuple) and len(c) == 2 for c in candidates)
    
    # Distances should be sorted (ascending)
    distances = [d for _, d in candidates]
    assert all(distances[i] <= distances[i+1] for i in range(len(distances)-1))

def test_match_records(basic_config, sample_records):
    """Test record matching."""
    engine = MatchEngine(basic_config)
    
    # Test exact SSN match
    record1 = sample_records[0]
    record2 = record1.copy()
    match_type, confidence = engine.match_records(record1, record2)
    assert match_type == MatchType.EXACT
    assert confidence == 1.0
    
    # Test fuzzy name match with same DOB
    record2 = sample_records[1]  # Jon vs John, same DOB
    match_type, confidence = engine.match_records(record1, record2)
    assert match_type == MatchType.FUZZY
    assert confidence >= 0.8
    
    # Test no match
    record3 = sample_records[2]  # Different name and DOB
    match_type, confidence = engine.match_records(record1, record3)
    assert match_type == MatchType.NO_MATCH or match_type == MatchType.POTENTIAL
    assert confidence < 0.9

def test_process_batch(basic_config, sample_records):
    """Test batch processing."""
    engine = MatchEngine(basic_config)
    
    # Add all records
    for record in sample_records:
        engine.add_record(record)
    
    # Process batch
    matches = engine.process_batch(sample_records, batch_size=2)
    
    # Verify matches structure
    assert isinstance(matches, list)
    for match in matches:
        assert len(match) == 4  # (idx1, idx2, match_type, confidence)
        assert isinstance(match[0], int)
        assert isinstance(match[1], int)
        assert isinstance(match[2], MatchType)
        assert isinstance(match[3], float)
        
        # Verify indices are valid
        assert 0 <= match[0] < len(sample_records)
        assert 0 <= match[1] < len(sample_records)
        
        # Verify no self-matches
        assert match[0] != match[1]
        
        # Verify confidence is valid
        assert 0.0 <= match[3] <= 1.0 