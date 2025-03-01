from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
from enum import Enum

class MatchType(Enum):
    EXACT = "EXACT"
    FUZZY = "FUZZY"
    EMBEDDING = "EMBEDDING"
    POTENTIAL = "POTENTIAL"
    NO_MATCH = "NO_MATCH"
    ERROR = "ERROR"

@dataclass
class BlockingConfig:
    """Configuration for blocking strategy."""
    blocking_keys: List[str]
    block_size_limit: int = 1000
    embedding_model: str = "Salesforce/SFR-Embedding-2_R"
    vector_similarity_threshold: float = 0.8

@dataclass
class FieldConfig:
    """Configuration for individual field matching."""
    name: str
    match_type: MatchType
    weight: float = 1.0
    threshold: float = 0.8
    required: bool = False
    fuzzy_method: Optional[str] = None  # e.g., "levenshtein", "jaro_winkler"
    embedding_model: Optional[str] = "Salesforce/SFR-Embedding-2_R"

@dataclass
class MatchRuleConfig:
    """Configuration for a single match rule."""
    name: str
    rule_id: str
    fields: List[FieldConfig]
    min_confidence: float = 0.8
    blocking_fields: Optional[List[str]] = None
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.fields:
            raise ValueError("At least one field must be defined")
            
        total_weight = sum(field.weight for field in self.fields)
        if not (0.99 <= total_weight <= 1.01):  # Allow for small floating-point differences
            raise ValueError(f"Field weights must sum to 1.0 (got {total_weight})")
            
        # Validate individual weights
        for field in self.fields:
            if field.weight <= 0 or field.weight > 1:
                raise ValueError(f"Field weight must be between 0 and 1 (got {field.weight} for {field.name})")
                
        # Validate field names are unique
        field_names = [field.name for field in self.fields]
        if len(field_names) != len(set(field_names)):
            raise ValueError("Field names must be unique")

@dataclass
class MetadataConfig:
    """Configuration for database metadata."""
    master_table: str = "master_records"
    xref_table: str = "cross_references"
    results_table: str = "match_results"
    stats_table: str = "match_statistics"
    schema: str = "public"
    vector_column: str = "embedding"

@dataclass
class MatchConfig:
    """Main configuration combining blocking and matching rules."""
    blocking: BlockingConfig
    rules: List[MatchRuleConfig]
    metadata: MetadataConfig
    use_gpu: bool = False
    cache_size: int = 1000
    batch_size: int = 100
    expected_records: Optional[int] = None
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_config()
    
    def _validate_config(self):
        """Validate the configuration settings."""
        if not self.rules:
            raise ValueError("At least one match rule must be defined")
        
        for rule in self.rules:
            if not rule.fields:
                raise ValueError(f"Match rule '{rule.name}' must have at least one field")
            
            weights_sum = sum(field.weight for field in rule.fields)
            if not (0.99 <= weights_sum <= 1.01):  # Allow for small floating-point differences
                raise ValueError(f"Field weights in rule '{rule.name}' must sum to 1.0")
