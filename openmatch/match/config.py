"""
Configuration classes for the match engine.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol, Union, Any, Callable
from enum import Enum
import yaml
import re


class ComparisonRule(Protocol):
    """Protocol for record comparison rules."""
    
    def compare(self, record1: Dict, record2: Dict) -> float:
        """Compare two records and return similarity score.
        
        Args:
            record1: First record to compare
            record2: Second record to compare
            
        Returns:
            Similarity score between 0 and 1
        """
        ...


class MatchType(Enum):
    EXACT = "exact"
    FUZZY = "fuzzy"
    PHONETIC = "phonetic"
    NUMERIC = "numeric"
    DATE = "date"
    ADDRESS = "address"
    CONDITIONAL = "conditional"
    SEGMENTED = "segmented"
    CUSTOM = "custom"


class ComparisonOperator(Enum):
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IN = "in"
    NOT_IN = "not_in"


@dataclass
class NullHandling:
    """Configuration for handling null values in matching."""
    match_nulls: bool = False
    null_equality_score: float = 0.0
    require_both_non_null: bool = True
    null_field_score: float = 0.0


@dataclass
class SegmentConfig:
    """Configuration for segmented matching."""
    segment_field: str
    segment_values: Dict[str, float]  # value -> weight
    default_weight: float = 1.0


@dataclass
class ConditionalRule:
    """Configuration for conditional matching."""
    condition_field: str
    operator: ComparisonOperator
    value: Any
    match_config: 'FieldMatchConfig'


@dataclass
class CustomMatchConfig:
    """Configuration for custom matching functions."""
    function_name: str
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FieldMatchConfig:
    """Configuration for field-level matching."""
    match_type: MatchType
    weight: float = 1.0
    threshold: float = 0.0
    null_handling: NullHandling = field(default_factory=NullHandling)
    
    # Type-specific configurations
    fuzzy_params: Dict[str, Any] = field(default_factory=dict)
    phonetic_algorithm: Optional[str] = None
    date_format: Optional[str] = None
    numeric_tolerance: Optional[float] = None
    
    # Advanced matching configurations
    conditional_rules: List[ConditionalRule] = field(default_factory=list)
    segment_config: Optional[SegmentConfig] = None
    custom_config: Optional[CustomMatchConfig] = None
    
    # Preprocessing
    preprocessors: List[Union[str, Callable]] = field(default_factory=list)
    
    def __post_init__(self):
        if self.match_type == MatchType.FUZZY and not self.fuzzy_params:
            self.fuzzy_params = {
                "method": "levenshtein",
                "threshold": 0.8
            }


@dataclass
class BlockingConfig:
    """Configuration for blocking strategies."""
    blocking_keys: List[str]
    method: str = "standard"  # standard, lsh, sorted_neighborhood
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.method == "lsh" and "num_bands" not in self.parameters:
            self.parameters["num_bands"] = 20
        elif self.method == "sorted_neighborhood" and "window_size" not in self.parameters:
            self.parameters["window_size"] = 3


@dataclass
class MatchConfig:
    """Main configuration for the matching engine."""
    # Field configurations
    field_configs: Dict[str, FieldMatchConfig]
    
    # Blocking configuration
    blocking: Optional[BlockingConfig] = None
    
    # Overall matching parameters
    min_overall_score: float = 0.0
    score_aggregation: str = "weighted_average"  # weighted_average, min, max
    
    # Advanced options
    enable_caching: bool = True
    cache_size: int = 10000
    parallel_processing: bool = True
    num_workers: int = -1  # -1 means use all available cores
    
    def get_field_weight(self, field_name: str) -> float:
        """Get the weight for a field."""
        if field_name not in self.field_configs:
            return 0.0
        return self.field_configs[field_name].weight
    
    def get_total_weight(self) -> float:
        """Get the total weight of all fields."""
        return sum(config.weight for config in self.field_configs.values())
    
    def validate(self) -> List[str]:
        """Validate the configuration."""
        errors = []
        
        # Check field configs
        if not self.field_configs:
            errors.append("No field configurations provided")
            
        # Validate weights
        total_weight = self.get_total_weight()
        if total_weight == 0:
            errors.append("Total field weight cannot be zero")
            
        # Validate thresholds
        if not (0 <= self.min_overall_score <= 1):
            errors.append("min_overall_score must be between 0 and 1")
            
        # Validate blocking config
        if self.blocking:
            if not self.blocking.blocking_keys:
                errors.append("Blocking keys must be specified when blocking is enabled")
                
        return errors

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'MatchConfig':
        """Create a MatchConfig instance from a dictionary."""
        field_configs = {}
        for field_name, field_config in config_dict.get("field_configs", {}).items():
            field_configs[field_name] = FieldMatchConfig(**field_config)
            
        blocking = None
        if "blocking" in config_dict:
            blocking = BlockingConfig(**config_dict["blocking"])
            
        return cls(
            field_configs=field_configs,
            blocking=blocking,
            min_overall_score=config_dict.get("min_overall_score", 0.0),
            score_aggregation=config_dict.get("score_aggregation", "weighted_average"),
            enable_caching=config_dict.get("enable_caching", True),
            cache_size=config_dict.get("cache_size", 10000),
            parallel_processing=config_dict.get("parallel_processing", True),
            num_workers=config_dict.get("num_workers", -1)
        )


# Common comparison rules
class ExactMatchRule:
    """Rule for exact string matching."""
    
    def __init__(self, field: str):
        self.field = field
        
    def compare(self, record1: Dict, record2: Dict) -> float:
        """Compare field values exactly."""
        val1 = str(record1.get(self.field, "")).lower()
        val2 = str(record2.get(self.field, "")).lower()
        return float(val1 == val2)


class FuzzyMatchRule:
    """Rule for fuzzy string matching."""
    
    def __init__(
        self,
        field: str,
        threshold: float = 0.8,
        method: str = "levenshtein"
    ):
        self.field = field
        self.threshold = threshold
        self.method = method
        
    def compare(self, record1: Dict, record2: Dict) -> float:
        """Compare field values using fuzzy matching."""
        from Levenshtein import ratio
        
        val1 = str(record1.get(self.field, "")).lower()
        val2 = str(record2.get(self.field, "")).lower()
        
        if self.method == "levenshtein":
            score = ratio(val1, val2)
        else:
            raise ValueError(f"Unknown fuzzy match method: {self.method}")
            
        return float(score >= self.threshold)


class PhoneticMatchRule:
    """Rule for phonetic matching."""
    
    def __init__(self, field: str, method: str = "soundex"):
        self.field = field
        self.method = method
        
    def compare(self, record1: Dict, record2: Dict) -> float:
        """Compare field values using phonetic matching."""
        import jellyfish
        
        val1 = str(record1.get(self.field, "")).lower()
        val2 = str(record2.get(self.field, "")).lower()
        
        if self.method == "soundex":
            return float(
                jellyfish.soundex(val1) == jellyfish.soundex(val2)
            )
        elif self.method == "metaphone":
            return float(
                jellyfish.metaphone(val1) == jellyfish.metaphone(val2)
            )
        else:
            raise ValueError(f"Unknown phonetic method: {self.method}")


class AddressMatchRule:
    """Rule for address matching."""
    
    def __init__(self, field: str, threshold: float = 0.8):
        self.field = field
        self.threshold = threshold
        
    def compare(self, record1: Dict, record2: Dict) -> float:
        """Compare addresses using specialized matching."""
        # TODO: Implement proper address parsing and comparison
        # For now, use a simple fuzzy match
        return FuzzyMatchRule(
            self.field,
            threshold=self.threshold
        ).compare(record1, record2)
