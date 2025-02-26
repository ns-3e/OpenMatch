"""
Configuration classes for the trust framework.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Union
from enum import Enum
from datetime import datetime, timedelta
import yaml


class QualityDimension(Enum):
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"


class SurvivorshipStrategy(Enum):
    MOST_RECENT = "most_recent"
    MOST_COMPLETE = "most_complete"
    MOST_TRUSTED = "most_trusted"
    LONGEST = "longest"
    SHORTEST = "shortest"
    CUSTOM = "custom"


@dataclass
class FieldValidation:
    """Configuration for field-level validation."""
    required: bool = False
    data_type: str = "string"
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allowed_values: Optional[List[Any]] = None
    custom_validator: Optional[Callable[[Any], bool]] = None


@dataclass
class SourceConfig:
    """Configuration for data source."""
    name: str
    reliability_score: float
    priority: int
    update_frequency: Optional[timedelta] = None
    last_update_field: Optional[str] = None
    field_mappings: Dict[str, str] = field(default_factory=dict)
    validation_rules: Dict[str, FieldValidation] = field(default_factory=dict)


@dataclass
class QualityConfig:
    """Configuration for data quality scoring."""
    dimensions: Dict[QualityDimension, float] = field(default_factory=dict)
    field_weights: Dict[str, float] = field(default_factory=dict)
    completeness_rules: Dict[str, float] = field(default_factory=dict)
    validity_rules: Dict[str, FieldValidation] = field(default_factory=dict)
    consistency_rules: List[Dict[str, Any]] = field(default_factory=list)
    timeliness_decay: Optional[timedelta] = None
    custom_metrics: Dict[str, Callable[[Dict[str, Any]], float]] = field(default_factory=dict)

    def __post_init__(self):
        # Set default dimension weights if not provided
        if not self.dimensions:
            self.dimensions = {
                QualityDimension.COMPLETENESS: 0.3,
                QualityDimension.ACCURACY: 0.2,
                QualityDimension.CONSISTENCY: 0.2,
                QualityDimension.TIMELINESS: 0.15,
                QualityDimension.UNIQUENESS: 0.1,
                QualityDimension.VALIDITY: 0.05
            }


@dataclass
class SurvivorshipConfig:
    """Configuration for survivorship rules."""
    default_strategy: SurvivorshipStrategy = SurvivorshipStrategy.MOST_TRUSTED
    field_strategies: Dict[str, SurvivorshipStrategy] = field(default_factory=dict)
    custom_strategies: Dict[str, Callable[[List[Dict[str, Any]]], Any]] = field(default_factory=dict)
    source_priority: Dict[str, int] = field(default_factory=dict)
    timestamp_field: str = "last_updated"
    trust_threshold: float = 0.0
    conflict_resolution: str = "highest_trust"  # highest_trust, most_recent, custom
    custom_resolution: Optional[Callable[[List[Any]], Any]] = None


@dataclass
class TrustConfig:
    """Main configuration for the trust framework."""
    # Source configurations
    sources: Dict[str, SourceConfig]
    
    # Quality scoring configuration
    quality: QualityConfig
    
    # Survivorship configuration
    survivorship: SurvivorshipConfig
    
    # Global settings
    enable_caching: bool = True
    cache_size: int = 10000
    min_trust_score: float = 0.0
    trust_score_weights: Dict[str, float] = field(default_factory=lambda: {
        "source_reliability": 0.4,
        "data_quality": 0.6
    })
    
    def validate(self) -> List[str]:
        """Validate the configuration."""
        errors = []
        
        # Validate sources
        if not self.sources:
            errors.append("No source configurations provided")
        
        # Validate quality config
        if sum(self.quality.dimensions.values()) != 1.0:
            errors.append("Quality dimension weights must sum to 1.0")
            
        # Validate survivorship config
        if self.survivorship.conflict_resolution == "custom" and not self.survivorship.custom_resolution:
            errors.append("Custom conflict resolution requires custom_resolution function")
            
        # Validate trust score weights
        if sum(self.trust_score_weights.values()) != 1.0:
            errors.append("Trust score weights must sum to 1.0")
            
        return errors

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'TrustConfig':
        """Create a TrustConfig instance from a dictionary."""
        # Convert source configs
        sources = {
            name: SourceConfig(**source_config)
            for name, source_config in config_dict.get("sources", {}).items()
        }
        
        # Convert quality config
        quality_dict = config_dict.get("quality", {})
        quality_dict["dimensions"] = {
            QualityDimension(k): v
            for k, v in quality_dict.get("dimensions", {}).items()
        }
        quality = QualityConfig(**quality_dict)
        
        # Convert survivorship config
        survivorship_dict = config_dict.get("survivorship", {})
        if "default_strategy" in survivorship_dict:
            survivorship_dict["default_strategy"] = SurvivorshipStrategy(
                survivorship_dict["default_strategy"]
            )
        if "field_strategies" in survivorship_dict:
            survivorship_dict["field_strategies"] = {
                k: SurvivorshipStrategy(v)
                for k, v in survivorship_dict["field_strategies"].items()
            }
        survivorship = SurvivorshipConfig(**survivorship_dict)
        
        return cls(
            sources=sources,
            quality=quality,
            survivorship=survivorship,
            enable_caching=config_dict.get("enable_caching", True),
            cache_size=config_dict.get("cache_size", 10000),
            min_trust_score=config_dict.get("min_trust_score", 0.0),
            trust_score_weights=config_dict.get("trust_score_weights", {
                "source_reliability": 0.4,
                "data_quality": 0.6
            })
        )


# Common format validators
def validate_email(value: str) -> bool:
    """Validate email format."""
    import re
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, str(value)))


def validate_phone(value: str) -> bool:
    """Validate phone number format."""
    import phonenumbers
    try:
        number = phonenumbers.parse(str(value), "US")
        return phonenumbers.is_valid_number(number)
    except:
        return False


def validate_date(value: str) -> bool:
    """Validate date format."""
    from datetime import datetime
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return True
    except:
        return False 