"""
OpenMatch Trust Module - Handles trust scoring and reliability calculations.
"""

from typing import Dict, Any
from dataclasses import dataclass

from .config import (
    QualityDimension,
    SurvivorshipStrategy,
    FieldValidation,
    SourceConfig,
    QualityConfig,
    SurvivorshipConfig,
    TrustConfig
)

from .scoring import TrustScoring
from .rules import TrustRules
from .framework import TrustFramework

__all__ = [
    # Configuration
    'QualityDimension',
    'SurvivorshipStrategy',
    'FieldValidation',
    'SourceConfig',
    'QualityConfig',
    'SurvivorshipConfig',
    'TrustConfig',
    
    # Components
    'TrustScoring',
    'TrustRules',
    'TrustFramework'
]

@dataclass
class TrustConfig:
    """Configuration for trust scoring."""
    source_reliability: Dict[str, float]
    field_weights: Dict[str, float]
    completeness_weight: float = 0.3
    accuracy_weight: float = 0.4
    timeliness_weight: float = 0.3
    min_trust_score: float = 0.0
    max_trust_score: float = 1.0
    default_source_weight: float = 0.5
    default_field_weight: float = 0.5

class TrustFramework:
    """Handles trust scoring and reliability calculations."""
    
    def __init__(self, config: TrustConfig):
        """Initialize trust framework.
        
        Args:
            config: Trust framework configuration
        """
        self.config = config
    
    def calculate_trust_scores(self, record: Dict[str, Any]) -> Dict[str, float]:
        """Calculate trust scores for a record.
        
        Args:
            record: Record to calculate trust scores for
            
        Returns:
            Dictionary of field trust scores
        """
        source = record.get("source", "unknown")
        source_reliability = self.config.source_reliability.get(source, self.config.default_source_weight)
        
        trust_scores = {}
        for field, value in record.items():
            if field in self.config.field_weights:
                field_weight = self.config.field_weights.get(field, self.config.default_field_weight)
                trust_scores[field] = source_reliability * field_weight
        
        return trust_scores
