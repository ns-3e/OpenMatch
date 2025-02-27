"""
OpenMatch Trust Module - Handles trust scoring and reliability calculations.
"""

from typing import Dict, Any, List
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
    """Framework for calculating trust scores."""
    
    def __init__(self, config: TrustConfig):
        self.config = config

    def calculate_trust_scores(self, records: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Calculate trust scores for a group of records.
        
        Args:
            records: List of records to calculate trust scores for
            
        Returns:
            Dictionary mapping record IDs to trust scores
        """
        trust_scores = {}
        
        for record in records:
            source = record.get('source')
            if not source:
                continue
                
            # Get source reliability score
            source_score = self.config.source_reliability.get(source, 0.5)
            
            # Calculate field-level scores
            field_scores = {}
            for field, weight in self.config.field_weights.items():
                if field in record and record[field]:
                    # Simple completeness check for now
                    field_scores[field] = weight
                else:
                    field_scores[field] = 0.0
            
            # Calculate overall trust score
            if field_scores:
                total_weight = sum(self.config.field_weights.values())
                weighted_sum = sum(
                    score * self.config.field_weights[field]
                    for field, score in field_scores.items()
                )
                field_score = weighted_sum / total_weight
            else:
                field_score = 0.0
            
            # Combine source and field scores
            trust_scores[record['id']] = (source_score + field_score) / 2
        
        return trust_scores
