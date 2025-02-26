from typing import Dict, List, Any, Optional
from datetime import datetime
import re
from functools import lru_cache

from .config import (
    TrustConfig,
    QualityDimension,
    FieldValidation,
    SourceConfig
)

class TrustScoring:
    """Calculates trust and quality scores for records."""
    
    def __init__(self, config: TrustConfig):
        """Initialize trust scoring.
        
        Args:
            config: Trust framework configuration
        """
        self.config = config
        
        # Initialize caching if enabled
        if config.enable_caching:
            self.calculate_quality_score = lru_cache(maxsize=config.cache_size)(
                self._calculate_quality_score
            )
            self.calculate_trust_score = lru_cache(maxsize=config.cache_size)(
                self._calculate_trust_score
            )
        else:
            self.calculate_quality_score = self._calculate_quality_score
            self.calculate_trust_score = self._calculate_trust_score

    def calculate_record_scores(
        self,
        record: Dict[str, Any],
        source: str
    ) -> Dict[str, float]:
        """Calculate all scores for a record.
        
        Args:
            record: Record to score
            source: Source system name
            
        Returns:
            Dictionary of scores by dimension
        """
        quality_score = self.calculate_quality_score(record, source)
        trust_score = self.calculate_trust_score(record, source)
        
        return {
            "quality_score": quality_score,
            "trust_score": trust_score,
            "dimension_scores": {
                dim.value: self._calculate_dimension_score(record, source, dim)
                for dim in QualityDimension
            }
        }

    def _calculate_quality_score(
        self,
        record: Dict[str, Any],
        source: str
    ) -> float:
        """Calculate quality score for a record."""
        scores = {}
        
        for dimension, weight in self.config.quality.dimensions.items():
            score = self._calculate_dimension_score(record, source, dimension)
            scores[dimension] = score * weight
            
        return sum(scores.values())

    def _calculate_trust_score(
        self,
        record: Dict[str, Any],
        source: str
    ) -> float:
        """Calculate overall trust score for a record."""
        # Get source reliability
        source_config = self.config.sources.get(source)
        if not source_config:
            return 0.0
            
        # Calculate component scores
        quality_score = self.calculate_quality_score(record, source)
        
        # Calculate weighted sum
        weights = self.config.trust_score_weights
        trust_score = (
            weights["source_reliability"] * source_config.reliability_score +
            weights["data_quality"] * quality_score
        )
        
        return max(0.0, min(1.0, trust_score))

    def _calculate_dimension_score(
        self,
        record: Dict[str, Any],
        source: str,
        dimension: QualityDimension
    ) -> float:
        """Calculate score for a specific quality dimension."""
        if dimension == QualityDimension.COMPLETENESS:
            return self._calculate_completeness(record)
        elif dimension == QualityDimension.ACCURACY:
            return self._calculate_accuracy(record, source)
        elif dimension == QualityDimension.CONSISTENCY:
            return self._calculate_consistency(record)
        elif dimension == QualityDimension.TIMELINESS:
            return self._calculate_timeliness(record, source)
        elif dimension == QualityDimension.UNIQUENESS:
            return self._calculate_uniqueness(record)
        elif dimension == QualityDimension.VALIDITY:
            return self._calculate_validity(record, source)
        else:
            return 0.0

    def _calculate_completeness(self, record: Dict[str, Any]) -> float:
        """Calculate completeness score."""
        if not record:
            return 0.0
            
        weights = self.config.quality.completeness_rules
        if not weights:
            # Equal weights if not specified
            fields = set(record.keys())
            weights = {field: 1.0 / len(fields) for field in fields}
            
        score = 0.0
        for field, weight in weights.items():
            if field in record and record[field] is not None:
                score += weight
                
        return score

    def _calculate_accuracy(
        self,
        record: Dict[str, Any],
        source: str
    ) -> float:
        """Calculate accuracy score based on source reliability and validation."""
        source_config = self.config.sources.get(source)
        if not source_config:
            return 0.0
            
        # Start with source reliability
        score = source_config.reliability_score
        
        # Adjust based on field validation
        valid_fields = 0
        total_fields = 0
        
        for field, validation in source_config.validation_rules.items():
            if field not in record:
                continue
                
            total_fields += 1
            if self._validate_field(record[field], validation):
                valid_fields += 1
                
        if total_fields > 0:
            validation_score = valid_fields / total_fields
            # Combine source reliability and validation scores
            score = (score + validation_score) / 2
            
        return score

    def _calculate_consistency(self, record: Dict[str, Any]) -> float:
        """Calculate consistency score based on rules."""
        if not self.config.quality.consistency_rules:
            return 1.0
            
        valid_rules = 0
        total_rules = 0
        
        for rule in self.config.quality.consistency_rules:
            total_rules += 1
            if self._check_consistency_rule(record, rule):
                valid_rules += 1
                
        return valid_rules / total_rules if total_rules > 0 else 1.0

    def _calculate_timeliness(
        self,
        record: Dict[str, Any],
        source: str
    ) -> float:
        """Calculate timeliness score based on record age."""
        source_config = self.config.sources.get(source)
        if not source_config or not source_config.last_update_field:
            return 0.0
            
        last_update = record.get(source_config.last_update_field)
        if not last_update:
            return 0.0
            
        if isinstance(last_update, str):
            try:
                last_update = datetime.fromisoformat(last_update)
            except ValueError:
                return 0.0
                
        age = datetime.utcnow() - last_update
        
        if not self.config.quality.timeliness_decay:
            return 1.0
            
        # Calculate decay score
        decay_factor = age / self.config.quality.timeliness_decay
        return max(0.0, 1.0 - decay_factor)

    def _calculate_uniqueness(self, record: Dict[str, Any]) -> float:
        """Calculate uniqueness score.
        
        Note: This is a placeholder implementation. In practice, this would
        require access to the full dataset to check for duplicates.
        """
        return 1.0

    def _calculate_validity(
        self,
        record: Dict[str, Any],
        source: str
    ) -> float:
        """Calculate validity score based on field validation rules."""
        source_config = self.config.sources.get(source)
        if not source_config:
            return 0.0
            
        valid_fields = 0
        total_fields = 0
        
        # Check source-specific validation rules
        for field, validation in source_config.validation_rules.items():
            if field not in record:
                continue
                
            total_fields += 1
            if self._validate_field(record[field], validation):
                valid_fields += 1
                
        # Check global validation rules
        for field, validation in self.config.quality.validity_rules.items():
            if field not in record:
                continue
                
            total_fields += 1
            if self._validate_field(record[field], validation):
                valid_fields += 1
                
        return valid_fields / total_fields if total_fields > 0 else 0.0

    def _validate_field(
        self,
        value: Any,
        validation: FieldValidation
    ) -> bool:
        """Validate a field value against validation rules."""
        if validation.required and value is None:
            return False
            
        if value is None:
            return True
            
        # Type validation
        if validation.data_type == "string":
            if not isinstance(value, str):
                return False
            if validation.min_length and len(value) < validation.min_length:
                return False
            if validation.max_length and len(value) > validation.max_length:
                return False
            if validation.pattern and not re.match(validation.pattern, value):
                return False
                
        # Allowed values
        if validation.allowed_values and value not in validation.allowed_values:
            return False
            
        # Custom validation
        if validation.custom_validator and not validation.custom_validator(value):
            return False
            
        return True

    def _check_consistency_rule(
        self,
        record: Dict[str, Any],
        rule: Dict[str, Any]
    ) -> bool:
        """Check if a record satisfies a consistency rule."""
        rule_type = rule.get("type")
        
        if rule_type == "dependency":
            # If field A exists, field B must exist
            field_a = rule.get("field_a")
            field_b = rule.get("field_b")
            if field_a in record and record[field_a] is not None:
                return field_b in record and record[field_b] is not None
                
        elif rule_type == "mutual_exclusion":
            # Fields cannot both have values
            field_a = rule.get("field_a")
            field_b = rule.get("field_b")
            return not (
                field_a in record and record[field_a] is not None and
                field_b in record and record[field_b] is not None
            )
            
        elif rule_type == "value_dependency":
            # If field A has value X, field B must have value Y
            field_a = rule.get("field_a")
            field_b = rule.get("field_b")
            value_a = rule.get("value_a")
            value_b = rule.get("value_b")
            
            if (field_a in record and record[field_a] == value_a):
                return field_b in record and record[field_b] == value_b
                
        return True
