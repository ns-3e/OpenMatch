from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, field
import jellyfish
from thefuzz import fuzz
from .config import MatchType, FieldConfig, MatchRuleConfig
from .matchers import MatcherFactory

class FieldMatcher:
    """Base class for field matching implementations."""
    
    @staticmethod
    def compute_similarity(value1: Any, value2: Any) -> float:
        """Compute similarity between two field values."""
        raise NotImplementedError

class ExactMatcher(FieldMatcher):
    """Matcher for exact field comparisons."""
    
    @staticmethod
    def compute_similarity(value1: Any, value2: Any) -> float:
        """Return 1.0 for exact match, 0.0 otherwise."""
        if value1 is None or value2 is None:
            return 0.0
        return float(str(value1).lower() == str(value2).lower())

class FuzzyMatcher(FieldMatcher):
    """Matcher for fuzzy field comparisons."""
    
    @staticmethod
    def compute_similarity(value1: Any, value2: Any, method: str = "jaro_winkler") -> float:
        """Compute fuzzy similarity using specified method."""
        if value1 is None or value2 is None:
            return 0.0
            
        str1, str2 = str(value1).lower(), str(value2).lower()
        
        if method == "jaro_winkler":
            return jellyfish.jaro_winkler_similarity(str1, str2)
        elif method == "levenshtein":
            return 1 - (jellyfish.levenshtein_distance(str1, str2) / max(len(str1), len(str2)))
        elif method == "ratio":
            return fuzz.ratio(str1, str2) / 100.0
        else:
            raise ValueError(f"Unsupported fuzzy matching method: {method}")

class MatchRule:
    """Rule for matching records based on configured fields."""
    
    def __init__(self, config: MatchRuleConfig):
        self.config = config
        self.matchers = {}
        self._initialize_matchers()
    
    def _initialize_matchers(self):
        """Initialize appropriate matchers for each field."""
        for field in self.config.fields:
            self.matchers[field.name] = MatcherFactory.create_matcher(
                field.match_type.value,
                embedding_model=field.embedding_model
            )
    
    def compute_field_similarity(self, field: FieldConfig, value1: Any, value2: Any) -> float:
        """Compute similarity for a single field."""
        if value1 is None or value2 is None:
            return 0.0
            
        matcher = self.matchers.get(field.name)
        if not matcher:
            return 1.0 if value1 == value2 else 0.0
            
        if field.match_type == MatchType.EMBEDDING:
            return matcher.compute_similarity(str(value1), str(value2))
        elif field.match_type == MatchType.FUZZY:
            return matcher.compute_similarity(value1, value2, field.fuzzy_method)
        else:
            return 1.0 if value1 == value2 else 0.0
    
    def compute_match_confidence(self, record1: Dict[str, Any], record2: Dict[str, Any]) -> float:
        """Compute overall match confidence for two records."""
        total_score = 0.0
        total_weight = 0.0
        
        for field in self.config.fields:
            if field.required and (field.name not in record1 or field.name not in record2):
                return 0.0
                
            if field.name in record1 and field.name in record2:
                similarity = self.compute_field_similarity(
                    field, 
                    record1[field.name], 
                    record2[field.name]
                )
                
                if similarity < field.threshold and field.required:
                    return 0.0
                    
                total_score += similarity * field.weight
                total_weight += field.weight
        
        return total_score / total_weight if total_weight > 0 else 0.0

    def apply(self, record1: Dict[str, Any], record2: Dict[str, Any], fast_mode: bool = False) -> Tuple[MatchType, float]:
        """Apply the rule to two records and return match type and confidence score.
        
        Args:
            record1: First record to compare
            record2: Second record to compare
            fast_mode: If True, use faster but less accurate matching
            
        Returns:
            Tuple of (MatchType, confidence_score)
        """
        try:
            total_score = 0.0
            total_weight = 0.0
            
            # In fast mode, only check required fields
            fields_to_check = [f for f in self.config.fields if f.required] if fast_mode else self.config.fields
            
            for field in fields_to_check:
                value1 = record1.get(field.name)
                value2 = record2.get(field.name)
                
                # Skip if both values are None
                if value1 is None and value2 is None:
                    continue
                    
                # If field is required and either value is None, no match
                if field.required and (value1 is None or value2 is None):
                    return MatchType.NO_MATCH, 0.0
                
                # Compute field similarity
                similarity = self.compute_field_similarity(field, value1, value2)
                
                # If exact match is required and not met, no match
                if field.match_type == MatchType.EXACT and similarity < 1.0:
                    return MatchType.NO_MATCH, 0.0
                
                # Add weighted score
                total_score += similarity * field.weight
                total_weight += field.weight
            
            # Compute final confidence score
            if total_weight == 0:
                return MatchType.NO_MATCH, 0.0
            
            confidence = total_score / total_weight
            
            # Determine match type based on confidence
            if confidence >= self.config.min_confidence:
                if confidence == 1.0:
                    return MatchType.EXACT, confidence
                else:
                    return MatchType.FUZZY, confidence
            elif confidence >= self.config.min_confidence * 0.8:  # 80% of min confidence for potential matches
                return MatchType.POTENTIAL, confidence
            else:
                return MatchType.NO_MATCH, confidence
                
        except Exception as e:
            print(f"Error applying match rule: {str(e)}")
            return MatchType.ERROR, 0.0

# Preset rules
def create_exact_ssn_rule() -> MatchRuleConfig:
    """Create a rule for exact SSN matching."""
    return MatchRuleConfig(
        name="exact_ssn",
        rule_id="EXACT_SSN_001",
        fields=[
            FieldConfig(
                name="ssn",
                match_type=MatchType.EXACT,
                weight=1.0,
                required=True
            )
        ]
    )

def create_fuzzy_name_dob_rule() -> MatchRuleConfig:
    """Create a rule for fuzzy name matching with DOB."""
    return MatchRuleConfig(
        name="fuzzy_name_dob",
        rule_id="FUZZY_NAME_DOB_001",
        fields=[
            FieldConfig(
                name="first_name",
                match_type=MatchType.FUZZY,
                weight=0.3,
                threshold=0.8,
                fuzzy_method="jaro_winkler"
            ),
            FieldConfig(
                name="last_name",
                match_type=MatchType.FUZZY,
                weight=0.4,
                threshold=0.8,
                fuzzy_method="jaro_winkler"
            ),
            FieldConfig(
                name="dob",
                match_type=MatchType.EXACT,
                weight=0.3,
                required=True
            )
        ]
    )
