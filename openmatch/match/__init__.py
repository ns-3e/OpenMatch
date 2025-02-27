"""Match configuration and engine for OpenMatch."""

from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any, Optional

class MatchType(Enum):
    """Types of matching algorithms."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    PHONETIC = "phonetic"
    ADDRESS = "address"

class NullHandling(Enum):
    """How to handle null values in matching."""
    IGNORE = "ignore"
    NO_MATCH = "no_match"
    MATCH = "match"

@dataclass
class FieldMatchConfig:
    """Configuration for field-level matching."""
    match_type: MatchType
    threshold: float = 0.8
    case_sensitive: bool = False
    null_handling: NullHandling = NullHandling.IGNORE
    phonetic_algorithm: Optional[str] = None

@dataclass
class BlockingConfig:
    """Configuration for blocking strategy."""
    blocking_keys: List[str]
    min_block_size: int = 2
    max_block_size: int = 1000
    method: str = "standard"

@dataclass
class MatchConfig:
    """Configuration for match engine."""
    blocking: BlockingConfig
    field_configs: Dict[str, FieldMatchConfig]
    min_overall_score: float = 0.8
    score_aggregation: str = "weighted_average"
    parallel_processing: bool = True
    num_workers: int = 4

class MatchEngine:
    """Engine for finding matches between records."""
    
    def __init__(self, config: MatchConfig):
        self.config = config

    def find_matches(self, records: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """
        Find matches in a list of records.
        
        Args:
            records: List of records to match
            
        Returns:
            List of matched record groups
        """
        if not records:
            return []
            
        # Group records into blocks
        blocks = self._create_blocks(records)
        
        # Find matches within each block
        match_groups = []
        for block in blocks:
            if len(block) < self.config.blocking.min_block_size:
                continue
                
            if len(block) > self.config.blocking.max_block_size:
                continue
                
            # Compare records within block
            group = self._find_matches_in_block(block)
            if group:
                match_groups.append(group)
                
        return match_groups

    def _create_blocks(self, records: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Create blocks of records for comparison."""
        blocks = {}
        
        for record in records:
            # Create blocking key
            key_parts = []
            for key in self.config.blocking.blocking_keys:
                value = record.get(key, "")
                if isinstance(value, str):
                    # For string values, use first 3 characters
                    key_parts.append(value[:3].lower())
                else:
                    key_parts.append(str(value))
                    
            block_key = "|".join(key_parts)
            
            # Add record to block
            if block_key not in blocks:
                blocks[block_key] = []
            blocks[block_key].append(record)
            
        return list(blocks.values())

    def _find_matches_in_block(self, block: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """Find matches between records in a block."""
        if len(block) < 2:
            return None
            
        # Compare each record pair
        matches = []
        for i, record1 in enumerate(block):
            for record2 in block[i+1:]:
                score = self._compare_records(record1, record2)
                if score >= self.config.min_overall_score:
                    matches.append(record1)
                    matches.append(record2)
                    
        return list(set(matches)) if matches else None

    def _compare_records(self, record1: Dict[str, Any], record2: Dict[str, Any]) -> float:
        """Compare two records and return match score."""
        field_scores = []
        field_weights = []
        
        for field, config in self.config.field_configs.items():
            value1 = record1.get(field)
            value2 = record2.get(field)
            
            # Handle null values
            if value1 is None or value2 is None:
                if config.null_handling == NullHandling.IGNORE:
                    continue
                elif config.null_handling == NullHandling.NO_MATCH:
                    field_scores.append(0.0)
                    field_weights.append(1.0)
                    continue
                elif config.null_handling == NullHandling.MATCH:
                    if value1 is None and value2 is None:
                        field_scores.append(1.0)
                        field_weights.append(1.0)
                    else:
                        field_scores.append(0.0)
                        field_weights.append(1.0)
                    continue
            
            # Compare values based on match type
            score = 0.0
            if config.match_type == MatchType.EXACT:
                if not config.case_sensitive and isinstance(value1, str) and isinstance(value2, str):
                    score = 1.0 if value1.lower() == value2.lower() else 0.0
                else:
                    score = 1.0 if value1 == value2 else 0.0
                    
            elif config.match_type == MatchType.FUZZY:
                if isinstance(value1, str) and isinstance(value2, str):
                    # Simple Levenshtein-based score for now
                    import Levenshtein
                    max_len = max(len(value1), len(value2))
                    if max_len == 0:
                        score = 1.0
                    else:
                        distance = Levenshtein.distance(value1.lower(), value2.lower())
                        score = 1.0 - (distance / max_len)
                        
            elif config.match_type == MatchType.PHONETIC:
                if isinstance(value1, str) and isinstance(value2, str):
                    # Use specified phonetic algorithm
                    if config.phonetic_algorithm == "soundex":
                        import jellyfish
                        score = 1.0 if jellyfish.soundex(value1) == jellyfish.soundex(value2) else 0.0
                    else:
                        # Default to metaphone
                        import jellyfish
                        score = 1.0 if jellyfish.metaphone(value1) == jellyfish.metaphone(value2) else 0.0
                        
            elif config.match_type == MatchType.ADDRESS:
                # Simple address comparison for now
                if isinstance(value1, dict) and isinstance(value2, dict):
                    # Compare each address component
                    components = ["street", "city", "state", "postal_code"]
                    component_scores = []
                    for component in components:
                        comp1 = value1.get(component, "").lower()
                        comp2 = value2.get(component, "").lower()
                        if comp1 and comp2:
                            import Levenshtein
                            max_len = max(len(comp1), len(comp2))
                            if max_len == 0:
                                component_scores.append(1.0)
                            else:
                                distance = Levenshtein.distance(comp1, comp2)
                                component_scores.append(1.0 - (distance / max_len))
                    
                    if component_scores:
                        score = sum(component_scores) / len(component_scores)
            
            # Apply threshold
            if score < config.threshold:
                score = 0.0
                
            field_scores.append(score)
            field_weights.append(1.0)  # Equal weights for now
            
        # Calculate overall score
        if not field_scores:
            return 0.0
            
        if self.config.score_aggregation == "weighted_average":
            total_weight = sum(field_weights)
            if total_weight == 0:
                return 0.0
            return sum(s * w for s, w in zip(field_scores, field_weights)) / total_weight
            
        elif self.config.score_aggregation == "minimum":
            return min(field_scores)
            
        else:  # default to average
            return sum(field_scores) / len(field_scores)

__all__ = [
    'MatchType',
    'NullHandling',
    'FieldMatchConfig',
    'BlockingConfig',
    'MatchConfig',
    'MatchEngine'
]
