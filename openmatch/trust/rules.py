from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from functools import lru_cache

from .config import (
    TrustConfig,
    SurvivorshipStrategy,
    SurvivorshipConfig
)

class TrustRules:
    """Implements survivorship and trust rules."""
    
    def __init__(self, config: TrustConfig):
        """Initialize trust rules.
        
        Args:
            config: Trust framework configuration
        """
        self.config = config
        
        # Initialize caching if enabled
        if config.enable_caching:
            self.apply_survivorship = lru_cache(maxsize=config.cache_size)(
                self._apply_survivorship
            )
        else:
            self.apply_survivorship = self._apply_survivorship

    def apply_survivorship_rules(
        self,
        records: List[Dict[str, Any]],
        trust_scores: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """Apply survivorship rules to select winning record values.
        
        Args:
            records: List of records to merge
            trust_scores: Optional dict mapping record IDs to trust scores
            
        Returns:
            Merged record with winning values
        """
        if not records:
            return {}
            
        # Initialize merged record
        merged = {}
        
        # Get all field names
        fields = set()
        for record in records:
            fields.update(record.keys())
            
        # Apply survivorship rules for each field
        for field in fields:
            merged[field] = self.apply_survivorship(
                field,
                [r.get(field) for r in records],
                [r.get("source") for r in records],
                trust_scores
            )
            
        return merged

    def _apply_survivorship(
        self,
        field: str,
        values: List[Any],
        sources: List[str],
        trust_scores: Optional[Dict[str, float]] = None
    ) -> Any:
        """Apply survivorship rules to select winning value for a field."""
        if not values:
            return None
            
        # Get strategy for field
        strategy = self.config.survivorship.field_strategies.get(
            field,
            self.config.survivorship.default_strategy
        )
        
        # Apply strategy
        if strategy == SurvivorshipStrategy.MOST_RECENT:
            return self._most_recent_value(values, sources)
            
        elif strategy == SurvivorshipStrategy.MOST_COMPLETE:
            return self._most_complete_value(values)
            
        elif strategy == SurvivorshipStrategy.MOST_TRUSTED:
            return self._most_trusted_value(values, sources, trust_scores)
            
        elif strategy == SurvivorshipStrategy.LONGEST:
            return self._longest_value(values)
            
        elif strategy == SurvivorshipStrategy.SHORTEST:
            return self._shortest_value(values)
            
        elif strategy == SurvivorshipStrategy.CUSTOM:
            return self._apply_custom_strategy(field, values, sources)
            
        else:
            raise ValueError(f"Unknown survivorship strategy: {strategy}")

    def _most_recent_value(
        self,
        values: List[Any],
        sources: List[str]
    ) -> Any:
        """Select most recent value based on source update frequency."""
        if not values:
            return None
            
        # Get source configs
        source_configs = [
            self.config.sources.get(source)
            for source in sources
        ]
        
        # Filter out None values and sources without configs
        valid_values = [
            (value, config)
            for value, config in zip(values, source_configs)
            if value is not None and config is not None
        ]
        
        if not valid_values:
            return None
            
        # Sort by update frequency (more frequent updates first)
        sorted_values = sorted(
            valid_values,
            key=lambda x: (
                x[1].update_frequency or timedelta(days=365),
                -x[1].priority
            )
        )
        
        return sorted_values[0][0]

    def _most_complete_value(self, values: List[Any]) -> Any:
        """Select most complete value."""
        if not values:
            return None
            
        # Filter out None values
        valid_values = [v for v in values if v is not None]
        
        if not valid_values:
            return None
            
        # For strings, select longest value
        if all(isinstance(v, str) for v in valid_values):
            return max(valid_values, key=len)
            
        # For other types, prefer non-empty values
        return valid_values[0]

    def _most_trusted_value(
        self,
        values: List[Any],
        sources: List[str],
        trust_scores: Optional[Dict[str, float]] = None
    ) -> Any:
        """Select value from most trusted source."""
        if not values:
            return None
            
        # Get source configs
        source_configs = [
            self.config.sources.get(source)
            for source in sources
        ]
        
        # Filter out None values and sources without configs
        valid_values = [
            (value, config)
            for value, config in zip(values, source_configs)
            if value is not None and config is not None
        ]
        
        if not valid_values:
            return None
            
        # Sort by reliability score and priority
        sorted_values = sorted(
            valid_values,
            key=lambda x: (
                -x[1].reliability_score,
                -x[1].priority
            )
        )
        
        return sorted_values[0][0]

    def _longest_value(self, values: List[Any]) -> Any:
        """Select longest value."""
        if not values:
            return None
            
        # Filter out None values
        valid_values = [v for v in values if v is not None]
        
        if not valid_values:
            return None
            
        # For strings, select longest value
        if all(isinstance(v, str) for v in valid_values):
            return max(valid_values, key=len)
            
        # For other types, return first non-None value
        return valid_values[0]

    def _shortest_value(self, values: List[Any]) -> Any:
        """Select shortest value."""
        if not values:
            return None
            
        # Filter out None values
        valid_values = [v for v in values if v is not None]
        
        if not valid_values:
            return None
            
        # For strings, select shortest value
        if all(isinstance(v, str) for v in valid_values):
            return min(valid_values, key=len)
            
        # For other types, return first non-None value
        return valid_values[0]

    def _apply_custom_strategy(
        self,
        field: str,
        values: List[Any],
        sources: List[str]
    ) -> Any:
        """Apply custom survivorship strategy."""
        if not values:
            return None
            
        # Get custom strategy
        strategy = self.config.survivorship.custom_strategies.get(field)
        if not strategy:
            raise ValueError(f"No custom strategy defined for field: {field}")
            
        # Create records list for strategy
        records = [
            {"value": value, "source": source}
            for value, source in zip(values, sources)
            if value is not None
        ]
        
        if not records:
            return None
            
        return strategy(records)

    def resolve_conflicts(
        self,
        values: List[Any],
        sources: List[str],
        trust_scores: Optional[Dict[str, float]] = None
    ) -> Any:
        """Resolve conflicts between multiple values."""
        if not values:
            return None
            
        resolution = self.config.survivorship.conflict_resolution
        
        if resolution == "highest_trust":
            if trust_scores:
                # Use provided trust scores
                valid_values = [
                    (value, trust_scores.get(source, 0.0))
                    for value, source in zip(values, sources)
                    if value is not None
                ]
                if valid_values:
                    return max(valid_values, key=lambda x: x[1])[0]
            return self._most_trusted_value(values, sources, trust_scores)
            
        elif resolution == "most_recent":
            return self._most_recent_value(values, sources)
            
        elif resolution == "custom":
            if not self.config.survivorship.custom_resolution:
                raise ValueError("Custom conflict resolution requires custom_resolution function")
            return self.config.survivorship.custom_resolution(values)
            
        else:
            raise ValueError(f"Unknown conflict resolution: {resolution}")

    def validate_trust_threshold(
        self,
        value: Any,
        source: str,
        trust_score: float
    ) -> bool:
        """Check if value meets minimum trust threshold."""
        return trust_score >= self.config.survivorship.trust_threshold
