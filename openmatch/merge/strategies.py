"""
Merge strategies implementation for OpenMatch.
"""

from typing import Dict, List, Optional, Protocol
from abc import ABC, abstractmethod
import logging
from datetime import datetime

from openmatch.utils.logging import setup_logging


class MergeStrategy(ABC):
    """Abstract base class for merge strategies."""
    
    @abstractmethod
    def merge_records(
        self,
        records: List[Dict],
        golden_id: str,
        trust_scores: Optional[Dict[str, Dict[str, float]]] = None
    ) -> Dict:
        """Merge a list of records into a golden record.
        
        Args:
            records: List of records to merge
            golden_id: ID for the golden record
            trust_scores: Optional trust scores for records
            
        Returns:
            Merged golden record
        """
        pass


class DefaultMergeStrategy(MergeStrategy):
    """Default strategy using most trusted/recent values."""
    
    def __init__(
        self,
        id_field: str = "id",
        timestamp_field: str = "last_updated",
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the default merge strategy.
        
        Args:
            id_field: Field containing record ID
            timestamp_field: Field containing record timestamp
            logger: Optional logger instance
        """
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.logger = logger or setup_logging(__name__)
        
    def merge_records(
        self,
        records: List[Dict],
        golden_id: str,
        trust_scores: Optional[Dict[str, Dict[str, float]]] = None
    ) -> Dict:
        """Merge records using most trusted/recent values.
        
        Args:
            records: List of records to merge
            golden_id: ID for the golden record
            trust_scores: Optional trust scores for records
            
        Returns:
            Merged golden record
        """
        if not records:
            return {}
            
        # Start with empty golden record
        golden_record = {}
        
        # Get all field names
        fields = set()
        for record in records:
            fields.update(record.keys())
            
        # Remove metadata fields
        fields.discard(self.id_field)
        fields.discard(self.timestamp_field)
        
        # Process each field
        for field in fields:
            values = []
            
            # Collect values and metadata
            for record in records:
                if field not in record:
                    continue
                    
                value = record[field]
                record_id = record[self.id_field]
                
                # Get trust score
                trust_score = 0.0
                if trust_scores and record_id in trust_scores:
                    trust_score = trust_scores[record_id]["total"]
                    
                # Get timestamp
                timestamp = None
                if self.timestamp_field in record:
                    try:
                        timestamp = datetime.fromisoformat(
                            record[self.timestamp_field].replace("Z", "+00:00")
                        )
                    except (ValueError, TypeError):
                        pass
                        
                values.append({
                    "value": value,
                    "trust_score": trust_score,
                    "timestamp": timestamp
                })
                
            if not values:
                continue
                
            # Select best value
            if trust_scores:
                # Use most trusted value
                best_value = max(
                    values,
                    key=lambda x: x["trust_score"]
                )["value"]
            else:
                # Use most recent value
                best_value = max(
                    (v for v in values if v["timestamp"]),
                    key=lambda x: x["timestamp"],
                    default=values[0]
                )["value"]
                
            golden_record[field] = best_value
            
        return golden_record


class WeightedMergeStrategy(MergeStrategy):
    """Strategy that combines values using weights."""
    
    def __init__(
        self,
        field_weights: Optional[Dict[str, float]] = None,
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the weighted merge strategy.
        
        Args:
            field_weights: Optional weights for specific fields
            logger: Optional logger instance
        """
        self.field_weights = field_weights or {}
        self.logger = logger or setup_logging(__name__)
        
    def merge_records(
        self,
        records: List[Dict],
        golden_id: str,
        trust_scores: Optional[Dict[str, Dict[str, float]]] = None
    ) -> Dict:
        """Merge records using weighted combinations.
        
        Args:
            records: List of records to merge
            golden_id: ID for the golden record
            trust_scores: Optional trust scores for records
            
        Returns:
            Merged golden record
        """
        if not records:
            return {}
            
        golden_record = {}
        fields = set().union(*(record.keys() for record in records))
        
        for field in fields:
            if field in self.field_weights:
                # Use weighted combination
                values = []
                weights = []
                
                for record in records:
                    if field not in record:
                        continue
                        
                    value = record[field]
                    weight = self.field_weights[field]
                    
                    # Adjust weight by trust score
                    if trust_scores:
                        record_id = record.get("id")
                        if record_id in trust_scores:
                            weight *= trust_scores[record_id]["total"]
                            
                    values.append(value)
                    weights.append(weight)
                    
                if values:
                    # Combine values based on type
                    if all(isinstance(v, (int, float)) for v in values):
                        # Weighted average for numbers
                        total_weight = sum(weights)
                        if total_weight > 0:
                            golden_record[field] = sum(
                                v * w for v, w in zip(values, weights)
                            ) / total_weight
                    else:
                        # Most heavily weighted value for others
                        max_weight_idx = weights.index(max(weights))
                        golden_record[field] = values[max_weight_idx]
            else:
                # Use default strategy
                values = [
                    record[field]
                    for record in records
                    if field in record
                ]
                if values:
                    # Use most common value
                    from collections import Counter
                    golden_record[field] = Counter(values).most_common(1)[0][0]
                    
        return golden_record


class CustomMergeStrategy(MergeStrategy):
    """Strategy that uses custom merge functions by field."""
    
    def __init__(
        self,
        merge_functions: Dict[str, callable],
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the custom merge strategy.
        
        Args:
            merge_functions: Dictionary mapping fields to merge functions
            logger: Optional logger instance
        """
        self.merge_functions = merge_functions
        self.logger = logger or setup_logging(__name__)
        
    def merge_records(
        self,
        records: List[Dict],
        golden_id: str,
        trust_scores: Optional[Dict[str, Dict[str, float]]] = None
    ) -> Dict:
        """Merge records using custom functions.
        
        Args:
            records: List of records to merge
            golden_id: ID for the golden record
            trust_scores: Optional trust scores for records
            
        Returns:
            Merged golden record
        """
        if not records:
            return {}
            
        golden_record = {}
        fields = set().union(*(record.keys() for record in records))
        
        for field in fields:
            if field in self.merge_functions:
                try:
                    # Get values for this field
                    values = [
                        record[field]
                        for record in records
                        if field in record
                    ]
                    
                    if values:
                        # Apply custom merge function
                        golden_record[field] = self.merge_functions[field](
                            values,
                            trust_scores=trust_scores
                        )
                except Exception as e:
                    self.logger.error(
                        f"Error in custom merge for {field}: {str(e)}"
                    )
            else:
                # Use default strategy
                values = [
                    record[field]
                    for record in records
                    if field in record
                ]
                if values:
                    golden_record[field] = values[0]
                    
        return golden_record


# Example custom merge functions
def merge_addresses(
    values: List[str],
    trust_scores: Optional[Dict[str, Dict[str, float]]] = None
) -> str:
    """Merge address values.
    
    Args:
        values: List of address strings
        trust_scores: Optional trust scores
        
    Returns:
        Merged address string
    """
    # TODO: Implement proper address parsing and merging
    return values[0]


def merge_phone_numbers(
    values: List[str],
    trust_scores: Optional[Dict[str, Dict[str, float]]] = None
) -> str:
    """Merge phone number values.
    
    Args:
        values: List of phone numbers
        trust_scores: Optional trust scores
        
    Returns:
        Merged phone number
    """
    import phonenumbers
    
    # Parse and validate numbers
    valid_numbers = []
    for value in values:
        try:
            number = phonenumbers.parse(value, "US")
            if phonenumbers.is_valid_number(number):
                valid_numbers.append(number)
        except:
            continue
            
    if valid_numbers:
        # Format first valid number
        return phonenumbers.format_number(
            valid_numbers[0],
            phonenumbers.PhoneNumberFormat.INTERNATIONAL
        )
    return values[0]
