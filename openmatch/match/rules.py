from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime
import jellyfish
import editdistance
import usaddress
import phonenumbers
from dateutil.parser import parse as parse_date
import re

from .config import (
    MatchType,
    ComparisonOperator,
    FieldMatchConfig,
    NullHandling,
    SegmentConfig,
    ConditionalRule
)

class MatchRules:
    """Implementation of various matching rules and strategies."""
    
    @staticmethod
    def handle_nulls(
        value1: Any,
        value2: Any,
        null_handling: NullHandling
    ) -> Optional[float]:
        """Handle null values according to configuration."""
        if value1 is None or value2 is None:
            if not null_handling.match_nulls:
                return 0.0
            if null_handling.require_both_non_null:
                return None
            if value1 is None and value2 is None:
                return null_handling.null_equality_score
            return null_handling.null_field_score
        return None

    @staticmethod
    def exact_match(
        value1: Any,
        value2: Any,
        case_sensitive: bool = False
    ) -> float:
        """Perform exact matching."""
        if not case_sensitive and isinstance(value1, str) and isinstance(value2, str):
            return float(value1.lower() == value2.lower())
        return float(value1 == value2)

    @staticmethod
    def fuzzy_match(
        value1: str,
        value2: str,
        method: str = "levenshtein",
        threshold: float = 0.8
    ) -> float:
        """Perform fuzzy string matching."""
        if not isinstance(value1, str) or not isinstance(value2, str):
            return 0.0
            
        value1 = value1.lower()
        value2 = value2.lower()
        
        if method == "levenshtein":
            max_len = max(len(value1), len(value2))
            if max_len == 0:
                return 1.0
            distance = editdistance.eval(value1, value2)
            score = 1 - (distance / max_len)
        elif method == "jaro":
            score = jellyfish.jaro_similarity(value1, value2)
        elif method == "jaro_winkler":
            score = jellyfish.jaro_winkler_similarity(value1, value2)
        else:
            raise ValueError(f"Unsupported fuzzy match method: {method}")
            
        return score if score >= threshold else 0.0

    @staticmethod
    def phonetic_match(
        value1: str,
        value2: str,
        algorithm: str = "soundex"
    ) -> float:
        """Perform phonetic matching."""
        if not isinstance(value1, str) or not isinstance(value2, str):
            return 0.0
            
        if algorithm == "soundex":
            return float(
                jellyfish.soundex(value1) == jellyfish.soundex(value2)
            )
        elif algorithm == "metaphone":
            return float(
                jellyfish.metaphone(value1) == jellyfish.metaphone(value2)
            )
        elif algorithm == "nysiis":
            return float(
                jellyfish.nysiis(value1) == jellyfish.nysiis(value2)
            )
        else:
            raise ValueError(f"Unsupported phonetic algorithm: {algorithm}")

    @staticmethod
    def numeric_match(
        value1: Union[int, float],
        value2: Union[int, float],
        tolerance: float = 0.0
    ) -> float:
        """Perform numeric matching with tolerance."""
        try:
            num1 = float(value1)
            num2 = float(value2)
            if tolerance == 0:
                return float(num1 == num2)
            return float(abs(num1 - num2) <= tolerance)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def date_match(
        value1: Any,
        value2: Any,
        format: Optional[str] = None
    ) -> float:
        """Perform date matching."""
        try:
            if format:
                date1 = datetime.strptime(str(value1), format)
                date2 = datetime.strptime(str(value2), format)
            else:
                date1 = parse_date(str(value1))
                date2 = parse_date(str(value2))
            return float(date1 == date2)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def address_match(
        addr1: str,
        addr2: str
    ) -> float:
        """Perform address matching."""
        try:
            # Parse addresses
            parsed1 = dict(usaddress.tag(addr1)[0])
            parsed2 = dict(usaddress.tag(addr2)[0])
            
            # Compare components with different weights
            weights = {
                "AddressNumber": 0.25,
                "StreetName": 0.35,
                "StreetNamePostType": 0.15,
                "PlaceName": 0.15,
                "StateName": 0.05,
                "ZipCode": 0.05
            }
            
            score = 0.0
            for component, weight in weights.items():
                val1 = parsed1.get(component, "").lower()
                val2 = parsed2.get(component, "").lower()
                
                if component in ["AddressNumber", "ZipCode"]:
                    score += weight * float(val1 == val2)
                else:
                    score += weight * MatchRules.fuzzy_match(val1, val2)
                    
            return score
            
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def evaluate_condition(
        value: Any,
        operator: ComparisonOperator,
        target: Any
    ) -> bool:
        """Evaluate a condition for conditional matching."""
        if operator == ComparisonOperator.EQUALS:
            return value == target
        elif operator == ComparisonOperator.NOT_EQUALS:
            return value != target
        elif operator == ComparisonOperator.GREATER_THAN:
            return value > target
        elif operator == ComparisonOperator.LESS_THAN:
            return value < target
        elif operator == ComparisonOperator.CONTAINS:
            return target in value
        elif operator == ComparisonOperator.STARTS_WITH:
            return str(value).startswith(str(target))
        elif operator == ComparisonOperator.ENDS_WITH:
            return str(value).endswith(str(target))
        elif operator == ComparisonOperator.REGEX:
            return bool(re.match(target, str(value)))
        elif operator == ComparisonOperator.IN:
            return value in target
        elif operator == ComparisonOperator.NOT_IN:
            return value not in target
        return False

    @classmethod
    def match_field(
        cls,
        value1: Any,
        value2: Any,
        config: FieldMatchConfig
    ) -> float:
        """Match two values according to field configuration."""
        # Handle null values
        null_score = cls.handle_nulls(value1, value2, config.null_handling)
        if null_score is not None:
            return null_score
            
        # Apply preprocessors
        for preprocessor in config.preprocessors:
            if isinstance(preprocessor, str):
                # Built-in preprocessors
                if preprocessor == "lower":
                    value1 = str(value1).lower()
                    value2 = str(value2).lower()
                elif preprocessor == "strip":
                    value1 = str(value1).strip()
                    value2 = str(value2).strip()
                elif preprocessor == "normalize_phone":
                    try:
                        value1 = phonenumbers.format_number(
                            phonenumbers.parse(str(value1), "US"),
                            phonenumbers.PhoneNumberFormat.E164
                        )
                        value2 = phonenumbers.format_number(
                            phonenumbers.parse(str(value2), "US"),
                            phonenumbers.PhoneNumberFormat.E164
                        )
                    except:
                        pass
            else:
                # Custom preprocessor function
                value1 = preprocessor(value1)
                value2 = preprocessor(value2)
                
        # Handle conditional rules
        for rule in config.conditional_rules:
            if cls.evaluate_condition(value1, rule.operator, rule.value):
                return cls.match_field(value1, value2, rule.match_config)
                
        # Handle segmented matching
        if config.segment_config:
            segment = value1.get(config.segment_config.segment_field)
            weight = config.segment_config.segment_values.get(
                segment,
                config.segment_config.default_weight
            )
            return weight * cls._match_by_type(value1, value2, config)
            
        return cls._match_by_type(value1, value2, config)

    @classmethod
    def _match_by_type(
        cls,
        value1: Any,
        value2: Any,
        config: FieldMatchConfig
    ) -> float:
        """Apply the appropriate matching strategy based on type."""
        if config.match_type == MatchType.EXACT:
            return cls.exact_match(value1, value2)
        elif config.match_type == MatchType.FUZZY:
            return cls.fuzzy_match(
                str(value1),
                str(value2),
                **config.fuzzy_params
            )
        elif config.match_type == MatchType.PHONETIC:
            return cls.phonetic_match(
                str(value1),
                str(value2),
                config.phonetic_algorithm or "soundex"
            )
        elif config.match_type == MatchType.NUMERIC:
            return cls.numeric_match(
                value1,
                value2,
                config.numeric_tolerance or 0.0
            )
        elif config.match_type == MatchType.DATE:
            return cls.date_match(
                value1,
                value2,
                config.date_format
            )
        elif config.match_type == MatchType.ADDRESS:
            return cls.address_match(str(value1), str(value2))
        elif config.match_type == MatchType.CUSTOM:
            if not config.custom_config:
                raise ValueError("Custom match type requires custom_config")
            # Custom matching would be implemented by the user
            raise NotImplementedError(
                f"Custom matching function {config.custom_config.function_name} "
                "not implemented"
            )
        else:
            raise ValueError(f"Unsupported match type: {config.match_type}")
