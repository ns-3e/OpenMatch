"""
OpenMatch MDM matching engine package
"""

from .engine import MatchEngine
from .config import MatchConfig, BlockingConfig, FieldConfig, MatchRuleConfig, MatchType, MetadataConfig
from .rules import MatchRule, create_exact_ssn_rule, create_fuzzy_name_dob_rule

__all__ = [
    'MatchEngine',
    'MatchConfig',
    'BlockingConfig',
    'FieldConfig',
    'MatchRuleConfig',
    'MatchType',
    'MetadataConfig',
    'MatchRule',
    'create_exact_ssn_rule',
    'create_fuzzy_name_dob_rule'
]
