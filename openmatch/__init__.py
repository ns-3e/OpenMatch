"""
OpenMatch MDM - Master Data Management System
"""

from .match import (
    MatchType,
    BlockingConfig,
    FieldConfig,
    MatchRuleConfig,
    MetadataConfig,
    MatchConfig,
    MatchRule,
    create_exact_ssn_rule,
    create_fuzzy_name_dob_rule,
    MatchEngine
)

from .connectors.database import DatabaseConfig, DatabaseConnector
from .connectors.init_db import init_database, reset_schema
from .connectors.schema import (
    MasterRecord,
    SourceRecord,
    MatchResult,
    MergeHistory,
    RuleSet
)

__version__ = "0.1.0"

__all__ = [
    # Match components
    "MatchType",
    "BlockingConfig",
    "FieldConfig",
    "MatchRuleConfig",
    "MetadataConfig",
    "MatchConfig",
    "MatchRule",
    "create_exact_ssn_rule",
    "create_fuzzy_name_dob_rule",
    "MatchEngine",
    
    # Database components
    "DatabaseConfig",
    "DatabaseConnector",
    "init_database",
    "reset_schema",
    
    # Schema models
    "MasterRecord",
    "SourceRecord",
    "MatchResult",
    "MergeHistory",
    "RuleSet"
]
