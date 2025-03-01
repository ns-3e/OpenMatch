"""
Database connectivity and operations for OpenMatch.
"""

from .database import DatabaseConfig, DatabaseConnector
from .init_db import init_database, reset_schema
from .schema import (
    Base,
    MasterRecord,
    SourceRecord,
    MatchResult,
    MergeHistory,
    RuleSet
)

__all__ = [
    # Database connection
    "DatabaseConfig",
    "DatabaseConnector",
    "init_database",
    "reset_schema",
    
    # Schema models
    "Base",
    "MasterRecord",
    "SourceRecord",
    "MatchResult",
    "MergeHistory",
    "RuleSet"
]
