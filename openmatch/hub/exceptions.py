"""
OpenMatch Hub and DB Manager exception hierarchy.
"""
from typing import Optional, Any, Dict


class OpenMatchError(Exception):
    """Base exception class for all OpenMatch errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class ConfigurationError(OpenMatchError):
    """Raised when there is an error in configuration."""
    pass


class InitializationError(OpenMatchError):
    """Raised when there is an error during system initialization."""
    pass


class ConnectionError(OpenMatchError):
    """Raised when there is an error establishing connections."""
    pass


class DatabaseError(OpenMatchError):
    """Base class for database-related errors."""
    pass


class QueryError(DatabaseError):
    """Raised when there is an error in query execution."""
    pass


class TransactionError(DatabaseError):
    """Raised when there is an error in transaction management."""
    pass


class CacheError(OpenMatchError):
    """Raised when there is an error in cache operations."""
    pass


class ValidationError(OpenMatchError):
    """Raised when there is a validation error."""
    pass


class ResourceError(OpenMatchError):
    """Raised when there is an error accessing or managing resources."""
    pass


class SecurityError(OpenMatchError):
    """Raised when there is a security-related error."""
    pass
