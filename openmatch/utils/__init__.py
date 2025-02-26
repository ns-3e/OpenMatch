from .config import (
    ConfigManager,
    ConfigError,
    ConfigValidationError,
    ConfigLoadError
)

from .logging import (
    setup_logging,
    get_logger,
    JsonFormatter,
    LoggerAdapter,
    ContextLogger
)

from .validation import (
    ValidationError,
    validate_type,
    validate_required,
    validate_length,
    validate_pattern,
    validate_range,
    validate_email,
    validate_phone,
    validate_date,
    validate_list,
    validate_dict,
    validate_dataclass,
    validator
)

__all__ = [
    # Configuration
    'ConfigManager',
    'ConfigError',
    'ConfigValidationError',
    'ConfigLoadError',
    
    # Logging
    'setup_logging',
    'get_logger',
    'JsonFormatter',
    'LoggerAdapter',
    'ContextLogger',
    
    # Validation
    'ValidationError',
    'validate_type',
    'validate_required',
    'validate_length',
    'validate_pattern',
    'validate_range',
    'validate_email',
    'validate_phone',
    'validate_date',
    'validate_list',
    'validate_dict',
    'validate_dataclass',
    'validator'
]

"""
OpenMatch Utils Module - Utility functions and helpers.
"""

import logging
from typing import Optional

def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    format_str: str = '%(asctime)s [%(levelname)s] %(message)s'
) -> logging.Logger:
    """Set up logging configuration.
    
    Args:
        level: Logging level
        log_file: Optional log file path
        format_str: Log message format string
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger('openmatch')
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(format_str)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if log file specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
