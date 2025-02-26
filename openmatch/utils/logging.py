import logging
import sys
from typing import Optional, Union, Dict, Any
from pathlib import Path
import json
from datetime import datetime

class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def __init__(self, **kwargs):
        """Initialize formatter with optional fields."""
        self.extra_fields = kwargs
        super().__init__()
        
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
            
        # Add extra fields from record
        if hasattr(record, "extra_fields"):
            data.update(record.extra_fields)
            
        # Add configured extra fields
        data.update(self.extra_fields)
        
        return json.dumps(data)

def setup_logging(
    name: str,
    level: Union[str, int] = logging.INFO,
    log_file: Optional[Union[str, Path]] = None,
    json_format: bool = False,
    extra_fields: Optional[Dict[str, Any]] = None,
    propagate: bool = False
) -> logging.Logger:
    """Set up logger with configurable handlers and formatters.
    
    Args:
        name: Logger name
        level: Logging level
        log_file: Optional file path for logging
        json_format: Whether to use JSON formatting
        extra_fields: Optional extra fields to include in JSON logs
        propagate: Whether to propagate to parent loggers
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = propagate
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        
    # Create formatters
    if json_format:
        formatter = JsonFormatter(**(extra_fields or {}))
    else:
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s [%(name)s:%(funcName)s:%(lineno)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    return logger

class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter for adding context to log messages."""
    
    def __init__(self, logger: logging.Logger, extra: Dict[str, Any]):
        """Initialize adapter with logger and extra fields."""
        super().__init__(logger, extra)
        
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Process log message and add extra fields."""
        # Ensure extra_fields exists in kwargs
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        if "extra_fields" not in kwargs["extra"]:
            kwargs["extra"]["extra_fields"] = {}
            
        # Add adapter's extra fields
        kwargs["extra"]["extra_fields"].update(self.extra)
        
        return msg, kwargs

def get_logger(
    name: str,
    context: Optional[Dict[str, Any]] = None,
    **kwargs
) -> Union[logging.Logger, LoggerAdapter]:
    """Get logger with optional context.
    
    Args:
        name: Logger name
        context: Optional context dictionary to add to logs
        **kwargs: Additional arguments for setup_logging
        
    Returns:
        Logger or LoggerAdapter instance
    """
    logger = setup_logging(name, **kwargs)
    
    if context:
        return LoggerAdapter(logger, context)
    return logger

class ContextLogger:
    """Context manager for temporarily adding context to logs."""
    
    def __init__(self, logger: logging.Logger, **context):
        """Initialize with logger and context fields."""
        self.logger = logger
        self.context = context
        self.original_extra = {}
        
    def __enter__(self) -> logging.Logger:
        """Add context when entering."""
        if isinstance(self.logger, LoggerAdapter):
            self.original_extra = self.logger.extra.copy()
            self.logger.extra.update(self.context)
        else:
            self.logger = LoggerAdapter(self.logger, self.context)
        return self.logger
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original context when exiting."""
        if isinstance(self.logger, LoggerAdapter):
            self.logger.extra = self.original_extra
