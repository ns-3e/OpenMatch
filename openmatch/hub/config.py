"""
Configuration management for OpenMatch Hub.
"""
from typing import Any, Dict, Optional
from pathlib import Path
import os
import yaml
from pydantic import BaseModel, Field
import logging

from .exceptions import ConfigurationError


class DatabaseConfig(BaseModel):
    """Database configuration settings."""
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    pool_min_size: int = Field(default=1, description="Minimum pool size")
    pool_max_size: int = Field(default=10, description="Maximum pool size")
    ssl_mode: Optional[str] = Field(default=None, description="SSL mode")


class CacheConfig(BaseModel):
    """Cache configuration settings."""
    enabled: bool = Field(default=True, description="Enable caching")
    backend: str = Field(default="memory", description="Cache backend type")
    ttl: int = Field(default=300, description="Default TTL in seconds")
    max_size: int = Field(default=1000, description="Maximum cache size")


class LoggingConfig(BaseModel):
    """Logging configuration settings."""
    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                       description="Log format")
    file: Optional[str] = Field(default=None, description="Log file path")


class OpenMatchConfig(BaseModel):
    """Main configuration class for OpenMatch."""
    database: DatabaseConfig
    cache: CacheConfig = Field(default_factory=CacheConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    debug: bool = Field(default=False, description="Debug mode")
    environment: str = Field(default="production", description="Environment name")


class ConfigManager:
    """Configuration manager for OpenMatch."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'config'):
            self.config = None
            self._logger = logging.getLogger(__name__)
    
    @classmethod
    def load_config(cls, config_path: Optional[str] = None) -> OpenMatchConfig:
        """
        Load configuration from file and/or environment variables.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            OpenMatchConfig instance
            
        Raises:
            ConfigurationError: If configuration loading fails
        """
        instance = cls()
        
        try:
            # Load from file if provided
            config_data = {}
            if config_path:
                config_file = Path(config_path)
                if not config_file.exists():
                    raise ConfigurationError(f"Configuration file not found: {config_path}")
                
                with open(config_file, 'r') as f:
                    config_data = yaml.safe_load(f)
            
            # Override with environment variables
            config_data = instance._override_from_env(config_data)
            
            # Create config instance
            instance.config = OpenMatchConfig(**config_data)
            instance._setup_logging()
            
            return instance.config
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {str(e)}")
    
    def _override_from_env(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Override configuration with environment variables."""
        env_mapping = {
            "OPENMATCH_DB_HOST": ("database", "host"),
            "OPENMATCH_DB_PORT": ("database", "port"),
            "OPENMATCH_DB_NAME": ("database", "database"),
            "OPENMATCH_DB_USER": ("database", "username"),
            "OPENMATCH_DB_PASS": ("database", "password"),
            "OPENMATCH_CACHE_ENABLED": ("cache", "enabled"),
            "OPENMATCH_LOG_LEVEL": ("logging", "level"),
            "OPENMATCH_DEBUG": ("debug",),
            "OPENMATCH_ENV": ("environment",),
        }
        
        for env_var, config_path in env_mapping.items():
            if env_var in os.environ:
                current = config
                for i, key in enumerate(config_path):
                    if i == len(config_path) - 1:
                        current[key] = self._convert_env_value(os.environ[env_var])
                    else:
                        current = current.setdefault(key, {})
        
        return config
    
    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type."""
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value
    
    def _setup_logging(self) -> None:
        """Setup logging based on configuration."""
        if not self.config:
            return
            
        logging.basicConfig(
            level=self.config.logging.level,
            format=self.config.logging.format,
            filename=self.config.logging.file
        )
    
    @property
    def current_config(self) -> Optional[OpenMatchConfig]:
        """Get current configuration."""
        return self.config
