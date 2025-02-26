from typing import Dict, Any, Optional, Union, Type, TypeVar, List
from pathlib import Path
import yaml
from dataclasses import is_dataclass, asdict
import json
import os
from functools import lru_cache

T = TypeVar('T')

class ConfigError(Exception):
    """Base exception for configuration errors."""
    pass

class ConfigValidationError(ConfigError):
    """Exception raised for configuration validation errors."""
    pass

class ConfigLoadError(ConfigError):
    """Exception raised for configuration loading errors."""
    pass

class ConfigManager:
    """Manager for loading and validating configurations."""
    
    def __init__(self, config_dir: Optional[Union[str, Path]] = None):
        """Initialize config manager.
        
        Args:
            config_dir: Optional directory containing config files
        """
        self.config_dir = Path(config_dir) if config_dir else Path.cwd() / "config"
        self._cache = {}
        
    def load_yaml(self, path: Union[str, Path]) -> Dict[str, Any]:
        """Load YAML configuration file.
        
        Args:
            path: Path to YAML file
            
        Returns:
            Configuration dictionary
            
        Raises:
            ConfigLoadError: If file cannot be loaded
        """
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise ConfigLoadError(f"Failed to load config file {path}: {str(e)}")
            
    @lru_cache(maxsize=128)
    def get_config(
        self,
        name: str,
        config_class: Optional[Type[T]] = None
    ) -> Union[Dict[str, Any], T]:
        """Get configuration by name.
        
        Args:
            name: Configuration name (without extension)
            config_class: Optional class to instantiate
            
        Returns:
            Configuration dictionary or instance
            
        Raises:
            ConfigLoadError: If configuration cannot be loaded
        """
        # Check cache first
        cache_key = f"{name}_{config_class.__name__ if config_class else 'dict'}"
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # Find config file
        config_file = self.config_dir / f"{name}.yaml"
        if not config_file.exists():
            config_file = self.config_dir / f"{name}.yml"
            
        if not config_file.exists():
            raise ConfigLoadError(f"Configuration file not found: {name}")
            
        # Load config
        config_dict = self.load_yaml(config_file)
        
        # Convert to class instance if specified
        if config_class:
            try:
                if hasattr(config_class, "from_dict"):
                    config = config_class.from_dict(config_dict)
                else:
                    config = config_class(**config_dict)
                self._cache[cache_key] = config
                return config
            except Exception as e:
                raise ConfigValidationError(
                    f"Failed to create {config_class.__name__} instance: {str(e)}"
                )
                
        self._cache[cache_key] = config_dict
        return config_dict
        
    def save_config(
        self,
        config: Union[Dict[str, Any], Any],
        name: str,
        overwrite: bool = False
    ) -> None:
        """Save configuration to file.
        
        Args:
            config: Configuration dictionary or instance
            name: Configuration name (without extension)
            overwrite: Whether to overwrite existing file
            
        Raises:
            ConfigError: If file exists and overwrite is False
        """
        config_file = self.config_dir / f"{name}.yaml"
        
        # Create config directory if it doesn't exist
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Check if file exists
        if config_file.exists() and not overwrite:
            raise ConfigError(f"Configuration file already exists: {name}")
            
        # Convert instance to dictionary if needed
        if not isinstance(config, dict):
            if is_dataclass(config):
                config = asdict(config)
            elif hasattr(config, "to_dict"):
                config = config.to_dict()
            else:
                config = vars(config)
                
        # Save config
        try:
            with open(config_file, 'w') as f:
                yaml.safe_dump(config, f, default_flow_style=False)
        except Exception as e:
            raise ConfigError(f"Failed to save configuration: {str(e)}")
            
        # Clear cache for this config
        cache_keys = [k for k in self._cache if k.startswith(f"{name}_")]
        for key in cache_keys:
            del self._cache[key]
            
    def merge_configs(
        self,
        base: Union[str, Dict[str, Any]],
        override: Union[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Merge two configurations.
        
        Args:
            base: Base configuration name or dictionary
            override: Override configuration name or dictionary
            
        Returns:
            Merged configuration dictionary
        """
        # Load configs if names provided
        if isinstance(base, str):
            base = self.get_config(base)
        if isinstance(override, str):
            override = self.get_config(override)
            
        def deep_merge(d1: Dict, d2: Dict) -> Dict:
            """Recursively merge dictionaries."""
            result = d1.copy()
            for k, v in d2.items():
                if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                    result[k] = deep_merge(result[k], v)
                else:
                    result[k] = v
            return result
            
        return deep_merge(base, override)
        
    def validate_config(
        self,
        config: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> List[str]:
        """Validate configuration against schema.
        
        Args:
            config: Configuration to validate
            schema: Validation schema
            
        Returns:
            List of validation errors
        """
        errors = []
        
        def validate_value(
            value: Any,
            schema_value: Any,
            path: str
        ) -> None:
            """Recursively validate configuration values."""
            if isinstance(schema_value, dict):
                if not isinstance(value, dict):
                    errors.append(f"{path}: Expected dictionary")
                    return
                    
                # Check required fields
                for k, v in schema_value.items():
                    if k.endswith('?'):
                        # Optional field
                        k = k[:-1]
                        if k in value:
                            validate_value(value[k], v, f"{path}.{k}")
                    else:
                        # Required field
                        if k not in value:
                            errors.append(f"{path}: Missing required field '{k}'")
                        else:
                            validate_value(value[k], v, f"{path}.{k}")
                            
            elif isinstance(schema_value, list):
                if not isinstance(value, list):
                    errors.append(f"{path}: Expected list")
                    return
                    
                # Validate list items
                if schema_value:
                    item_schema = schema_value[0]
                    for i, item in enumerate(value):
                        validate_value(item, item_schema, f"{path}[{i}]")
                        
            elif isinstance(schema_value, type):
                if not isinstance(value, schema_value):
                    errors.append(
                        f"{path}: Expected {schema_value.__name__}, "
                        f"got {type(value).__name__}"
                    )
                    
            elif callable(schema_value):
                try:
                    if not schema_value(value):
                        errors.append(f"{path}: Validation failed")
                except Exception as e:
                    errors.append(f"{path}: Validation error - {str(e)}")
                    
        validate_value(config, schema, "root")
        return errors
        
    def clear_cache(self) -> None:
        """Clear configuration cache."""
        self._cache.clear()
        
    def get_env_config(
        self,
        prefix: str,
        separator: str = "__"
    ) -> Dict[str, Any]:
        """Get configuration from environment variables.
        
        Args:
            prefix: Environment variable prefix
            separator: Nested key separator
            
        Returns:
            Configuration dictionary
        """
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Remove prefix and split into parts
                key = key[len(prefix):].lstrip(separator)
                parts = key.split(separator)
                
                # Try to parse value
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    pass
                    
                # Build nested dictionary
                current = config
                for part in parts[:-1]:
                    current = current.setdefault(part, {})
                current[parts[-1]] = value
                
        return config
