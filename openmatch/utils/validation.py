from typing import Any, Callable, Dict, List, Optional, Type, Union
from functools import wraps
import re
from datetime import datetime
import inspect
import phonenumbers
from dataclasses import is_dataclass, asdict

class ValidationError(Exception):
    """Base exception for validation errors."""
    pass

def validate_type(value: Any, expected_type: Type) -> None:
    """Validate value type.
    
    Args:
        value: Value to validate
        expected_type: Expected type
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(value, expected_type):
        raise ValidationError(
            f"Expected type {expected_type.__name__}, "
            f"got {type(value).__name__}"
        )

def validate_required(value: Any) -> None:
    """Validate required value.
    
    Args:
        value: Value to validate
        
    Raises:
        ValidationError: If validation fails
    """
    if value is None:
        raise ValidationError("Value is required")

def validate_length(
    value: str,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None
) -> None:
    """Validate string length.
    
    Args:
        value: String to validate
        min_length: Minimum length
        max_length: Maximum length
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(value, str):
        raise ValidationError("Value must be a string")
        
    length = len(value)
    if min_length is not None and length < min_length:
        raise ValidationError(f"Length must be at least {min_length}")
    if max_length is not None and length > max_length:
        raise ValidationError(f"Length must be at most {max_length}")

def validate_pattern(value: str, pattern: str) -> None:
    """Validate string pattern.
    
    Args:
        value: String to validate
        pattern: Regex pattern
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(value, str):
        raise ValidationError("Value must be a string")
        
    if not re.match(pattern, value):
        raise ValidationError(f"Value does not match pattern: {pattern}")

def validate_range(
    value: Union[int, float],
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None
) -> None:
    """Validate numeric range.
    
    Args:
        value: Number to validate
        min_value: Minimum value
        max_value: Maximum value
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(value, (int, float)):
        raise ValidationError("Value must be a number")
        
    if min_value is not None and value < min_value:
        raise ValidationError(f"Value must be at least {min_value}")
    if max_value is not None and value > max_value:
        raise ValidationError(f"Value must be at most {max_value}")

def validate_email(value: str) -> None:
    """Validate email address.
    
    Args:
        value: Email to validate
        
    Raises:
        ValidationError: If validation fails
    """
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    try:
        validate_pattern(value, pattern)
    except ValidationError:
        raise ValidationError("Invalid email address")

def validate_phone(value: str, region: str = "US") -> None:
    """Validate phone number.
    
    Args:
        value: Phone number to validate
        region: Region code
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        number = phonenumbers.parse(value, region)
        if not phonenumbers.is_valid_number(number):
            raise ValidationError("Invalid phone number")
    except phonenumbers.NumberParseException:
        raise ValidationError("Invalid phone number format")

def validate_date(
    value: str,
    format: str = "%Y-%m-%d",
    min_date: Optional[datetime] = None,
    max_date: Optional[datetime] = None
) -> None:
    """Validate date string.
    
    Args:
        value: Date string to validate
        format: Date format
        min_date: Minimum date
        max_date: Maximum date
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        date = datetime.strptime(value, format)
        if min_date and date < min_date:
            raise ValidationError(f"Date must be after {min_date}")
        if max_date and date > max_date:
            raise ValidationError(f"Date must be before {max_date}")
    except ValueError:
        raise ValidationError(f"Invalid date format, expected {format}")

def validate_list(
    value: List[Any],
    item_validator: Optional[Callable[[Any], None]] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None
) -> None:
    """Validate list and optionally its items.
    
    Args:
        value: List to validate
        item_validator: Optional function to validate items
        min_length: Minimum length
        max_length: Maximum length
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(value, list):
        raise ValidationError("Value must be a list")
        
    length = len(value)
    if min_length is not None and length < min_length:
        raise ValidationError(f"List must have at least {min_length} items")
    if max_length is not None and length > max_length:
        raise ValidationError(f"List must have at most {max_length} items")
        
    if item_validator:
        for i, item in enumerate(value):
            try:
                item_validator(item)
            except ValidationError as e:
                raise ValidationError(f"Invalid item at index {i}: {str(e)}")

def validate_dict(
    value: Dict[str, Any],
    schema: Dict[str, Dict[str, Any]]
) -> None:
    """Validate dictionary against schema.
    
    Args:
        value: Dictionary to validate
        schema: Validation schema
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(value, dict):
        raise ValidationError("Value must be a dictionary")
        
    for key, field_schema in schema.items():
        # Check required fields
        if field_schema.get("required", False) and key not in value:
            raise ValidationError(f"Missing required field: {key}")
            
        if key in value:
            field_value = value[key]
            
            # Validate type
            if "type" in field_schema:
                validate_type(field_value, field_schema["type"])
                
            # Validate other constraints
            for validator_name, validator_args in field_schema.items():
                if validator_name == "type" or validator_name == "required":
                    continue
                    
                validator = globals().get(f"validate_{validator_name}")
                if validator:
                    if isinstance(validator_args, dict):
                        validator(field_value, **validator_args)
                    else:
                        validator(field_value, validator_args)

def validate_dataclass(value: Any) -> None:
    """Validate dataclass instance.
    
    Args:
        value: Instance to validate
        
    Raises:
        ValidationError: If validation fails
    """
    if not is_dataclass(value):
        raise ValidationError("Value must be a dataclass instance")
        
    # Get field validators from class
    validators = getattr(value, "__validators__", {})
    
    # Convert to dictionary
    data = asdict(value)
    
    # Validate each field
    for field_name, field_value in data.items():
        validator = validators.get(field_name)
        if validator:
            try:
                validator(field_value)
            except ValidationError as e:
                raise ValidationError(f"Invalid field {field_name}: {str(e)}")

def validator(
    *validators: Callable,
    **validator_kwargs: Dict[str, Any]
) -> Callable:
    """Decorator to validate function arguments.
    
    Args:
        *validators: Validator functions
        **validator_kwargs: Validator arguments
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get function signature
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            
            # Validate each argument
            for param_name, param_value in bound_args.arguments.items():
                # Get validators for parameter
                param_validators = []
                
                # Add explicit validators
                for v in validators:
                    if param_name in validator_kwargs.get(v.__name__, {}):
                        param_validators.append(
                            lambda x, v=v, k=validator_kwargs[v.__name__][param_name]:
                                v(x, **k) if isinstance(k, dict) else v(x, k)
                        )
                
                # Run validators
                for validate in param_validators:
                    try:
                        validate(param_value)
                    except ValidationError as e:
                        raise ValidationError(
                            f"Invalid argument {param_name}: {str(e)}"
                        )
                        
            return func(*args, **kwargs)
        return wrapper
    return decorator
