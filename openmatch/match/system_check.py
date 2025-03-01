"""
System requirement checks and model selection for OpenMatch.
"""

import os
import psutil
import logging
from enum import Enum
from typing import Dict, Optional, Tuple
import torch

logger = logging.getLogger(__name__)

class SystemTier(Enum):
    """System capability tiers based on available resources."""
    LOW = "low"      # Limited resources, use smallest models
    MEDIUM = "medium"  # Moderate resources, use balanced models
    HIGH = "high"    # High resources, can use large models

# Model configurations with their requirements and characteristics
EMBEDDING_MODELS = {
    # Tiny models for very limited resources
    'paraphrase-MiniLM-L3-v2': {
        'size_mb': 61,
        'dimension': 384,
        'min_ram_gb': 2,
        'performance': 'basic',
        'tier': SystemTier.LOW,
        'description': 'Extremely lightweight model, good for basic text matching'
    },
    'all-MiniLM-L6-v2': {
        'size_mb': 91,
        'dimension': 384,
        'min_ram_gb': 4,
        'performance': 'good',
        'tier': SystemTier.LOW,
        'description': 'Small but effective model, good balance of size and performance'
    },
    # Medium-sized models
    'all-mpnet-base-v2': {
        'size_mb': 420,
        'dimension': 768,
        'min_ram_gb': 8,
        'performance': 'very good',
        'tier': SystemTier.MEDIUM,
        'description': 'Well-balanced model with good performance across many tasks'
    },
    'multi-qa-mpnet-base-dot-v1': {
        'size_mb': 420,
        'dimension': 768,
        'min_ram_gb': 8,
        'performance': 'very good',
        'tier': SystemTier.MEDIUM,
        'description': 'Optimized for question-answering and semantic search'
    },
    # Larger models for better performance
    'all-roberta-large-v1': {
        'size_mb': 1340,
        'dimension': 1024,
        'min_ram_gb': 16,
        'performance': 'excellent',
        'tier': SystemTier.HIGH,
        'description': 'High-performance model for demanding applications'
    }
}

def check_system_resources() -> Dict:
    """Check available system resources.
    
    Returns:
        Dict containing system specifications
    """
    try:
        cpu_count = psutil.cpu_count(logical=False)
        total_ram = psutil.virtual_memory().total / (1024 ** 3)  # Convert to GB
        available_ram = psutil.virtual_memory().available / (1024 ** 3)
        gpu_available = torch.cuda.is_available()
        if gpu_available:
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / (1024 ** 3)
        else:
            gpu_memory = 0
            
        return {
            'cpu_cores': cpu_count,
            'total_ram_gb': round(total_ram, 1),
            'available_ram_gb': round(available_ram, 1),
            'gpu_available': gpu_available,
            'gpu_memory_gb': round(gpu_memory, 1) if gpu_available else 0
        }
    except Exception as e:
        logger.error(f"Error checking system resources: {e}")
        return {}

def determine_system_tier(resources: Dict) -> SystemTier:
    """Determine the system capability tier based on available resources.
    
    Args:
        resources: Dictionary of system resources
        
    Returns:
        SystemTier enum value
    """
    available_ram = resources.get('available_ram_gb', 0)
    gpu_available = resources.get('gpu_available', False)
    cpu_cores = resources.get('cpu_cores', 1)
    
    if gpu_available or (available_ram >= 16 and cpu_cores >= 4):
        return SystemTier.HIGH
    elif available_ram >= 8 and cpu_cores >= 2:
        return SystemTier.MEDIUM
    else:
        return SystemTier.LOW

def get_recommended_model(resources: Optional[Dict] = None) -> Tuple[str, Dict]:
    """Get the recommended embedding model based on system resources.
    
    Args:
        resources: Optional dictionary of system resources. If not provided,
                 will check system resources automatically.
                 
    Returns:
        Tuple of (model_name, model_config)
    """
    if resources is None:
        resources = check_system_resources()
        
    tier = determine_system_tier(resources)
    available_ram = resources.get('available_ram_gb', 0)
    
    # Filter models by tier and RAM requirements
    suitable_models = {
        name: config for name, config in EMBEDDING_MODELS.items()
        if config['tier'] == tier and config['min_ram_gb'] <= available_ram
    }
    
    if not suitable_models:
        # Fallback to the smallest model if no suitable models found
        logger.warning("No suitable models found for system resources, using smallest model")
        return 'paraphrase-MiniLM-L3-v2', EMBEDDING_MODELS['paraphrase-MiniLM-L3-v2']
    
    # Return the model with the best performance in the suitable tier
    best_model = max(suitable_models.items(), key=lambda x: len(x[1]['performance']))
    return best_model

def validate_model_requirements(model_name: str) -> bool:
    """Validate if the system meets the requirements for a specific model.
    
    Args:
        model_name: Name of the model to validate
        
    Returns:
        bool: True if system meets requirements, False otherwise
    """
    if model_name not in EMBEDDING_MODELS:
        logger.warning(f"Unknown model: {model_name}")
        return False
        
    resources = check_system_resources()
    model_config = EMBEDDING_MODELS[model_name]
    
    if resources.get('available_ram_gb', 0) < model_config['min_ram_gb']:
        logger.warning(
            f"Insufficient RAM. Model {model_name} requires {model_config['min_ram_gb']}GB, "
            f"but only {resources['available_ram_gb']}GB available"
        )
        return False
        
    return True

def print_model_recommendations():
    """Print recommended models for different system tiers."""
    resources = check_system_resources()
    tier = determine_system_tier(resources)
    
    print("\nSystem Resources:")
    print(f"CPU Cores: {resources['cpu_cores']}")
    print(f"Total RAM: {resources['total_ram_gb']:.1f}GB")
    print(f"Available RAM: {resources['available_ram_gb']:.1f}GB")
    print(f"GPU Available: {resources['gpu_available']}")
    if resources['gpu_available']:
        print(f"GPU Memory: {resources['gpu_memory_gb']:.1f}GB")
    print(f"\nSystem Tier: {tier.value.upper()}")
    
    print("\nRecommended Models:")
    for name, config in EMBEDDING_MODELS.items():
        if config['min_ram_gb'] <= resources['available_ram_gb']:
            print(f"\n{name}:")
            print(f"  Size: {config['size_mb']}MB")
            print(f"  Dimension: {config['dimension']}")
            print(f"  Min RAM: {config['min_ram_gb']}GB")
            print(f"  Performance: {config['performance']}")
            print(f"  Description: {config['description']}")
            
    recommended_model, config = get_recommended_model(resources)
    print(f"\nBest Model for Your System: {recommended_model}")
    print(f"Description: {config['description']}") 