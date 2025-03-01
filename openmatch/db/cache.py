"""
Caching layer for database operations.
"""
from typing import Any, Dict, Optional, Union, Callable
import logging
import time
import json
from abc import ABC, abstractmethod
from functools import wraps

from ..hub.config import CacheConfig
from ..hub.exceptions import CacheError


class CacheBackend(ABC):
    """Abstract base class for cache backends."""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Retrieve a value from cache."""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Store a value in cache."""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete a value from cache."""
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """Clear all values from cache."""
        pass
    
    @abstractmethod
    async def ping(self) -> bool:
        """Check if cache is available."""
        pass


class MemoryCache(CacheBackend):
    """In-memory cache implementation."""
    
    def __init__(self, max_size: int = 1000):
        self._cache: Dict[str, tuple[Any, Optional[float]]] = {}
        self._max_size = max_size
        self._logger = logging.getLogger(__name__)
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        if key not in self._cache:
            return None
            
        value, expiry = self._cache[key]
        if expiry and time.time() > expiry:
            del self._cache[key]
            return None
            
        return value
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Store a value in cache.
        
        Args:
            key: Cache key
            value: Value to store
            ttl: Time to live in seconds
            
        Raises:
            CacheError: If cache is full
        """
        if len(self._cache) >= self._max_size:
            # Evict oldest entries
            sorted_items = sorted(self._cache.items(), key=lambda x: x[1][1] or float('inf'))
            for old_key, _ in sorted_items[:len(self._cache) // 4]:
                del self._cache[old_key]
                
        expiry = time.time() + ttl if ttl else None
        self._cache[key] = (value, expiry)
    
    async def delete(self, key: str) -> None:
        """
        Delete a value from cache.
        
        Args:
            key: Cache key
        """
        self._cache.pop(key, None)
    
    async def clear(self) -> None:
        """Clear all values from cache."""
        self._cache.clear()
    
    async def ping(self) -> bool:
        """Check if cache is available."""
        return True


class CacheManager:
    """
    Cache manager that handles caching operations and backend selection.
    """
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self._backend: Optional[CacheBackend] = None
        self._logger = logging.getLogger(__name__)
        
        if not self.config.enabled:
            return
            
        if self.config.backend == "memory":
            self._backend = MemoryCache(max_size=self.config.max_size)
        else:
            raise CacheError(f"Unsupported cache backend: {self.config.backend}")
    
    def _generate_key(self, *args: Any, **kwargs: Any) -> str:
        """Generate a cache key from arguments."""
        key_parts = [str(arg) for arg in args]
        key_parts.extend(f"{k}:{v}" for k, v in sorted(kwargs.items()))
        return json.dumps(key_parts)
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found or cache is disabled
        """
        if not self.config.enabled or not self._backend:
            return None
            
        try:
            return await self._backend.get(key)
        except Exception as e:
            self._logger.warning(f"Cache get failed: {str(e)}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Store a value in cache.
        
        Args:
            key: Cache key
            value: Value to store
            ttl: Time to live in seconds (overrides default)
        """
        if not self.config.enabled or not self._backend:
            return
            
        try:
            await self._backend.set(key, value, ttl or self.config.ttl)
        except Exception as e:
            self._logger.warning(f"Cache set failed: {str(e)}")
    
    async def delete(self, key: str) -> None:
        """
        Delete a value from cache.
        
        Args:
            key: Cache key
        """
        if not self.config.enabled or not self._backend:
            return
            
        try:
            await self._backend.delete(key)
        except Exception as e:
            self._logger.warning(f"Cache delete failed: {str(e)}")
    
    async def clear(self) -> None:
        """Clear all values from cache."""
        if not self.config.enabled or not self._backend:
            return
            
        try:
            await self._backend.clear()
        except Exception as e:
            self._logger.warning(f"Cache clear failed: {str(e)}")
    
    async def ping(self) -> bool:
        """
        Check if cache is available.
        
        Returns:
            bool: True if cache is available and enabled
        """
        if not self.config.enabled or not self._backend:
            return False
            
        try:
            return await self._backend.ping()
        except Exception:
            return False
    
    def cached(self, ttl: Optional[int] = None) -> Callable:
        """
        Decorator for caching function results.
        
        Args:
            ttl: Optional TTL override
            
        Returns:
            Decorated function
            
        Usage:
            @cache_manager.cached(ttl=300)
            async def get_user(user_id: int) -> Dict:
                # Function implementation
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                if not self.config.enabled or not self._backend:
                    return await func(*args, **kwargs)
                    
                cache_key = self._generate_key(func.__name__, *args, **kwargs)
                cached_value = await self.get(cache_key)
                
                if cached_value is not None:
                    return cached_value
                    
                result = await func(*args, **kwargs)
                await self.set(cache_key, result, ttl)
                return result
                
            return wrapper
        return decorator
