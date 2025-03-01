"""
Main database manager module for OpenMatch.
"""
from typing import Any, Dict, List, Optional, Union, TypeVar, Callable, Awaitable
import logging
from contextlib import asynccontextmanager

from ..hub.config import DatabaseConfig, CacheConfig
from ..hub.exceptions import DatabaseError, ConnectionError
from .connection import ConnectionPool
from .transaction import TransactionManager
from .cache import CacheManager
from .query_builder import QueryBuilder, InsertBuilder, JoinType, OrderDirection

T = TypeVar('T')


class DBManager:
    """
    Main database manager class that provides a unified interface for database operations.
    """
    
    def __init__(self, config: DatabaseConfig, cache_config: Optional[CacheConfig] = None):
        self.config = config
        self._logger = logging.getLogger(__name__)
        
        self._pool = ConnectionPool(config)
        self._cache = CacheManager(cache_config or CacheConfig())
        
        self._initialized = False
    
    async def initialize(self) -> None:
        """
        Initialize database connections and cache.
        
        Raises:
            ConnectionError: If connection initialization fails
        """
        if self._initialized:
            return
            
        try:
            await self._pool.initialize()
            self._initialized = True
            self._logger.info("Database manager initialized successfully")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize database manager: {str(e)}")
    
    async def shutdown(self) -> None:
        """Gracefully shutdown database connections."""
        if not self._initialized:
            return
            
        await self._pool.close()
        await self._cache.clear()
        self._initialized = False
        self._logger.info("Database manager shut down successfully")
    
    @asynccontextmanager
    async def connection(self):
        """
        Get a database connection from the pool.
        
        Usage:
            async with db_manager.connection() as conn:
                # Use connection
        """
        if not self._initialized:
            raise ConnectionError("Database manager not initialized")
            
        async with self._pool.acquire() as connection:
            yield connection
    
    @asynccontextmanager
    async def transaction(self):
        """
        Start a new transaction.
        
        Usage:
            async with db_manager.transaction() as txn:
                # Execute queries within transaction
        """
        if not self._initialized:
            raise ConnectionError("Database manager not initialized")
            
        async with self._pool.acquire() as connection:
            transaction_manager = TransactionManager(connection)
            async with transaction_manager.transaction() as transaction:
                yield transaction
    
    async def execute(self, query: str, *args, timeout: Optional[float] = None) -> str:
        """
        Execute a query that doesn't return rows.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            Command completion status string
        """
        async with self.connection() as conn:
            return await conn.execute(query, *args, timeout=timeout)
    
    async def fetch(self, query: str, *args, timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return all rows.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            List of dictionaries representing rows
        """
        async with self.connection() as conn:
            return await conn.fetch(query, *args, timeout=timeout)
    
    async def fetchrow(self, query: str, *args, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        Execute a query and return the first row.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            Dictionary representing the row or None if no rows returned
        """
        async with self.connection() as conn:
            return await conn.fetchrow(query, *args, timeout=timeout)
    
    async def fetchval(self, query: str, *args, column: int = 0, timeout: Optional[float] = None) -> Any:
        """
        Execute a query and return a single value.
        
        Args:
            query: SQL query string
            *args: Query parameters
            column: Zero-based index of the column to return
            timeout: Optional timeout in seconds
            
        Returns:
            First value of the first row
        """
        async with self.connection() as conn:
            return await conn.fetchval(query, *args, column=column, timeout=timeout)
    
    def query(self) -> QueryBuilder:
        """
        Create a new query builder instance.
        
        Returns:
            QueryBuilder instance
            
        Usage:
            query = db_manager.query()\\
                .select("id", "name")\\
                .from_("users")\\
                .where("age > {}", 18)
            result = await db_manager.fetch(*query.build())
        """
        return QueryBuilder()
    
    def insert(self, table: str) -> InsertBuilder:
        """
        Create a new insert builder instance.
        
        Args:
            table: Target table name
            
        Returns:
            InsertBuilder instance
            
        Usage:
            insert = db_manager.insert("users")\\
                .columns("name", "age")\\
                .values("John", 30)
            await db_manager.execute(*insert.build())
        """
        return InsertBuilder(table)
    
    async def run_in_transaction(self, func: Callable[['DBManager'], Awaitable[T]]) -> T:
        """
        Execute a function within a transaction context.
        
        Args:
            func: Async function that takes a DBManager instance and returns a value
            
        Returns:
            The value returned by the function
            
        Usage:
            async def create_user(db: DBManager, name: str, age: int) -> int:
                query = db.insert("users")\\
                    .columns("name", "age")\\
                    .values(name, age)\\
                    .returning("id")
                result = await db.fetchval(*query.build())
                return result
                
            user_id = await db_manager.run_in_transaction(
                lambda db: create_user(db, "John", 30)
            )
        """
        async with self.transaction() as transaction:
            return await func(self)
    
    def cached(self, ttl: Optional[int] = None) -> Callable:
        """
        Decorator for caching function results.
        
        Args:
            ttl: Optional TTL override
            
        Returns:
            Decorated function
            
        Usage:
            @db_manager.cached(ttl=300)
            async def get_user(user_id: int) -> Dict:
                return await db_manager.fetchrow(
                    "SELECT * FROM users WHERE id = $1",
                    user_id
                )
        """
        return self._cache.cached(ttl)
    
    async def ping(self) -> bool:
        """
        Check if database connection is alive.
        
        Returns:
            bool: True if connection is alive, False otherwise
        """
        try:
            async with self.connection() as conn:
                return await conn.ping()
        except Exception:
            return False
    
    @property
    def is_initialized(self) -> bool:
        """Check if database manager is initialized."""
        return self._initialized
