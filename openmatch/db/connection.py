"""
Database connection management for OpenMatch.
"""
from typing import Optional, Any, Dict
import asyncio
import logging
from contextlib import asynccontextmanager

import asyncpg
from ..hub.config import DatabaseConfig
from ..hub.exceptions import ConnectionError, DatabaseError


class Connection:
    """
    Wrapper around database connection with additional functionality.
    """
    
    def __init__(self, connection: asyncpg.Connection):
        self._connection = connection
        self._logger = logging.getLogger(__name__)
        self._transaction_depth = 0
    
    async def execute(self, query: str, *args, timeout: Optional[float] = None) -> str:
        """
        Execute a query that doesn't return rows.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            Command completion status string
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            return await self._connection.execute(query, *args, timeout=timeout)
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {str(e)}")
    
    async def fetch(self, query: str, *args, timeout: Optional[float] = None) -> list:
        """
        Execute a query and return all rows.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            List of Record objects
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            return await self._connection.fetch(query, *args, timeout=timeout)
        except Exception as e:
            raise DatabaseError(f"Query fetch failed: {str(e)}")
    
    async def fetchrow(self, query: str, *args, timeout: Optional[float] = None) -> Optional[asyncpg.Record]:
        """
        Execute a query and return the first row.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            Record object or None if no rows returned
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            return await self._connection.fetchrow(query, *args, timeout=timeout)
        except Exception as e:
            raise DatabaseError(f"Query fetchrow failed: {str(e)}")
    
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
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            return await self._connection.fetchval(query, *args, column=column, timeout=timeout)
        except Exception as e:
            raise DatabaseError(f"Query fetchval failed: {str(e)}")
    
    @asynccontextmanager
    async def transaction(self):
        """
        Start a new transaction.
        
        Usage:
            async with connection.transaction():
                # Execute queries within transaction
        """
        self._transaction_depth += 1
        if self._transaction_depth == 1:
            transaction = self._connection.transaction()
            await transaction.start()
        try:
            yield self
        except Exception:
            if self._transaction_depth == 1:
                await transaction.rollback()
            raise
        else:
            if self._transaction_depth == 1:
                await transaction.commit()
        finally:
            self._transaction_depth -= 1
    
    async def close(self) -> None:
        """Close the database connection."""
        await self._connection.close()
    
    @property
    def is_closed(self) -> bool:
        """Check if the connection is closed."""
        return self._connection.is_closed()
    
    async def ping(self) -> bool:
        """
        Check if the connection is alive.
        
        Returns:
            bool: True if connection is alive, False otherwise
        """
        try:
            await self.fetchval("SELECT 1")
            return True
        except Exception:
            return False


class ConnectionPool:
    """
    Database connection pool manager.
    """
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._pool: Optional[asyncpg.Pool] = None
        self._logger = logging.getLogger(__name__)
    
    async def initialize(self) -> None:
        """
        Initialize the connection pool.
        
        Raises:
            ConnectionError: If pool initialization fails
        """
        try:
            self._pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.username,
                password=self.config.password,
                database=self.config.database,
                min_size=self.config.pool_min_size,
                max_size=self.config.pool_max_size,
                ssl=self.config.ssl_mode == "require"
            )
        except Exception as e:
            raise ConnectionError(f"Failed to initialize connection pool: {str(e)}")
    
    @asynccontextmanager
    async def acquire(self) -> Connection:
        """
        Acquire a connection from the pool.
        
        Returns:
            Connection wrapper
            
        Raises:
            ConnectionError: If connection acquisition fails
        """
        if not self._pool:
            raise ConnectionError("Connection pool not initialized")
            
        try:
            async with self._pool.acquire() as connection:
                yield Connection(connection)
        except Exception as e:
            raise ConnectionError(f"Failed to acquire connection: {str(e)}")
    
    async def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            await self._pool.close()
    
    async def ping(self) -> bool:
        """
        Check if the pool is healthy by testing a connection.
        
        Returns:
            bool: True if pool is healthy, False otherwise
        """
        try:
            async with self.acquire() as conn:
                return await conn.ping()
        except Exception:
            return False
