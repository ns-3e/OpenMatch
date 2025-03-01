"""
Transaction management for OpenMatch database operations.
"""
from typing import Optional, Any, Callable, TypeVar, Awaitable
import logging
from contextlib import asynccontextmanager

from .connection import Connection
from ..hub.exceptions import TransactionError

T = TypeVar('T')


class Transaction:
    """
    Transaction management class that provides isolation and atomicity.
    """
    
    def __init__(self, connection: Connection):
        self._connection = connection
        self._logger = logging.getLogger(__name__)
        self._savepoint_id = 0
    
    async def execute(self, query: str, *args, timeout: Optional[float] = None) -> str:
        """
        Execute a query within the transaction.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            Command completion status string
        """
        return await self._connection.execute(query, *args, timeout=timeout)
    
    async def fetch(self, query: str, *args, timeout: Optional[float] = None) -> list:
        """
        Execute a query and return all rows within the transaction.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            List of Record objects
        """
        return await self._connection.fetch(query, *args, timeout=timeout)
    
    async def fetchrow(self, query: str, *args, timeout: Optional[float] = None) -> Any:
        """
        Execute a query and return the first row within the transaction.
        
        Args:
            query: SQL query string
            *args: Query parameters
            timeout: Optional timeout in seconds
            
        Returns:
            Record object or None if no rows returned
        """
        return await self._connection.fetchrow(query, *args, timeout=timeout)
    
    async def fetchval(self, query: str, *args, column: int = 0, timeout: Optional[float] = None) -> Any:
        """
        Execute a query and return a single value within the transaction.
        
        Args:
            query: SQL query string
            *args: Query parameters
            column: Zero-based index of the column to return
            timeout: Optional timeout in seconds
            
        Returns:
            First value of the first row
        """
        return await self._connection.fetchval(query, *args, column=column, timeout=timeout)
    
    @asynccontextmanager
    async def savepoint(self):
        """
        Create a savepoint within the current transaction.
        
        Usage:
            async with transaction.savepoint():
                # Execute queries within savepoint
        """
        self._savepoint_id += 1
        savepoint_name = f"sp_{self._savepoint_id}"
        
        try:
            await self.execute(f"SAVEPOINT {savepoint_name}")
            yield self
        except Exception:
            await self.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            raise
        else:
            await self.execute(f"RELEASE SAVEPOINT {savepoint_name}")
    
    async def run_in_transaction(self, func: Callable[['Transaction'], Awaitable[T]]) -> T:
        """
        Execute a function within the transaction context.
        
        Args:
            func: Async function that takes a Transaction instance and returns a value
            
        Returns:
            The value returned by the function
            
        Raises:
            TransactionError: If the transaction fails
        """
        try:
            return await func(self)
        except Exception as e:
            raise TransactionError(f"Transaction operation failed: {str(e)}")


class TransactionManager:
    """
    Manager class for handling database transactions.
    """
    
    def __init__(self, connection: Connection):
        self._connection = connection
        self._logger = logging.getLogger(__name__)
    
    @asynccontextmanager
    async def transaction(self):
        """
        Start a new transaction.
        
        Usage:
            async with transaction_manager.transaction() as txn:
                # Execute queries within transaction
        """
        async with self._connection.transaction() as conn:
            yield Transaction(conn)
    
    async def run_in_transaction(self, func: Callable[[Transaction], Awaitable[T]]) -> T:
        """
        Execute a function within a new transaction.
        
        Args:
            func: Async function that takes a Transaction instance and returns a value
            
        Returns:
            The value returned by the function
            
        Raises:
            TransactionError: If the transaction fails
        """
        async with self.transaction() as txn:
            return await txn.run_in_transaction(func)
    
    @asynccontextmanager
    async def savepoint(self):
        """
        Create a savepoint within the current transaction.
        
        Usage:
            async with transaction_manager.savepoint():
                # Execute queries within savepoint
        """
        async with self.transaction() as txn:
            async with txn.savepoint() as sp:
                yield sp
