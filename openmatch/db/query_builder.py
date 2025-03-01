"""
Query builder module for safe SQL query construction.
"""
from typing import Any, Dict, List, Optional, Union, Tuple
import re
from enum import Enum

from ..hub.exceptions import ValidationError


class JoinType(Enum):
    """Enumeration of SQL join types."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL JOIN"


class OrderDirection(Enum):
    """Enumeration of SQL order directions."""
    ASC = "ASC"
    DESC = "DESC"


class QueryBuilder:
    """
    SQL query builder with safe parameter handling.
    """
    
    def __init__(self):
        self._table = ""
        self._columns: List[str] = []
        self._joins: List[Tuple[str, str, JoinType, str]] = []
        self._where_conditions: List[str] = []
        self._group_by: List[str] = []
        self._having_conditions: List[str] = []
        self._order_by: List[Tuple[str, OrderDirection]] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._parameters: Dict[str, Any] = {}
        self._param_counter = 0
    
    def _add_parameter(self, value: Any) -> str:
        """Add a parameter and return its placeholder name."""
        self._param_counter += 1
        param_name = f"p_{self._param_counter}"
        self._parameters[param_name] = value
        return f"${param_name}"
    
    def _validate_identifier(self, identifier: str) -> None:
        """Validate SQL identifier to prevent SQL injection."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValidationError(f"Invalid SQL identifier: {identifier}")
    
    def select(self, *columns: str) -> 'QueryBuilder':
        """
        Set the columns to select.
        
        Args:
            *columns: Column names to select
            
        Returns:
            Self for method chaining
        """
        self._columns = list(columns) if columns else ["*"]
        return self
    
    def from_(self, table: str) -> 'QueryBuilder':
        """
        Set the table to select from.
        
        Args:
            table: Table name
            
        Returns:
            Self for method chaining
        """
        self._validate_identifier(table)
        self._table = table
        return self
    
    def join(self, table: str, on: str, join_type: JoinType = JoinType.INNER, alias: Optional[str] = None) -> 'QueryBuilder':
        """
        Add a join clause.
        
        Args:
            table: Table to join
            on: Join condition
            join_type: Type of join
            alias: Optional table alias
            
        Returns:
            Self for method chaining
        """
        self._validate_identifier(table)
        if alias:
            self._validate_identifier(alias)
        self._joins.append((table, on, join_type, alias or ""))
        return self
    
    def where(self, condition: str, *values: Any) -> 'QueryBuilder':
        """
        Add a WHERE condition.
        
        Args:
            condition: WHERE condition with placeholders
            *values: Values for the placeholders
            
        Returns:
            Self for method chaining
        """
        placeholders = [self._add_parameter(value) for value in values]
        condition = condition.format(*placeholders)
        self._where_conditions.append(condition)
        return self
    
    def group_by(self, *columns: str) -> 'QueryBuilder':
        """
        Add GROUP BY columns.
        
        Args:
            *columns: Column names to group by
            
        Returns:
            Self for method chaining
        """
        for column in columns:
            self._validate_identifier(column)
        self._group_by.extend(columns)
        return self
    
    def having(self, condition: str, *values: Any) -> 'QueryBuilder':
        """
        Add a HAVING condition.
        
        Args:
            condition: HAVING condition with placeholders
            *values: Values for the placeholders
            
        Returns:
            Self for method chaining
        """
        placeholders = [self._add_parameter(value) for value in values]
        condition = condition.format(*placeholders)
        self._having_conditions.append(condition)
        return self
    
    def order_by(self, column: str, direction: OrderDirection = OrderDirection.ASC) -> 'QueryBuilder':
        """
        Add ORDER BY clause.
        
        Args:
            column: Column name to order by
            direction: Sort direction
            
        Returns:
            Self for method chaining
        """
        self._validate_identifier(column)
        self._order_by.append((column, direction))
        return self
    
    def limit(self, limit: int) -> 'QueryBuilder':
        """
        Set LIMIT clause.
        
        Args:
            limit: Maximum number of rows to return
            
        Returns:
            Self for method chaining
        """
        if limit < 0:
            raise ValidationError("Limit must be non-negative")
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> 'QueryBuilder':
        """
        Set OFFSET clause.
        
        Args:
            offset: Number of rows to skip
            
        Returns:
            Self for method chaining
        """
        if offset < 0:
            raise ValidationError("Offset must be non-negative")
        self._offset = offset
        return self
    
    def build(self) -> Tuple[str, Dict[str, Any]]:
        """
        Build the SQL query and parameters.
        
        Returns:
            Tuple of (query string, parameters dict)
            
        Raises:
            ValidationError: If query is invalid
        """
        if not self._table:
            raise ValidationError("No table specified")
            
        query_parts = ["SELECT", ", ".join(self._columns), "FROM", self._table]
        
        # Add joins
        for table, on, join_type, alias in self._joins:
            join_clause = [join_type.value, table]
            if alias:
                join_clause.append(f"AS {alias}")
            join_clause.extend(["ON", on])
            query_parts.extend(join_clause)
        
        # Add WHERE clause
        if self._where_conditions:
            query_parts.extend(["WHERE", " AND ".join(f"({condition})" for condition in self._where_conditions)])
        
        # Add GROUP BY clause
        if self._group_by:
            query_parts.extend(["GROUP BY", ", ".join(self._group_by)])
        
        # Add HAVING clause
        if self._having_conditions:
            query_parts.extend(["HAVING", " AND ".join(f"({condition})" for condition in self._having_conditions)])
        
        # Add ORDER BY clause
        if self._order_by:
            order_terms = [f"{column} {direction.value}" for column, direction in self._order_by]
            query_parts.extend(["ORDER BY", ", ".join(order_terms)])
        
        # Add LIMIT and OFFSET
        if self._limit is not None:
            query_parts.extend(["LIMIT", str(self._limit)])
        if self._offset is not None:
            query_parts.extend(["OFFSET", str(self._offset)])
        
        return " ".join(query_parts), self._parameters


class InsertBuilder:
    """
    Builder for INSERT queries.
    """
    
    def __init__(self, table: str):
        self._validate_identifier(table)
        self._table = table
        self._columns: List[str] = []
        self._values: List[List[Any]] = []
        self._parameters: Dict[str, Any] = {}
        self._param_counter = 0
        self._returning: List[str] = []
    
    def _validate_identifier(self, identifier: str) -> None:
        """Validate SQL identifier to prevent SQL injection."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValidationError(f"Invalid SQL identifier: {identifier}")
    
    def _add_parameter(self, value: Any) -> str:
        """Add a parameter and return its placeholder name."""
        self._param_counter += 1
        param_name = f"p_{self._param_counter}"
        self._parameters[param_name] = value
        return f"${param_name}"
    
    def columns(self, *columns: str) -> 'InsertBuilder':
        """
        Set the columns for the INSERT statement.
        
        Args:
            *columns: Column names
            
        Returns:
            Self for method chaining
        """
        for column in columns:
            self._validate_identifier(column)
        self._columns = list(columns)
        return self
    
    def values(self, *values: Any) -> 'InsertBuilder':
        """
        Add a row of values.
        
        Args:
            *values: Values to insert
            
        Returns:
            Self for method chaining
        """
        if len(values) != len(self._columns):
            raise ValidationError("Number of values must match number of columns")
        self._values.append(list(values))
        return self
    
    def returning(self, *columns: str) -> 'InsertBuilder':
        """
        Add RETURNING clause.
        
        Args:
            *columns: Column names to return
            
        Returns:
            Self for method chaining
        """
        for column in columns:
            self._validate_identifier(column)
        self._returning = list(columns)
        return self
    
    def build(self) -> Tuple[str, Dict[str, Any]]:
        """
        Build the INSERT query and parameters.
        
        Returns:
            Tuple of (query string, parameters dict)
            
        Raises:
            ValidationError: If query is invalid
        """
        if not self._columns:
            raise ValidationError("No columns specified")
        if not self._values:
            raise ValidationError("No values specified")
            
        query_parts = [
            "INSERT INTO",
            self._table,
            f"({', '.join(self._columns)})",
            "VALUES"
        ]
        
        value_groups = []
        for row in self._values:
            placeholders = [self._add_parameter(value) for value in row]
            value_groups.append(f"({', '.join(placeholders)})")
        
        query_parts.append(", ".join(value_groups))
        
        if self._returning:
            query_parts.extend(["RETURNING", ", ".join(self._returning)])
        
        return " ".join(query_parts), self._parameters
