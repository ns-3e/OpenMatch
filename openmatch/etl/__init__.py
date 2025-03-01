"""
OpenMatch ETL Package

This package provides functionality for extracting, transforming, and loading data
from source systems into the MDM database.
"""

from .manager import SourceSystemManager

__all__ = ['SourceSystemManager'] 