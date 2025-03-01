"""
Tests for table generation and record management functionality.
"""

import unittest
from datetime import datetime
import sqlalchemy as sa
import os

from openmatch.model import Model, Field, CharField, DateTimeField, FloatField
from openmatch.model.table_generator import TableGenerator
from openmatch.model.record_manager import RecordManager


class Person(Model):
    """Test person model."""
    first_name = CharField(max_length=100)
    last_name = CharField(max_length=100)
    email = CharField(max_length=255, unique=True)
    birth_date = DateTimeField(null=True)
    
    class Meta:
        table_name = 'person'
        xref = True
        history = True


class TestTableGeneration(unittest.TestCase):
    """Test cases for table generation and record management.
    
    This test suite verifies the functionality of:
    - Table generation from models
    - Record creation and management
    - Index creation
    - Cross-reference table generation
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up test database.
        
        Creates an in-memory SQLite database and initializes the table generator
        and record manager for testing.
        """
        # Use SQLite for testing
        cls.engine = sa.create_engine('sqlite:///:memory:')
        cls.table_generator = TableGenerator(cls.engine, schema='mdm')
        cls.record_manager = RecordManager(cls.engine, schema='mdm')
        
    def setUp(self):
        """Set up test case.
        
        Creates test tables before each test method.
        """
        # Create tables for test
        self.tables = self.table_generator.generate_tables(Person)
        
    def tearDown(self):
        """Clean up after test.
        
        Drops all tables after each test method.
        """
        # Drop all tables
        Person._meta.metadata.drop_all(self.engine)
        
    def test_table_generation(self):
        """Test table generation.
        
        Verifies that:
        - Master and xref tables are created correctly
        - All required columns are present
        - Model-specific columns are added
        - Indexes are created properly
        """
        # Verify master table creation
        self.assertIn('person_master', self.tables)
        master_table = self.tables['master']
        
        # Check required columns
        self.assertIn('record_id', master_table.columns)
        self.assertIn('source_system', master_table.columns)
        self.assertIn('source_id', master_table.columns)
        self.assertIn('created_at', master_table.columns)
        self.assertIn('updated_at', master_table.columns)
        self.assertIn('status', master_table.columns)
        self.assertIn('version', master_table.columns)
        
        # Check model-specific columns
        self.assertIn('first_name', master_table.columns)
        self.assertIn('last_name', master_table.columns)
        self.assertIn('email', master_table.columns)
        self.assertIn('birth_date', master_table.columns)
        
        # Verify xref table creation
        self.assertIn('person_xref', self.tables)
        xref_table = self.tables['xref']
        
        # Check xref-specific columns
        self.assertIn('master_record_id', xref_table.columns)
        self.assertIn('match_score', xref_table.columns)
        self.assertIn('match_status', xref_table.columns)
        self.assertIn('match_date', xref_table.columns)
        
    def test_record_creation(self):
        """Test record creation and retrieval.
        
        Verifies that:
        - Records can be created in master table
        - Record data is stored correctly
        - Record can be retrieved by ID
        - Record metadata is set properly
        """
        # Create test record
        record_data = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'birth_date': datetime.date(1990, 1, 1)
        }
        
        record = self.record_manager.create_record(
            'person',
            record_data,
            source_system='test'
        )
        
        # Verify record creation
        self.assertIsNotNone(record.record_id)
        self.assertEqual(record.source_system, 'test')
        self.assertEqual(record.first_name, 'John')
        self.assertEqual(record.last_name, 'Doe')
        
        # Verify retrieval
        retrieved = self.record_manager.get_record('person', record.record_id)
        self.assertEqual(retrieved.record_id, record.record_id)
        self.assertEqual(retrieved.first_name, 'John')
        
    def test_index_creation(self):
        """Test index creation.
        
        Verifies that:
        - Indexes are created for specified fields
        - Composite indexes are created correctly
        - System-generated indexes are present
        """
        inspector = sa.inspect(self.engine)
        
        # Check master table indexes
        master_indexes = inspector.get_indexes('person_master', schema='mdm')
        index_columns = {idx['column_names'] for idx in master_indexes}
        
        self.assertIn(['source_system', 'source_id'], index_columns)
        self.assertIn(['status'], index_columns)
        self.assertIn(['created_at'], index_columns)
        
        # Check xref table indexes
        xref_indexes = inspector.get_indexes('person_xref', schema='mdm')
        xref_index_columns = {idx['column_names'] for idx in xref_indexes}
        
        self.assertIn(['master_record_id'], xref_index_columns)
        self.assertIn(['match_status'], xref_index_columns)


if __name__ == '__main__':
    unittest.main() 