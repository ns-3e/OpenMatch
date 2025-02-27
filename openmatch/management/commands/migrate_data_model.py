"""
Django management command for data model migration.
"""

import logging
from typing import Any, Dict, List, Optional
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.exc import SQLAlchemyError

from openmatch.model import DataModelManager, DataModelConfig
from openmatch.model.config import PhysicalModelConfig


class Command(BaseCommand):
    help = 'Migrate the configured data model to the specified database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--db_connection',
            type=str,
            default='default',
            help='Database connection alias from Django DATABASES setting'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview SQL changes without applying them'
        )

    def _get_database_url(self, db_alias: str) -> str:
        """Convert Django database settings to SQLAlchemy URL."""
        if db_alias not in settings.DATABASES:
            raise CommandError(f"Unknown database alias: {db_alias}")

        db_settings = settings.DATABASES[db_alias]
        engine = db_settings['ENGINE']
        name = db_settings['NAME']
        user = db_settings.get('USER', '')
        password = db_settings.get('PASSWORD', '')
        host = db_settings.get('HOST', '')
        port = db_settings.get('PORT', '')

        # Map Django database engines to SQLAlchemy URLs
        engine_map = {
            'django.db.backends.postgresql': 'postgresql',
            'django.db.backends.mysql': 'mysql',
            'django.db.backends.sqlite3': 'sqlite',
            'django.db.backends.oracle': 'oracle'
        }

        if engine not in engine_map:
            raise CommandError(f"Unsupported database engine: {engine}")

        dialect = engine_map[engine]

        if dialect == 'sqlite':
            return f'sqlite:///{name}'
        else:
            auth = f'{user}:{password}@' if user else ''
            host_port = f'{host}:{port}' if port else host
            return f'{dialect}://{auth}{host_port}/{name}'

    def _get_table_differences(
        self,
        inspector: inspect,
        schema: str,
        physical_model: Dict[str, Any]
    ) -> Dict[str, List[str]]:
        """Compare existing database schema with physical model."""
        differences = {
            'create_tables': [],
            'alter_tables': [],
            'drop_tables': []
        }

        # Get existing tables
        existing_tables = set(inspector.get_table_names(schema=schema))

        # Check for tables to create or alter
        for entity_name, tables in physical_model.items():
            for table_type in ['master', 'history', 'xref']:
                table_config = tables[table_type]
                table_name = table_config['name']

                if table_name not in existing_tables:
                    differences['create_tables'].append(table_name)
                else:
                    # Compare columns
                    existing_columns = {
                        col['name']: col
                        for col in inspector.get_columns(table_name, schema=schema)
                    }
                    model_columns = {
                        col['name']: col
                        for col in table_config['columns']
                    }

                    if existing_columns != model_columns:
                        differences['alter_tables'].append(table_name)

        # Check for tables to drop (if they exist in DB but not in model)
        model_tables = set()
        for tables in physical_model.values():
            for table_type in ['master', 'history', 'xref']:
                model_tables.add(tables[table_type]['name'])

        differences['drop_tables'] = list(existing_tables - model_tables)

        return differences

    def _log_migration_plan(
        self,
        differences: Dict[str, List[str]],
        schema: str
    ) -> None:
        """Log the planned migration changes."""
        self.stdout.write('\nMigration Plan:')
        self.stdout.write('-' * 50)

        if differences['create_tables']:
            self.stdout.write('\nTables to create:')
            for table in differences['create_tables']:
                self.stdout.write(f'  + {schema}.{table}')

        if differences['alter_tables']:
            self.stdout.write('\nTables to alter:')
            for table in differences['alter_tables']:
                self.stdout.write(f'  ~ {schema}.{table}')

        if differences['drop_tables']:
            self.stdout.write('\nTables to drop:')
            for table in differences['drop_tables']:
                self.stdout.write(f'  - {schema}.{table}')

        if not any(differences.values()):
            self.stdout.write('\nNo changes detected.')

    def handle(self, *args, **options):
        try:
            # Set up logging
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(handler)

            # Get database URL
            db_alias = options['db_connection']
            database_url = self._get_database_url(db_alias)

            # Create SQLAlchemy engine
            engine = create_engine(database_url, echo=options['dry_run'])

            # Load data model configuration
            # Note: You'll need to implement how to load your configuration
            config = DataModelConfig.load()  # Implement this method
            
            # Create model manager
            manager = DataModelManager(config, engine, logger)

            # Get physical model
            physical_model = config.to_physical_model()
            schema = config.physical_model.schema_name

            # Analyze differences
            inspector = inspect(engine)
            differences = self._get_table_differences(
                inspector,
                schema,
                physical_model
            )

            # Log migration plan
            self._log_migration_plan(differences, schema)

            if options['dry_run']:
                self.stdout.write('\nDry run completed. No changes applied.')
                return

            # Apply migrations
            if any(differences.values()):
                # Drop tables first
                for table in differences['drop_tables']:
                    manager._drop_table(table, schema)

                # Create new tables
                for table in differences['create_tables']:
                    for entity_name, tables in physical_model.items():
                        for table_type, table_config in tables.items():
                            if table_config['name'] == table:
                                manager._create_table(table_config, schema)

                # Alter existing tables
                for table in differences['alter_tables']:
                    # First drop the table
                    manager._drop_table(table, schema)
                    # Then recreate it
                    for entity_name, tables in physical_model.items():
                        for table_type, table_config in tables.items():
                            if table_config['name'] == table:
                                manager._create_table(table_config, schema)

                self.stdout.write(self.style.SUCCESS(
                    '\nMigration completed successfully.'
                ))
            else:
                self.stdout.write('\nNo changes to apply.')

        except Exception as e:
            raise CommandError(f"Migration failed: {str(e)}") 