"""
Setup script to initialize test database with sample data.
"""
import os
import logging
from pathlib import Path
from sqlalchemy import text
from openmatch.tests.generate_test_data import DataGenerator, PostgresDataSaver
from openmatch.connectors import init_database, reset_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_test_database(
    num_records: int = 10000,  # Reduced number of base records
    host: str = "localhost",
    port: int = 5432,
    database: str = "openmatch_test",
    username: str = "postgres",
    password: str = None,
    schema: str = "mdm"
) -> None:
    """
    Set up test database with sample data.
    """
    if not password:
        password = os.environ.get("POSTGRES_PASSWORD")
        if not password:
            raise ValueError("Database password must be provided via argument or POSTGRES_PASSWORD environment variable")

    try:
        # Initialize MDM schema first
        connector = init_database(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema
        )
        
        # Check if source_records table has data
        with connector.session() as session:
            result = session.execute(text("SELECT COUNT(*) FROM mdm.source_records"))
            count = result.scalar()
            if count > 0:
                logger.info(f"Database already contains {count} source records, skipping data generation")
                return
        
        logger.info(f"Initializing database {database} with {num_records} records...")
        
        # Generate test data
        generator = DataGenerator()
        records = generator.generate_dataset(num_records)
        
        # Save to PostgreSQL persons table first
        db_config = {
            "dbname": database,
            "user": username,
            "password": password,
            "host": host,
            "port": port
        }
        
        db_saver = PostgresDataSaver(**db_config)
        db_saver.connect()
        db_saver.create_schema()
        db_saver.save_records(records)
        
        # Transfer data from persons to source_records
        with connector.session() as session:
            session.execute(text("""
                INSERT INTO mdm.source_records (source_id, source_system, record_data, created_at, updated_at)
                SELECT 
                    id as source_id,
                    CASE (random() * 4)::int
                        WHEN 0 THEN 'CRM'
                        WHEN 1 THEN 'ERP'
                        WHEN 2 THEN 'LEGACY'
                        WHEN 3 THEN 'WEB'
                        ELSE 'MOBILE'
                    END as source_system,
                    jsonb_build_object(
                        'first_name', first_name,
                        'last_name', last_name,
                        'birth_date', birth_date,
                        'ssn', ssn
                    ) as record_data,
                    NOW() as created_at,
                    NOW() as updated_at
                FROM persons
            """))
            session.commit()
        
        db_saver.close()
        logger.info("Test database setup completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to set up test database: {str(e)}")
        raise

if __name__ == "__main__":
    setup_test_database() 