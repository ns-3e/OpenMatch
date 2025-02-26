#!/usr/bin/env python3
"""
OpenMatch Quickstart Example
This script demonstrates the core functionality of OpenMatch for master data management.
It walks through the complete process of:
1. Configuring source systems and loading test data from PostgreSQL
2. Configuring the data model and validation rules
3. Setting up match rules and thresholds
4. Running match and merge operations
5. Analyzing and visualizing results
"""

import json
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import typer
import sqlalchemy as sa
from contextlib import contextmanager

from openmatch import MDMPipeline
from openmatch.config import (
    TrustConfig, 
    SurvivorshipRules,
    DataModelConfig,
    ValidationRules,
    PhysicalModelConfig
)
from openmatch.match import MatchConfig, MatchEngine, MatchType, FieldMatchConfig, BlockingConfig, NullHandling
from openmatch.trust import TrustFramework
from openmatch.lineage import LineageTracker
from openmatch.visualization import ResultsVisualizer
from openmatch.model import DataModelManager

# Configure console logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('mdm_operations.log')
    ]
)
logger = logging.getLogger(__name__)

class MDMLogger:
    """Handles logging of MDM operations to both console and database."""
    
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
        schema: str = "mdm"
    ):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.schema = schema
        self.job_id = str(uuid.uuid4())
        self.conn = None
        self.setup_logging_table()

    def setup_logging_table(self):
        """Create the logging table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}._logs (
            id SERIAL PRIMARY KEY,
            job_id UUID NOT NULL,
            event_id UUID NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            level VARCHAR(10) NOT NULL,
            operation VARCHAR(100) NOT NULL,
            message TEXT NOT NULL,
            details JSONB,
            duration_ms INTEGER,
            status VARCHAR(20) NOT NULL,
            source_count INTEGER,
            error TEXT
        )
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Create schema if it doesn't exist
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
                # Create logging table
                cur.execute(create_table_sql)
                # Create indexes
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_logs_job_id ON {self.schema}._logs(job_id)")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON {self.schema}._logs(timestamp)")
                conn.commit()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**self.conn_params)
            yield conn
        finally:
            if conn:
                conn.close()

    def log_event(
        self,
        operation: str,
        message: str,
        level: str = "INFO",
        details: Optional[Dict] = None,
        duration_ms: Optional[int] = None,
        status: str = "SUCCESS",
        source_count: Optional[int] = None,
        error: Optional[str] = None
    ):
        """Log an event to both console and database."""
        event_id = str(uuid.uuid4())
        
        # Console logging
        log_message = f"[{operation}] {message}"
        if duration_ms:
            log_message += f" (took {duration_ms}ms)"
        if error:
            log_message += f" ERROR: {error}"
            
        if level == "ERROR":
            logger.error(log_message)
        elif level == "WARNING":
            logger.warning(log_message)
        else:
            logger.info(log_message)
            
        # Database logging
        insert_sql = f"""
        INSERT INTO {self.schema}._logs (
            job_id, event_id, level, operation, message, details,
            duration_ms, status, source_count, error
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    insert_sql,
                    (
                        self.job_id,
                        event_id,
                        level,
                        operation,
                        message,
                        Json(details) if details else None,
                        duration_ms,
                        status,
                        source_count,
                        error
                    )
                )
                conn.commit()
        
        return event_id

    @contextmanager
    def operation_logger(
        self,
        operation: str,
        message: str,
        source_count: Optional[int] = None,
        details: Optional[Dict] = None
    ):
        """Context manager for logging operations with timing."""
        start_time = datetime.now()
        event_id = None
        
        try:
            # Log operation start
            event_id = self.log_event(
                operation=operation,
                message=f"Starting: {message}",
                details=details,
                source_count=source_count
            )
            
            yield event_id
            
            # Log operation completion
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            self.log_event(
                operation=operation,
                message=f"Completed: {message}",
                duration_ms=duration_ms,
                details=details,
                source_count=source_count
            )
            
        except Exception as e:
            # Log operation failure
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            self.log_event(
                operation=operation,
                message=f"Failed: {message}",
                level="ERROR",
                status="FAILED",
                error=str(e),
                duration_ms=duration_ms,
                details=details,
                source_count=source_count
            )
            raise

class PostgresDataLoader:
    """Handles loading data from PostgreSQL database."""
    
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
        mdm_logger: Optional[MDMLogger] = None
    ):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.conn = None
        self.cursor = None
        self.logger = mdm_logger

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            if self.logger:
                self.logger.log_event(
                    operation="DATABASE_CONNECTION",
                    message="Successfully connected to PostgreSQL database"
                )
        except Exception as e:
            if self.logger:
                self.logger.log_event(
                    operation="DATABASE_CONNECTION",
                    message="Failed to connect to PostgreSQL database",
                    level="ERROR",
                    error=str(e)
                )
            raise

    def load_records(self, batch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Load records from PostgreSQL with related data.
        Uses batch processing to handle large datasets efficiently.
        """
        records = []
        offset = 0
        
        if self.logger:
            self.logger.log_event(
                operation="DATA_LOADING",
                message="Starting to load records from source database",
                details={"batch_size": batch_size}
            )
        
        while True:
            with self.logger.operation_logger(
                operation="BATCH_LOADING",
                message=f"Loading batch from offset {offset}",
                details={"batch_size": batch_size, "offset": offset}
            ):
                # Query for the next batch of person records with their related data
                self.cursor.execute("""
                    WITH batch AS (
                        SELECT * FROM persons
                        ORDER BY id
                        LIMIT %s OFFSET %s
                    )
                    SELECT 
                        p.*,
                        json_agg(DISTINCT e.*) FILTER (WHERE e.id IS NOT NULL) as emails,
                        json_agg(DISTINCT ph.*) FILTER (WHERE ph.id IS NOT NULL) as phones,
                        json_agg(DISTINCT a.*) FILTER (WHERE a.id IS NOT NULL) as addresses
                    FROM batch p
                    LEFT JOIN emails e ON p.id = e.person_id
                    LEFT JOIN phones ph ON p.id = ph.person_id
                    LEFT JOIN addresses a ON p.id = a.person_id
                    GROUP BY p.id, p.source, p.first_name, p.last_name, 
                             p.birth_date, p.ssn, p.gender, p.created_at
                """, (batch_size, offset))
                
                batch = self.cursor.fetchall()
                if not batch:
                    break
                
                # Process each record to format it correctly
                processed_records = []
                for record in batch:
                    # Convert record from RealDictRow to regular dict
                    record = dict(record)
                    
                    # Process emails
                    emails = record.pop('emails', [])
                    if emails and emails[0] is not None:
                        # Get primary email
                        primary_email = next((e['email'] for e in emails if e['is_primary']), None)
                        record['email'] = primary_email or emails[0]['email']
                    
                    # Process phones
                    phones = record.pop('phones', [])
                    if phones and phones[0] is not None:
                        # Get primary phone
                        primary_phone = next((p['phone_number'] for p in phones if p['is_primary']), None)
                        record['phone'] = primary_phone or phones[0]['phone_number']
                    
                    # Process addresses
                    addresses = record.pop('addresses', [])
                    if addresses and addresses[0] is not None:
                        # Get primary address
                        primary_address = next((a for a in addresses if a['is_primary']), addresses[0])
                        record['address'] = {
                            'street': primary_address['street'],
                            'city': primary_address['city'],
                            'state': primary_address['state'],
                            'postal_code': primary_address['postal_code'],
                            'country': primary_address['country']
                        }
                    
                    processed_records.append(record)
                
                records.extend(processed_records)
                
                if self.logger:
                    self.logger.log_event(
                        operation="BATCH_PROGRESS",
                        message=f"Loaded {len(records)} records so far",
                        source_count=len(records)
                    )
                
                offset += batch_size
                
                if len(batch) < batch_size:
                    break
        
        if self.logger:
            self.logger.log_event(
                operation="DATA_LOADING",
                message="Completed loading all records",
                source_count=len(records)
            )
        
        return records

    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            if self.logger:
                self.logger.log_event(
                    operation="DATABASE_CONNECTION",
                    message="Database connection closed"
                )

def load_test_data(
    dbname: str,
    user: str,
    password: str,
    host: str = "localhost",
    port: int = 5432,
    batch_size: int = 1000,
    mdm_logger: Optional[MDMLogger] = None
) -> List[Dict[str, Any]]:
    """
    Load test data from PostgreSQL database.
    
    Args:
        dbname: Database name
        user: Database user
        password: Database password
        host: Database host
        port: Database port
        batch_size: Number of records to load in each batch
        
    Returns:
        List of record dictionaries
    """
    loader = PostgresDataLoader(dbname, user, password, host, port, mdm_logger)
    try:
        loader.connect()
        records = loader.load_records(batch_size)
        if mdm_logger:
            mdm_logger.log_event(
                operation="DATA_LOADING",
                message=f"Successfully loaded {len(records)} records from PostgreSQL database",
                source_count=len(records)
            )
        return records
    finally:
        loader.close()

def configure_data_model(schema_name: str = "mdm") -> DataModelConfig:
    """
    Configure the data model for person records.
    Defines required fields, data types, and validation rules.
    Also configures physical model settings for MDM tables.
    """
    # Define core person attributes
    data_model = DataModelConfig(
        entity_type="person",
        required_fields=[
            "id",
            "source",
            "first_name",
            "last_name",
            "email"
        ],
        field_types={
            "id": "string",
            "source": "string",
            "first_name": "string",
            "last_name": "string",
            "email": "email",
            "phone": "phone",
            "birth_date": "date",
            "ssn": "ssn",
            "gender": "string",
            "created_at": "datetime"
        },
        # Define nested objects
        complex_fields={
            "address": {
                "street": "string",
                "city": "string",
                "state": "string",
                "postal_code": "string",
                "country": "string"
            }
        }
    )
    
    # Add validation rules
    validation_rules = ValidationRules(
        field_rules={
            "email": {
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "unique": True
            },
            "phone": {
                "pattern": r"^\+?1?\d{9,15}$",
                "standardize": "E.164"
            },
            "ssn": {
                "pattern": r"^\d{3}-?\d{2}-?\d{4}$",
                "mask": True
            },
            "gender": {
                "allowed_values": ["M", "F", None]
            }
        }
    )
    
    # Configure physical model settings
    physical_model = PhysicalModelConfig(
        schema_name=schema_name,
        table_prefix="person",
        master_table_settings={
            "include_audit_fields": True,
            "partition_by": "source"
        },
        history_table_settings={
            "track_changes": True,
            "retention_period_days": 365
        },
        xref_table_settings={
            "include_match_details": True,
            "include_confidence_scores": True
        }
    )
    
    data_model.set_validation_rules(validation_rules)
    data_model.set_physical_model_config(physical_model)
    return data_model

def configure_trust_framework() -> TrustConfig:
    """
    Configure trust scores and reliability weights for different sources.
    """
    # Define source system reliability scores
    trust_config = TrustConfig(
        source_reliability={
            "CRM": 0.9,     # Most reliable source
            "ERP": 0.85,    # Enterprise system, quite reliable
            "WEB": 0.7,     # Web form entries, less reliable
            "MOBILE": 0.75, # Mobile app entries
            "LEGACY": 0.6   # Legacy system, least reliable
        },
        # Define field-level trust weights
        field_weights={
            "email": 1.0,     # Email is a strong identifier
            "ssn": 0.9,       # SSN is reliable when present
            "phone": 0.8,     # Phone numbers can change
            "name": 0.7,      # Names can have variations
            "address": 0.6    # Addresses may be outdated
        }
    )
    
    return trust_config

def configure_survivorship_rules() -> SurvivorshipRules:
    """
    Configure survivorship rules for resolving conflicts during merge.
    """
    return SurvivorshipRules(
        # Define source priority for each field
        priority_fields={
            "email": ["CRM", "ERP", "WEB", "MOBILE", "LEGACY"],
            "phone": ["CRM", "MOBILE", "ERP", "WEB", "LEGACY"],
            "name": ["CRM", "ERP", "WEB", "MOBILE", "LEGACY"],
            "address": ["CRM", "ERP", "MOBILE", "WEB", "LEGACY"],
            "birth_date": ["CRM", "ERP", "LEGACY", "WEB", "MOBILE"]
        },
        # Define custom merge rules
        merge_rules={
            "name": "most_complete",  # Take most complete name
            "email": "most_recent",   # Take most recent email
            "phone": "most_recent",   # Take most recent phone
            "address": "most_recent", # Take most recent address
            "birth_date": "earliest"  # Take earliest birth date
        }
    )

def configure_match_rules() -> MatchConfig:
    """
    Configure matching rules and thresholds.
    """
    # Define field-level match configurations
    field_configs = {
        "email": FieldMatchConfig(
            match_type=MatchType.EXACT,
            weight=1.0,
            threshold=1.0
        ),
        "phone": FieldMatchConfig(
            match_type=MatchType.PHONETIC,
            weight=0.8,
            threshold=0.8,
            phonetic_algorithm="soundex"
        ),
        "first_name": FieldMatchConfig(
            match_type=MatchType.FUZZY,
            weight=0.7,
            threshold=0.7,
            fuzzy_params={"method": "levenshtein", "threshold": 0.7}
        ),
        "last_name": FieldMatchConfig(
            match_type=MatchType.FUZZY,
            weight=0.7,
            threshold=0.7,
            fuzzy_params={"method": "levenshtein", "threshold": 0.7}
        ),
        "address.street": FieldMatchConfig(
            match_type=MatchType.ADDRESS,
            weight=0.6,
            threshold=0.6
        ),
        "birth_date": FieldMatchConfig(
            match_type=MatchType.DATE,
            weight=0.5,
            threshold=0.5
        )
    }

    # Define blocking configuration
    blocking = BlockingConfig(
        blocking_keys=["postal_code", "last_name", "email"],
        method="standard"
    )

    return MatchConfig(
        field_configs=field_configs,
        blocking=blocking,
        min_overall_score=0.7,
        score_aggregation="weighted_average",
        parallel_processing=True,
        num_workers=4  # Use 4 worker threads
    )

def process_records(pipeline: MDMPipeline, records: List[Dict[str, Any]]):
    """
    Process records through the MDM pipeline and track lineage.
    """
    # Initialize lineage tracker
    lineage = LineageTracker()
    
    # Process records in batches
    batch_size = 1000
    total_processed = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}, records {i} to {min(i + batch_size, len(records))}")
        
        # Process batch
        results = pipeline.process_records(batch)
        
        # Track lineage for the batch
        for golden_record in results.golden_records:
            lineage.track_merge(
                source_records=results.source_records[golden_record["id"]],
                golden_record=golden_record
            )
        
        total_processed += len(batch)
        print(f"Processed {total_processed}/{len(records)} records")
    
    return results, lineage

def analyze_results(results, lineage):
    """
    Analyze and visualize the results of MDM processing.
    """
    visualizer = ResultsVisualizer()
    
    print("\n=== MDM Processing Results ===")
    
    # Display match statistics
    print("\nMatch Statistics:")
    print(f"Total Records Processed: {results.total_records}")
    print(f"Matched Groups Found: {results.match_groups_count}")
    print(f"Average Group Size: {results.avg_group_size:.2f}")
    
    # Display golden record statistics
    print("\nGolden Record Statistics:")
    print(f"Golden Records Created: {len(results.golden_records)}")
    print(f"Records Requiring Review: {len(results.review_required)}")
    
    # Display source statistics
    print("\nSource System Statistics:")
    for source, count in results.source_counts.items():
        print(f"{source}: {count} records")
    
    # Display data quality metrics
    print("\nData Quality Metrics:")
    for metric, value in results.quality_metrics.items():
        print(f"{metric}: {value:.2%}")
    
    # Generate visualizations
    visualizer.plot_match_distribution(results)
    visualizer.plot_source_distribution(results)
    visualizer.plot_data_quality_metrics(results)
    
    # Export detailed results if needed
    export_path = Path("mdm_results")
    export_path.mkdir(exist_ok=True)
    
    # Export golden records
    with open(export_path / "golden_records.json", "w") as f:
        json.dump(results.golden_records, f, indent=2)
    
    # Export lineage information
    with open(export_path / "lineage.json", "w") as f:
        json.dump(lineage.export(), f, indent=2)
    
    print(f"\nDetailed results exported to {export_path}")

def setup_mdm_tables(
    dbname: str,
    user: str,
    password: str,
    host: str = "localhost",
    port: int = 5432,
    schema: str = "mdm",
    mdm_logger: Optional[MDMLogger] = None
) -> None:
    """
    Set up the physical MDM tables using the DataModelManager.
    """
    with mdm_logger.operation_logger(
        operation="MDM_TABLE_SETUP",
        message="Setting up MDM tables"
    ):
        engine = sa.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        )
        
        data_model = configure_data_model(schema)
        model_manager = DataModelManager(data_model, engine)
        
        try:
            # Create schema if it doesn't exist
            with engine.connect() as conn:
                conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                conn.commit()
            
            # Create physical tables
            model_manager.create_physical_model()
            
            if mdm_logger:
                mdm_logger.log_event(
                    operation="MDM_TABLE_SETUP",
                    message=f"Successfully created MDM tables in schema '{schema}'"
                )
                
        except Exception as e:
            if mdm_logger:
                mdm_logger.log_event(
                    operation="MDM_TABLE_SETUP",
                    message="Failed to create MDM tables",
                    level="ERROR",
                    error=str(e)
                )
            raise

def main(
    dbname: str = typer.Option(..., help="PostgreSQL database name"),
    user: str = typer.Option(..., help="PostgreSQL user"),
    password: str = typer.Option(..., help="PostgreSQL password", prompt=True, hide_input=True),
    host: str = typer.Option("localhost", help="PostgreSQL host"),
    port: int = typer.Option(5432, help="PostgreSQL port"),
    batch_size: int = typer.Option(1000, help="Batch size for loading records"),
    schema: str = typer.Option("mdm", help="Schema name for MDM tables")
):
    """
    Main function demonstrating the complete MDM workflow.
    """
    print("=== OpenMatch MDM Quickstart Example ===")
    
    # Initialize MDM logger
    mdm_logger = MDMLogger(dbname, user, password, host, port, schema)
    
    with mdm_logger.operation_logger(
        operation="MDM_WORKFLOW",
        message="Starting MDM workflow"
    ):
        # 1. Set up MDM tables
        print("\n1. Setting up MDM tables...")
        setup_mdm_tables(dbname, user, password, host, port, schema, mdm_logger)
        
        # 2. Load test data from PostgreSQL
        print("\n2. Loading test data from PostgreSQL...")
        records = load_test_data(dbname, user, password, host, port, batch_size, mdm_logger)
        
        # 3. Configure the MDM pipeline
        print("\n3. Configuring MDM pipeline...")
        
        # Configure data model
        data_model = configure_data_model(schema)
        print("✓ Data model configured")
        
        # Configure trust framework
        trust_config = configure_trust_framework()
        print("✓ Trust framework configured")
        
        # Configure survivorship rules
        survivorship_rules = configure_survivorship_rules()
        print("✓ Survivorship rules configured")
        
        # Configure match rules
        match_config = configure_match_rules()
        print("✓ Match rules configured")
        
        # Initialize pipeline with database connection
        pipeline = MDMPipeline(
            data_model=data_model,
            trust_config=trust_config,
            survivorship_rules=survivorship_rules,
            match_config=match_config,
            db_config={
                "connection_string": f"postgresql://{user}:{password}@{host}:{port}/{dbname}",
                "schema": schema
            }
        )
        
        # 4. Process records
        print("\n4. Processing records...")
        results, lineage = process_records(pipeline, records)
        
        # 5. Analyze results
        print("\n5. Analyzing results...")
        analyze_results(results, lineage)

if __name__ == "__main__":
    typer.run(main) 