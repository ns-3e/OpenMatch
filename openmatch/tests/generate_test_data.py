#!/usr/bin/env python3
"""
Test Data Generator for OpenMatch
Generates realistic person records with variations and noise for testing MDM functionality.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
from faker import Faker
import typer
from pathlib import Path
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

# Initialize Faker
fake = Faker()
Faker.seed(12345)  # For reproducibility

# Constants for data generation
SOURCES = ["CRM", "ERP", "LEGACY", "WEB", "MOBILE"]
ERROR_RATE = 0.2  # 20% chance of introducing errors
DUPLICATE_RATE = 0.3  # 30% chance of creating duplicates with variations

class PostgresDataSaver:
    """Handles saving generated data to PostgreSQL database."""
    
    def __init__(self, dbname: str, user: str, password: str, host: str = "localhost", port: int = 5432):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            print("Successfully connected to PostgreSQL database")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    def create_schema(self):
        """Create the necessary database schema."""
        try:
            # Create persons table (root table)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS persons (
                    id VARCHAR(50) PRIMARY KEY,
                    source VARCHAR(20) NOT NULL,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    birth_date DATE,
                    ssn VARCHAR(20),
                    gender VARCHAR(1),
                    created_at TIMESTAMP WITH TIME ZONE
                );
            """)

            # Create emails table (1:M)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS emails (
                    id SERIAL PRIMARY KEY,
                    person_id VARCHAR(50) REFERENCES persons(id),
                    email VARCHAR(255) NOT NULL,
                    is_primary BOOLEAN DEFAULT false,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_email UNIQUE (email),
                    CONSTRAINT fk_person_email
                        FOREIGN KEY(person_id)
                        REFERENCES persons(id)
                        ON DELETE CASCADE
                );
            """)

            # Create phones table (1:M)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS phones (
                    id SERIAL PRIMARY KEY,
                    person_id VARCHAR(50) REFERENCES persons(id),
                    phone_number VARCHAR(50) NOT NULL,
                    type VARCHAR(20),  -- mobile, home, work, etc.
                    is_primary BOOLEAN DEFAULT false,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_person_phone
                        FOREIGN KEY(person_id)
                        REFERENCES persons(id)
                        ON DELETE CASCADE
                );
            """)

            # Create addresses table (1:M)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS addresses (
                    id SERIAL PRIMARY KEY,
                    person_id VARCHAR(50) REFERENCES persons(id),
                    street VARCHAR(255),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    postal_code VARCHAR(20),
                    country VARCHAR(50),
                    type VARCHAR(20),  -- home, work, shipping, billing, etc.
                    is_primary BOOLEAN DEFAULT false,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_person_address
                        FOREIGN KEY(person_id)
                        REFERENCES persons(id)
                        ON DELETE CASCADE
                );
            """)

            # Create indexes
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_persons_source ON persons(source);
                CREATE INDEX IF NOT EXISTS idx_persons_names ON persons(first_name, last_name);
                CREATE INDEX IF NOT EXISTS idx_emails_person ON emails(person_id);
                CREATE INDEX IF NOT EXISTS idx_emails_email ON emails(email);
                CREATE INDEX IF NOT EXISTS idx_phones_person ON phones(person_id);
                CREATE INDEX IF NOT EXISTS idx_phones_number ON phones(phone_number);
                CREATE INDEX IF NOT EXISTS idx_addresses_person ON addresses(person_id);
                CREATE INDEX IF NOT EXISTS idx_addresses_postal ON addresses(postal_code);
            """)

            self.conn.commit()
            print("Successfully created database schema")
        except Exception as e:
            self.conn.rollback()
            print(f"Error creating schema: {e}")
            raise

    def save_records(self, records: List[Dict[str, Any]]):
        """Save records to PostgreSQL database."""
        try:
            # Prepare data for batch insert
            person_records = []
            email_records = []
            phone_records = []
            address_records = []
            
            print("Preparing records for database insertion...")
            for record in records:
                # Extract related data
                address = record.pop("address")
                email = record.pop("email")
                phone = record.pop("phone")
                
                # Prepare person record
                person_records.append((
                    record["id"],
                    record["source"],
                    record["first_name"],
                    record["last_name"],
                    record["birth_date"],
                    record["ssn"],
                    record["gender"],
                    record["created_at"]
                ))
                
                # Prepare email record
                email_records.append((
                    record["id"],  # person_id
                    email,
                    True,  # is_primary
                    record["created_at"]
                ))
                
                # Add additional email if it's a duplicate record
                if random.random() < 0.3:  # 30% chance of additional email
                    alt_email = f"{record['first_name'].lower()}.{record['last_name'].lower()}.alt@{email.split('@')[1]}"
                    email_records.append((
                        record["id"],
                        alt_email,
                        False,  # is_primary
                        record["created_at"]
                    ))
                
                # Prepare phone record
                if phone:  # Only add phone record if phone is not None
                    phone_records.append((
                        record["id"],  # person_id
                        phone,
                        'mobile',  # type
                        True,  # is_primary
                        record["created_at"]
                    ))
                    
                    # Add additional phone if it's a duplicate record
                    if random.random() < 0.3:  # 30% chance of additional phone
                        alt_phone = fake.phone_number()
                        phone_records.append((
                            record["id"],
                            alt_phone,
                            'home',  # type
                            False,  # is_primary
                            record["created_at"]
                        ))
                
                # Prepare address record
                address_records.append((
                    record["id"],  # person_id
                    address["street"],
                    address["city"],
                    address["state"],
                    address["postal_code"],
                    address["country"],
                    'home',  # type
                    True,  # is_primary
                    record["created_at"]
                ))
                
                # Add additional address if it's a duplicate record
                if random.random() < 0.2:  # 20% chance of additional address
                    work_address = {
                        "street": fake.street_address(),
                        "city": address["city"],  # Same city
                        "state": address["state"],  # Same state
                        "postal_code": fake.zipcode(),
                        "country": "USA"
                    }
                    address_records.append((
                        record["id"],
                        work_address["street"],
                        work_address["city"],
                        work_address["state"],
                        work_address["postal_code"],
                        work_address["country"],
                        'work',  # type
                        False,  # is_primary
                        record["created_at"]
                    ))

            # Batch insert with progress reporting
            BATCH_SIZE = 5000
            
            print("Inserting person records...")
            for i in range(0, len(person_records), BATCH_SIZE):
                batch = person_records[i:i + BATCH_SIZE]
                if i % 10000 == 0:
                    print(f"Inserting persons {i}/{len(person_records)}...")
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO persons (
                        id, source, first_name, last_name,
                        birth_date, ssn, gender, created_at
                    ) VALUES %s
                    ON CONFLICT (id) DO NOTHING
                    """,
                    batch
                )
                self.conn.commit()

            print("Inserting email records...")
            for i in range(0, len(email_records), BATCH_SIZE):
                batch = email_records[i:i + BATCH_SIZE]
                if i % 10000 == 0:
                    print(f"Inserting emails {i}/{len(email_records)}...")
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO emails (
                        person_id, email, is_primary, created_at
                    ) VALUES %s
                    ON CONFLICT (email) DO NOTHING
                    """,
                    batch
                )
                self.conn.commit()

            print("Inserting phone records...")
            for i in range(0, len(phone_records), BATCH_SIZE):
                batch = phone_records[i:i + BATCH_SIZE]
                if i % 10000 == 0:
                    print(f"Inserting phones {i}/{len(phone_records)}...")
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO phones (
                        person_id, phone_number, type, is_primary, created_at
                    ) VALUES %s
                    """,
                    batch
                )
                self.conn.commit()

            print("Inserting address records...")
            for i in range(0, len(address_records), BATCH_SIZE):
                batch = address_records[i:i + BATCH_SIZE]
                if i % 10000 == 0:
                    print(f"Inserting addresses {i}/{len(address_records)}...")
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO addresses (
                        person_id, street, city, state, postal_code,
                        country, type, is_primary, created_at
                    ) VALUES %s
                    """,
                    batch
                )
                self.conn.commit()

            print(f"Successfully saved {len(records)} records to database")
            print(f"Total emails: {len(email_records)}")
            print(f"Total phones: {len(phone_records)}")
            print(f"Total addresses: {len(address_records)}")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error saving records: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Database connection closed")

class DataGenerator:
    def __init__(self):
        self.used_emails = set()
        self.used_phones = set()
        
    def generate_person_base(self) -> Dict[str, Any]:
        """Generate base person record with correct data."""
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}"
        phone = fake.phone_number()
        
        # Ensure unique email and phone
        while email in self.used_emails:
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,999)}@{fake.domain_name()}"
        while phone in self.used_phones:
            phone = fake.phone_number()
            
        self.used_emails.add(email)
        self.used_phones.add(phone)
        
        return {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "postal_code": fake.zipcode(),
                "country": "USA"
            },
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d"),
            "ssn": fake.ssn(),
            "gender": random.choice(["M", "F"]),
            "created_at": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
        }

    def introduce_errors(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Introduce realistic errors into the record."""
        record = record.copy()
        
        if random.random() < ERROR_RATE:
            error_type = random.choice([
                "typo", "missing_field", "wrong_format", "extra_spaces",
                "case_change", "field_swap"
            ])
            
            if error_type == "typo":
                field = random.choice(["first_name", "last_name", "email"])
                if field in ["first_name", "last_name"]:
                    text = record[field]
                    pos = random.randint(0, len(text)-1)
                    new_char = chr(ord(text[pos]) + random.randint(-1, 1))
                    record[field] = text[:pos] + new_char + text[pos+1:]
                    
            elif error_type == "missing_field":
                field = random.choice(["phone", "birth_date", "gender"])
                record[field] = None
                
            elif error_type == "wrong_format":
                if "phone" in record:
                    phone = record["phone"].replace("-", "").replace("(", "").replace(")", "")
                    record["phone"] = phone
                    
            elif error_type == "extra_spaces":
                field = random.choice(["first_name", "last_name"])
                record[field] = f" {record[field]} "
                
            elif error_type == "case_change":
                field = random.choice(["first_name", "last_name", "email"])
                record[field] = record[field].upper() if random.random() < 0.5 else record[field].lower()
                
            elif error_type == "field_swap":
                record["first_name"], record["last_name"] = record["last_name"], record["first_name"]
        
        return record

    def create_duplicate_with_variation(self, record: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Create a duplicate record with realistic variations."""
        duplicate = record.copy()
        
        # Always change the source and ID
        duplicate["source"] = source
        duplicate["id"] = f"{source}_{uuid.uuid4().hex[:8]}"
        
        # Introduce variations
        variation_type = random.choices(
            ["exact", "minor", "moderate", "major"],
            weights=[0.2, 0.4, 0.3, 0.1]  # Increased weights for exact and minor variations
        )[0]
        
        # Always preserve either SSN or birth_date to ensure blocking works
        preserve_ssn = random.random() < 0.7  # 70% chance to preserve SSN
        if preserve_ssn:
            # Keep SSN the same
            pass  # SSN is already copied from original
        else:
            # Keep birth_date the same
            duplicate["ssn"] = fake.ssn()
            
        if variation_type == "exact":
            # Keep both SSN and birth_date the same
            pass  # Both are already copied from original
            
        elif variation_type == "minor":
            # Keep SSN and birth_date, introduce minor variations in other fields
            if random.random() < 0.4:
                duplicate["first_name"] = record["first_name"][0] + "."  # Initial
            elif random.random() < 0.3:
                duplicate["first_name"] = record["first_name"].upper()
            
            if random.random() < 0.3:
                addr = duplicate["address"]
                addr["street"] = addr["street"].replace("Street", "St.").replace("Avenue", "Ave.")
            
            if random.random() < 0.4 and record.get("phone"):
                phone = record["phone"]
                duplicate["phone"] = phone.replace("-", "").replace("(", "").replace(")", "")
            
            if random.random() < 0.4:
                email = record["email"]
                name_part = email.split("@")[0]
                domain_part = email.split("@")[1]
                duplicate["email"] = f"{name_part.replace('.', '_')}@{domain_part}"
                
        elif variation_type == "moderate":
            # Already handled SSN/birth_date preservation above
            
            # Name variations
            if random.random() < 0.6:
                name = duplicate["first_name"]
                pos = random.randint(0, len(name)-1)
                new_char = chr(ord(name[pos]) + random.randint(-1, 1))
                duplicate["first_name"] = name[:pos] + new_char + name[pos+1:]
            
            if random.random() < 0.4:
                name = duplicate["last_name"]
                pos = random.randint(0, len(name)-1)
                new_char = chr(ord(name[pos]) + random.randint(-1, 1))
                duplicate["last_name"] = name[:pos] + new_char + name[pos+1:]
            
            # Different contact info
            if random.random() < 0.7:
                duplicate["email"] = fake.email()
            if random.random() < 0.7 and record.get("phone"):
                duplicate["phone"] = fake.phone_number()
                
        else:  # major variations
            # Already handled SSN/birth_date preservation above
            
            # Significant name changes
            if random.random() < 0.6:
                if random.random() < 0.5:
                    duplicate["first_name"] = fake.first_name()
                else:
                    duplicate["last_name"] = fake.last_name()
            
            # Different contact info
            duplicate["email"] = fake.email()
            if record.get("phone"):
                duplicate["phone"] = fake.phone_number()
            
            # Different address
            duplicate["address"] = {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "postal_code": fake.zipcode(),
                "country": "USA"
            }
        
        return duplicate

    def generate_dataset(self, num_records: int) -> List[Dict[str, Any]]:
        """Generate a complete dataset with duplicates and variations."""
        records = []
        base_records = []
        
        # Generate base records
        print(f"Generating {num_records} records...")
        base_record_count = int(num_records * 0.7)  # 70% unique records
        for i in range(base_record_count):
            if i % 1000 == 0:
                print(f"Generated {i}/{base_record_count} base records...")
                
            base_record = self.generate_person_base()
            source = random.choice(SOURCES)
            base_record["source"] = source
            base_record["id"] = f"{source}_{uuid.uuid4().hex[:8]}"
            
            # Maybe introduce errors
            if random.random() < ERROR_RATE:
                base_record = self.introduce_errors(base_record)
                
            records.append(base_record)
            base_records.append(base_record)
        
        print("Generating duplicates with variations...")
        duplicate_count = 0
        # Generate duplicates with variations
        for i, record in enumerate(base_records):
            if i % 1000 == 0:
                print(f"Processing duplicates for record {i}/{len(base_records)}...")
                
            if random.random() < DUPLICATE_RATE:
                num_duplicates = random.randint(1, 3)
                for _ in range(num_duplicates):
                    source = random.choice([s for s in SOURCES if s != record["source"]])
                    duplicate = self.create_duplicate_with_variation(record, source)
                    records.append(duplicate)
                    duplicate_count += 1
        
        print(f"Generated {len(records)} total records ({len(base_records)} base records + {duplicate_count} duplicates)")
        return records

def save_records(records: List[Dict[str, Any]], output_dir: Path, format: str = "json", db_config: Dict[str, Any] = None):
    """Save records in the specified format."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if format == "json":
        output_file = output_dir / "test_data.json"
        with open(output_file, "w") as f:
            json.dump(records, f, indent=2)
            
    elif format == "csv":
        output_file = output_dir / "test_data.csv"
        df = pd.json_normalize(records)
        df.to_csv(output_file, index=False)
        
    elif format == "parquet":
        output_file = output_dir / "test_data.parquet"
        df = pd.json_normalize(records)
        df.to_parquet(output_file, index=False)
        
    elif format == "postgres":
        if not db_config:
            raise ValueError("Database configuration is required for PostgreSQL format")
            
        db_saver = PostgresDataSaver(**db_config)
        try:
            db_saver.connect()
            db_saver.create_schema()
            db_saver.save_records(records)
        finally:
            db_saver.close()
            return
            
    print(f"Generated {len(records)} records and saved to {output_file}")

def main(
    num_records: int = typer.Option(10000, help="Number of records to generate"),
    output_dir: str = typer.Option("test_data", help="Output directory"),
    format: str = typer.Option("json", help="Output format (json, csv, parquet, or postgres)"),
    db_name: str = typer.Option(None, help="PostgreSQL database name"),
    db_user: str = typer.Option(None, help="PostgreSQL user"),
    db_password: str = typer.Option(None, help="PostgreSQL password"),
    db_host: str = typer.Option("localhost", help="PostgreSQL host"),
    db_port: int = typer.Option(5432, help="PostgreSQL port")
):
    """Generate test data for OpenMatch."""
    if format not in ["json", "csv", "parquet", "postgres"]:
        raise typer.BadParameter("Format must be json, csv, parquet, or postgres")
        
    generator = DataGenerator()
    records = generator.generate_dataset(num_records)
    
    db_config = None
    if format == "postgres":
        if not all([db_name, db_user, db_password]):
            raise typer.BadParameter("Database name, user, and password are required for PostgreSQL format")
        db_config = {
            "dbname": db_name,
            "user": db_user,
            "password": db_password,
            "host": db_host,
            "port": db_port
        }
        
    save_records(records, Path(output_dir), format, db_config)

if __name__ == "__main__":
    typer.run(main) 