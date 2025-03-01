"""
Database setup script for OpenMatch MDM.
"""

import os
import sys
from pathlib import Path
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

def setup_database():
    """Set up the MDM database and schema."""
    # Connect to PostgreSQL server
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user=os.getenv('MDM_DB_USER', 'postgres'),
        password=os.getenv('MDM_DB_PASSWORD', 'postgres')
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    try:
        # Create MDM database if it doesn't exist
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'mdm'")
        if not cur.fetchone():
            print("Creating MDM database...")
            cur.execute("CREATE DATABASE mdm")
        
        # Close connection to server and connect to mdm database
        cur.close()
        conn.close()
        
        # Connect to MDM database
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user=os.getenv('MDM_DB_USER', 'postgres'),
            password=os.getenv('MDM_DB_PASSWORD', 'postgres'),
            database="mdm"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Create MDM schema if it doesn't exist
        print("Creating MDM schema...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS mdm")
        
        # Try to create pgvector extension
        try:
            print("Creating pgvector extension...")
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            print("pgvector extension created successfully!")
        except psycopg2.Error as e:
            print("Warning: Could not create pgvector extension. Vector operations will use fallback mode.")
            print(f"To enable vector operations, please install pgvector: {e}")
        
        print("Database setup completed successfully!")
        
    except Exception as e:
        print(f"Error during database setup: {e}")
        raise
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    setup_database() 