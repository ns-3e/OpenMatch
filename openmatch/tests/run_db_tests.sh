#!/bin/bash

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

# Change to the project root directory
cd "$PROJECT_ROOT"

# Set up environment variables
export PYTHONPATH="$PROJECT_ROOT"
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-"postgres"}

# Drop and recreate the database
echo "Dropping and recreating test database..."
PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS openmatch_test;"
PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -U postgres -c "CREATE DATABASE openmatch_test;"

# Generate and load test data using the data generator
echo "Generating and loading 5M test records..."
python3 -m openmatch.tests.generate_test_data \
    --num-records 5000000 \
    --format postgres \
    --db-name openmatch_test \
    --db-user postgres \
    --db-password "$POSTGRES_PASSWORD" \
    --db-host localhost \
    --db-port 5432

# Set up test database schema and transfer data to source records
echo "Setting up test database schema..."
python3 -m openmatch.tests.setup_test_db

echo "Running database tests..."
python3 -m pytest openmatch/tests/test_db_operations.py -v 