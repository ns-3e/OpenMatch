# OpenMatch Testing Guide

This guide explains how to set up and run tests for the OpenMatch project.

## Prerequisites

Before running the tests, ensure you have the following installed:

- Python 3.8 or higher
- PostgreSQL 12 or higher
- pip (Python package manager)

### macOS Prerequisites

If you're on macOS and haven't installed Python yet:

1. Install Python using Homebrew:
```bash
brew install python@3.11  # or python@3.10, python@3.9, etc.
```

2. Verify the installation:
```bash
python3 --version
```

## Environment Setup

1. Clone the repository and navigate to the project root:
```bash
git clone <repository-url>
cd OpenMatch
```

2. Create and activate a virtual environment:

On macOS/Linux:
```bash
python3 -m venv venv
source venv/bin/activate
```

On Windows:
```bash
python -m venv venv
venv\Scripts\activate
```

3. Install dependencies:

First, install the core dependencies with specific versions to avoid compatibility issues:
```bash
# Install core ML dependencies
pip3 install sentence-transformers==2.2.2
pip3 install huggingface-hub==0.16.4
pip3 install transformers==4.30.2
pip3 install torch==2.0.1

# Install string matching dependencies
pip3 install jellyfish==1.0.0
pip3 install python-Levenshtein==0.21.1

# Install testing dependencies
pip3 install pytest==7.4.0
pip3 install pytest-cov==4.1.0
pip3 install pytest-xdist==3.3.1

# Install database dependencies
pip3 install sqlalchemy==2.0.20
pip3 install psycopg2-binary==2.9.7
pip3 install pandas==2.0.3

# Install utility dependencies
pip3 install tqdm==4.65.0
pip3 install numpy==1.24.3
pip3 install faiss-cpu==1.7.4  # Use faiss-gpu if you have CUDA
```

Then install any remaining requirements:
```bash
pip3 install -r requirements.txt
```

Finally, install the package in development mode:
```bash
pip3 install -e . --no-deps
```

4. Set up environment variables:

On macOS/Linux:
```bash
export PYTHONPATH="$(pwd)"
export POSTGRES_PASSWORD="your_postgres_password"  # Required for database access
```

On Windows (PowerShell):
```powershell
$env:PYTHONPATH = $(pwd)
$env:POSTGRES_PASSWORD = "your_postgres_password"
```

## Package Installation

Install the package in development mode:
```bash
pip install -e . --no-deps
```

## Dependency Management

The project has specific version requirements to ensure compatibility:

### Core Dependencies
- sentence-transformers==2.2.2
- huggingface-hub==0.16.4
- transformers==4.30.2
- torch==2.0.1

### String Matching Dependencies
- jellyfish==1.0.0
- python-Levenshtein==0.21.1

### Testing Dependencies
- pytest==7.4.0
- pytest-cov==4.1.0
- pytest-xdist==3.3.1

### Database Dependencies
- sqlalchemy==2.0.20
- psycopg2-binary==2.9.7
- pandas==2.0.3

### Utility Dependencies
- tqdm==4.65.0
- numpy==1.24.3
- faiss-cpu==1.7.4 (or faiss-gpu for CUDA support)

If you encounter dependency-related errors:

1. Clean your virtual environment:
```bash
deactivate  # If you're in a virtual environment
rm -rf venv  # Remove existing virtual environment
python3 -m venv venv  # Create a new virtual environment
source venv/bin/activate  # Activate it
```

2. Install dependencies in the correct order:
```bash
# Core ML dependencies
pip3 install sentence-transformers==2.2.2
pip3 install huggingface-hub==0.16.4
pip3 install transformers==4.30.2
pip3 install torch==2.0.1

# String matching dependencies
pip3 install jellyfish==1.0.0
pip3 install python-Levenshtein==0.21.1

# Testing dependencies
pip3 install pytest==7.4.0
pip3 install pytest-cov==4.1.0
pip3 install pytest-xdist==3.3.1

# Database dependencies
pip3 install sqlalchemy==2.0.20
pip3 install psycopg2-binary==2.9.7
pip3 install pandas==2.0.3

# Utility dependencies
pip3 install tqdm==4.65.0
pip3 install numpy==1.24.3
pip3 install faiss-cpu==1.7.4

# Then the rest of the requirements
pip3 install -r requirements.txt

# Finally, install the package in development mode
pip3 install -e . --no-deps
```

## Database Setup

### 1. PostgreSQL Installation and Setup

On macOS:
```bash
# Install PostgreSQL using Homebrew
brew install postgresql@14

# Start PostgreSQL service
brew services start postgresql@14

# Create postgres user with appropriate permissions
createuser -s postgres

# Set password for postgres user
psql postgres -c "ALTER USER postgres WITH PASSWORD 'postgres';"
```

### 2. Database Configuration

1. Set the database password in your environment:
```bash
export POSTGRES_PASSWORD="postgres"  # Use the password you set above
```

2. Verify PostgreSQL connection:
```bash
# Test connection
psql -h localhost -U postgres -d postgres -c "SELECT version();"
```

If you get a connection error, try these fixes:

a. Check PostgreSQL service status:
```bash
brew services list  # Check if postgresql is running
brew services restart postgresql@14  # Restart if needed
```

b. Verify PostgreSQL socket directory:
```bash
# Create socket directory if it doesn't exist
mkdir -p /tmp/postgresql
chmod 0777 /tmp/postgresql
```

c. Update connection settings in pg_hba.conf:
```bash
# Find your pg_hba.conf location
pg_config --sysconfdir

# Add these lines to pg_hba.conf
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all            postgres                                trust
host    all            postgres         127.0.0.1/32           md5
host    all            postgres         ::1/128                md5
```

### 3. Test Database Setup

The test suite will automatically:
- Create a test database named `openmatch_test` if it doesn't exist
- Initialize the schema and required tables
- Populate the database with test data (500K records by default)

Make sure PostgreSQL is running and accessible with these default settings:
- Host: localhost
- Port: 5432
- Username: postgres
- Database: openmatch_test
- Password: Set via POSTGRES_PASSWORD environment variable

To manually create and set up the test database:
```bash
# Create database
psql -h localhost -U postgres -c "CREATE DATABASE openmatch_test;"

# Verify database creation
psql -h localhost -U postgres -l | grep openmatch_test
```

### 4. Common Database Issues and Solutions

1. Permission Denied Errors:
```bash
# Grant all privileges to postgres user
psql -h localhost -U postgres -c "ALTER USER postgres WITH SUPERUSER;"
```

2. Database Already Exists:
```bash
# Drop and recreate database
psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS openmatch_test;"
psql -h localhost -U postgres -c "CREATE DATABASE openmatch_test;"
```

3. Connection Refused:
```bash
# Check PostgreSQL status
pg_isready -h localhost -p 5432

# If not running, start PostgreSQL
brew services start postgresql@14
```

4. Schema Creation Errors:
```bash
# Reset schema
psql -h localhost -U postgres -d openmatch_test -c "DROP SCHEMA IF EXISTS mdm CASCADE;"
```

5. Port Conflicts:
```bash
# Check if another process is using port 5432
lsof -i :5432

# If needed, specify a different port in your connection string
export POSTGRES_PORT=5433  # Use a different port
```

### 5. Verifying Database Setup

Run these commands to verify your setup:

```bash
# 1. Check PostgreSQL connection
psql -h localhost -U postgres -c "\conninfo"

# 2. List databases
psql -h localhost -U postgres -l

# 3. Check schema access
psql -h localhost -U postgres -d openmatch_test -c "\dn"

# 4. Verify table creation permissions
psql -h localhost -U postgres -d openmatch_test -c "CREATE TABLE test_table (id serial primary key);"
psql -h localhost -U postgres -d openmatch_test -c "DROP TABLE test_table;"
```

### 6. Running Tests with Clean Database

To start fresh with a new database:

```bash
# 1. Stop any running tests
pkill -f pytest

# 2. Drop existing database
psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS openmatch_test;"

# 3. Create new database
psql -h localhost -U postgres -c "CREATE DATABASE openmatch_test;"

# 4. Run tests with fresh database
./openmatch/tests/run_db_tests.sh
```

If you're still experiencing issues, try running the tests with debug output:
```bash
PYTHONPATH="$(pwd)" POSTGRES_PASSWORD="postgres" pytest openmatch/tests/test_db_operations.py -v --pdb
```

## Running Tests

### Running All Tests

To run the complete test suite:
```bash
python -m pytest openmatch/tests -v
```

### Running Specific Test Files

1. Database Operations Tests:
```bash
python -m pytest openmatch/tests/test_db_operations.py -v
```

2. Matching Rules Tests:
```bash
python -m pytest openmatch/tests/test_rules.py -v
```

3. Matching Engine Tests:
```bash
python -m pytest openmatch/tests/test_engine.py -v
```

### Running Database Tests with Fresh Data

To run database tests with a fresh dataset:
```bash
./openmatch/tests/run_db_tests.sh
```

This script will:
1. Check if the test database exists
2. Create it if necessary
3. Initialize schema and load test data if empty
4. Run the database-specific tests

## Test Data Generation

The test suite uses synthetic data generated with realistic patterns. You can customize the test data generation by modifying parameters in `setup_test_db.py`:

- `num_records`: Number of base records to generate (default: 10,000)
- `duplicate_rate`: Percentage of records that will have duplicates (default: 50%)
- `max_duplicates`: Maximum number of duplicates per record (default: 3)
- `variation_rate`: Percentage of duplicates that will have variations (default: 30%)

To generate a new test dataset with custom parameters:
```python
from openmatch.tests.setup_test_db import setup_test_database

setup_test_database(
    num_records=20000,           # Generate 20K base records
    duplicate_rate=0.6,          # 60% will have duplicates
    max_duplicates=4,           # Up to 4 duplicates per record
    variation_rate=0.4          # 40% will have variations
)
```

## Test Configuration

The test suite uses pytest fixtures defined in `conftest.py`. Key configurations include:

- Database connection parameters
- Test data generation settings
- Mock configurations for external services

To modify test configurations, edit the appropriate fixtures in `conftest.py`.

## Troubleshooting

1. Database Connection Issues:
   - Ensure PostgreSQL is running
   - Verify the POSTGRES_PASSWORD environment variable is set
   - Check database connection parameters in test configuration

2. Import Errors:
   - Verify PYTHONPATH is set correctly
   - Ensure all dependencies are installed
   - Check for any missing package installations

3. Test Data Generation Issues:
   - Ensure sufficient disk space for test data
   - Check PostgreSQL has necessary permissions
   - Verify memory availability for large datasets

## Contributing New Tests

When adding new tests:

1. Create test files in the `openmatch/tests` directory
2. Follow the existing naming convention: `test_*.py`
3. Use pytest fixtures from `conftest.py` where appropriate
4. Include both positive and negative test cases
5. Add appropriate assertions and error handling
6. Document any new test requirements or configurations

## Performance Testing

For performance testing with large datasets:

1. Modify `num_records` in `setup_test_db.py`
2. Use pytest's benchmark fixtures for timing sensitive operations
3. Monitor system resources during test execution
4. Consider using pytest-xdist for parallel test execution

## Continuous Integration

The test suite is designed to run in CI environments. Key considerations:

- Tests should be idempotent
- Database state is reset between test runs
- Resource cleanup is handled automatically
- Test execution time is optimized for CI pipelines 