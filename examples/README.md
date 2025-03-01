# OpenMatch Examples

This directory contains example code and configurations to help you get started with OpenMatch for Master Data Management (MDM) operations.

## Directory Structure

```
examples/
├── config/
│   └── settings.py         # Configuration settings for MDM operations
├── data/                   # Directory for sample data files
└── mdm_example.py         # Example script demonstrating core functionality
```

## Prerequisites

1. PostgreSQL 14+ with pgvector extension installed
2. Python 3.7+
3. OpenMatch library installed in your Python environment

## Database Setup

1. Install PostgreSQL and pgvector if you haven't already:

```bash
# On Ubuntu/Debian
sudo apt-get install postgresql-14 postgresql-14-pgvector

# On macOS with Homebrew
brew install postgresql@14
brew install pgvector
```

2. Create the MDM database:

```bash
psql -U postgres
CREATE DATABASE mdm;
\c mdm
CREATE EXTENSION IF NOT EXISTS vector;
```

## Configuration

1. Set up your environment variables:

```bash
export MDM_DB_USER=your_username
export MDM_DB_PASSWORD=your_password
export SOURCE_DB_USER=your_source_username
export SOURCE_DB_PASSWORD=your_source_password
```

2. Review and modify `config/settings.py` as needed:
   - Adjust database connection settings
   - Configure matching thresholds and weights
   - Set vector search parameters
   - Define source system configurations

## Running the Example

1. Make sure you're in the project root directory:

```bash
cd /path/to/OpenMatch
```

2. Run the example script:

```bash
python examples/mdm_example.py
```

The script will:
- Initialize the MDM system
- Process any pending matches in the system
- Demonstrate finding matches for a sample record

## Customizing the Example

1. To modify matching criteria:
   - Edit `MATCH_SETTINGS` in `config/settings.py`
   - Adjust thresholds and weights

2. To change vector search settings:
   - Edit `VECTOR_SETTINGS` in `config/settings.py`
   - Modify dimension size or index type

3. To add your own source system:
   - Add a new entry to `SOURCE_SYSTEMS` in `config/settings.py`
   - Configure the connection and field mappings

## Troubleshooting

1. Database Connection Issues:
   - Verify PostgreSQL is running
   - Check environment variables are set correctly
   - Ensure database and user permissions are properly configured

2. Vector Search Issues:
   - Verify pgvector extension is installed
   - Check vector dimensions match your configuration
   - Ensure indexes are properly created

3. Memory Issues:
   - Adjust `BATCH_SIZE` in processing settings
   - Monitor system memory usage
   - Consider reducing vector dimensions

## Next Steps

1. Explore the OpenMatch documentation for advanced features
2. Customize the matching rules for your use case
3. Integrate with your source systems
4. Implement custom merge strategies
5. Set up monitoring and logging

For more information, refer to the main OpenMatch documentation. 