#!/bin/bash

# Exit on error
set -e

echo "🚀 Starting OpenMatch Database Setup..."

# Check if PostgreSQL is running
echo "📋 Checking PostgreSQL status..."
if ! pg_isready > /dev/null 2>&1; then
    echo "❌ PostgreSQL is not running. Starting PostgreSQL..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew services start postgresql@14
    else
        sudo service postgresql start
    fi
    sleep 5  # Wait for PostgreSQL to start
fi

# Create Python virtual environment
echo "🐍 Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install faker==19.13.0 typer==0.9.0 pandas==2.1.4 numpy==1.26.3 psycopg2-binary==2.9.9
pip install openmatch[db]

# Database setup
echo "🗄️ Setting up database..."
DB_NAME="openmatch_test"
DB_USER=$(whoami)
DB_PASSWORD="postgres"

# Drop database if it exists
dropdb --if-exists $DB_NAME

# Create database
createdb $DB_NAME

# Create .env file
echo "📝 Creating .env file..."
cat << EOF > .env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=$DB_NAME
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
EOF

# Generate test data
echo "🔄 Generating test data..."
python test_data/generate_test_data.py \
    --num-records 100000 \
    --format postgres \
    --db-name $DB_NAME \
    --db-user $DB_USER \
    --db-password $DB_PASSWORD \
    --db-host localhost

# Verify data
echo "✅ Verifying data..."
psql $DB_NAME << EOF
\\echo '\nRecord Counts:'
SELECT 'Persons' as table_name, COUNT(*) as count FROM persons
UNION ALL
SELECT 'Emails', COUNT(*) FROM emails
UNION ALL
SELECT 'Phones', COUNT(*) FROM phones
UNION ALL
SELECT 'Addresses', COUNT(*) FROM addresses
ORDER BY table_name;

\\echo '\nSample Duplicates:'
WITH duplicates AS (
    SELECT p1.first_name, p1.last_name, p1.source as source1, p2.source as source2
    FROM persons p1
    JOIN persons p2 ON 
        (p1.first_name SIMILAR TO p2.first_name || '%' OR p2.first_name SIMILAR TO p1.first_name || '%')
        AND p1.last_name = p2.last_name
        AND p1.id != p2.id
    LIMIT 5
)
SELECT * FROM duplicates;
EOF

echo "
✨ Setup Complete! ✨

Your OpenMatch test database is ready with:
- Database name: $DB_NAME
- User: $DB_USER
- ~100,000 test records with fuzzy duplicates
- 1:M relationships for emails, phones, and addresses

To connect with pgAdmin:
1. Create a new server connection
2. Host: localhost
3. Port: 5432
4. Database: $DB_NAME
5. Username: $DB_USER
6. Password: $DB_PASSWORD

To start using OpenMatch with this data, see the examples in DB_QUICKSTART.md
" 