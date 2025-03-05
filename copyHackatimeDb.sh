#!/bin/bash

# Source environment variables from .env file if it exists
if [ -f .env ]; then
    source .env
    echo "Found and sourced .env file"
fi

# Exit on error
set -e

# Check required environment variables
if [ -z "$HACKATIME_DB_URL" ]; then
    echo "Error: HACKATIME_DB_URL environment variable is not set"
    exit 1
fi

if [ -z "$WAREHOUSE_DB_URL" ]; then
    echo "Error: WAREHOUSE_DB_URL environment variable is not set"
    exit 1
fi

echo "Testing database connections..."

# Test Hackatime DB connection
if ! psql "$HACKATIME_DB_URL" -c '\q' > /dev/null 2>&1; then
    echo "Error: Could not connect to Hackatime database. Please check HACKATIME_DB_URL."
    exit 1
fi

# Test Warehouse DB connection
if ! psql "$WAREHOUSE_DB_URL" -c '\q' > /dev/null 2>&1; then
    echo "Error: Could not connect to Warehouse database. Please check WAREHOUSE_DB_URL."
    exit 1
fi

echo "Database connections verified."

# Create tmp directory if it doesn't exist
mkdir -p tmp

# Create secure temporary file
DUMP_FILE=$(mktemp tmp/hackatime.XXXXXXXXXX)

echo "Dumping Hackatime database..."
# Dump the public schema from HACKATIME_DB_URL
pg_dump "$HACKATIME_DB_URL" \
  --schema=public \
  --format=custom \
  --verbose \
  -f "$DUMP_FILE"

echo "Creating hackatime schema in warehouse if it doesn't exist..."
psql "$WAREHOUSE_DB_URL" -v ON_ERROR_STOP=1 --echo-all -c "CREATE SCHEMA IF NOT EXISTS hackatime;"

echo "Restoring data to warehouse database..."
# Restore the dump to the hackatime schema in WAREHOUSE_DB_URL
pg_restore \
  --dbname="$WAREHOUSE_DB_URL" \
  --schema=hackatime \
  --no-owner \
  --no-privileges \
  --verbose \
  "$DUMP_FILE"

echo "Cleaning up..."
rm "$DUMP_FILE"

echo "Done!"