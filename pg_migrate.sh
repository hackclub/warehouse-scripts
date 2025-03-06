#!/bin/bash
# Postgres Schema Migration Script Wrapper
# This script runs the pg_migrate.py Python script and can be scheduled via cron

set -e

# Directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Load environment variables
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Make sure Python script is executable
chmod +x pg_migrate.py

# Check if virtual environment exists, create if not
if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
  
  # Activate virtual environment
  source venv/bin/activate
  
  # Install required packages
  pip install psycopg2-binary python-dotenv
else
  # Activate virtual environment
  source venv/bin/activate
fi

# Display help message
show_help() {
  echo "Usage: $0 [options] [source_db_url] [target_db_url] target_schema [incremental]"
  echo ""
  echo "Arguments:"
  echo "  source_db_url     Optional. URL for source database. Defaults to HACKATIME_DB_URL from .env"
  echo "  target_db_url     Optional. URL for target database. Defaults to WAREHOUSE_DB_URL from .env"
  echo "  target_schema     Required. Schema name to create in target database"
  echo "  incremental       Optional. true or false, defaults to true"
  echo ""
  echo "Options:"
  echo "  -h, --help        Show this help message"
  echo "  --batch-size N    Number of rows to process in a batch (default: 1000)"
  echo "  --debug           Enable debug logging"
  echo ""
  echo "Example: $0 --batch-size=500 \"\" \"\" db1"
  exit 1
}

# Parse command line options
POSITIONAL_ARGS=()
BATCH_SIZE=""
DEBUG=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      show_help
      ;;
    --batch-size)
      BATCH_SIZE="--batch-size $2"
      shift 2
      ;;
    --debug)
      DEBUG="--debug"
      shift
      ;;
    --batch-size=*)
      BATCH_SIZE="--batch-size ${1#*=}"
      shift
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

# Restore positional arguments
set -- "${POSITIONAL_ARGS[@]}"

# Parse command line arguments
SOURCE_DB=${1:-$HACKATIME_DB_URL}
TARGET_DB=${2:-$WAREHOUSE_DB_URL}
TARGET_SCHEMA=$3
INCREMENTAL=${4:-true}

# Validate inputs
if [ -z "$TARGET_SCHEMA" ]; then
  echo "ERROR: Target schema name must be provided"
  show_help
fi

# Convert incremental flag to Python argument
INCREMENTAL_ARG=""
if [ "$INCREMENTAL" = "true" ]; then
  INCREMENTAL_ARG="--incremental"
fi

# Turn off 'exit on error' for the migration command
# so we can handle errors better
set +e

# Run the migration
echo "Starting migration to schema '$TARGET_SCHEMA'..."
./pg_migrate.py \
  --source-db-url="$SOURCE_DB" \
  --target-db-url="$TARGET_DB" \
  --target-schema="$TARGET_SCHEMA" \
  $INCREMENTAL_ARG \
  $BATCH_SIZE \
  $DEBUG

# Capture the exit status
EXIT_STATUS=$?

# Turn 'exit on error' back on
set -e

# Check if the migration was successful
if [ $EXIT_STATUS -eq 0 ]; then
  echo "Migration completed successfully!"
else
  echo "Migration failed with exit code $EXIT_STATUS"
  echo "You can try running again with a different batch size: --batch-size=500"
  echo "Check logs for more details"
  exit $EXIT_STATUS
fi 