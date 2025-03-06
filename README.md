_AI generated_

# Postgres Schema Migration Tool

A tool for migrating data from one Postgres database to another with schema remapping. This is particularly useful for copying data from Rails applications where you want to preserve the data structure but place it into a specific schema in the target database.

## Features

- Migrate tables and data from one Postgres database to another
- Remap source schema (typically `public`) to a target schema of your choice
- Support for incremental updates by tracking the most recent timestamp
- Automatically creates target schema and tables if they don't exist
- Preserves primary key constraints
- Handles batched inserts for better performance
- Memory-efficient processing of large tables

## Requirements

- Python 3.6+
- Bash shell
- Access to source and target Postgres databases

## Setup

1. Clone this repository:
   ```
   git clone <repository-url>
   cd <repository-dir>
   ```

2. Set up your environment variables in a `.env` file:
   ```
   HACKATIME_DB_URL="postgres://username:password@hostname:port/dbname"
   WAREHOUSE_DB_URL="postgres://username:password@hostname:port/dbname"
   ```

3. Make the scripts executable:
   ```
   chmod +x pg_migrate.py pg_migrate.sh
   ```

## Usage

### Basic Usage

Run the migration using the shell script:

```bash
./pg_migrate.sh [options] [source_db_url] [target_db_url] target_schema [incremental]
```

### Command Line Arguments

- `source_db_url` (optional): URL for the source database. Defaults to HACKATIME_DB_URL from .env
- `target_db_url` (optional): URL for the target database. Defaults to WAREHOUSE_DB_URL from .env
- `target_schema` (required): Schema name to create in the target database
- `incremental` (optional): "true" or "false", defaults to "true"

### Options

- `--batch-size N`: Number of rows to process in a batch (default: 100000)
- `--debug`: Enable debug logging

### Examples

```bash
# Migrate data to 'db1' schema with incremental updates
./pg_migrate.sh postgres://user:pass@host:port/source_db postgres://user:pass@host:port/target_db db1 true

# Using environment variables from .env
./pg_migrate.sh "" "" db1

# Process with smaller batches to reduce memory usage
./pg_migrate.sh --batch-size=10000 "" "" db1
```

### Direct Python Usage

You can also run the Python script directly:

```bash
python3 pg_migrate.py --source-db-url="..." --target-db-url="..." --target-schema="db1" --incremental
```

Additional options:
- `--source-schema`: Source schema name (default: public)
- `--state-file`: File to store migration state (default: .migration_state.json)
- `--batch-size`: Number of rows to process in a batch (default: 100000)
- `--debug`: Enable debug logging

## Handling Large Tables

When migrating very large tables (millions of rows), you may need to adjust the batch size to optimize for your system's memory:

1. **Use incremental updates**: Tables with timestamp columns will automatically use incremental updates if the `--incremental` flag is set (the default).

2. **Adjust batch size**: Use `--batch-size=N` to change the number of rows processed in each batch. Smaller batches use less memory but may be slower.

## Setting Up Scheduled Execution

To run the migration on a regular schedule, you can set up a cron job manually:

```bash
crontab -e
```

Then add a line like this to run the migration every 15 minutes:

```
*/15 * * * * /path/to/pg_migrate.sh "source_db_url" "target_db_url" "target_schema" "true" >> /path/to/pg_migrate.log 2>&1
```

## How It Works

1. The script connects to both source and target databases
2. It gets a list of tables from the source schema
3. For each table:
   - Creates an equivalent table in the target schema if it doesn't exist
   - If incremental mode is enabled, looks for timestamp columns to track changes
   - Copies data from source to target, using timestamps for filtering in incremental mode
4. For incremental updates, it tracks the latest timestamp for each table in a state file

## Troubleshooting

- Check the log file (`pg_migrate.log`) for error messages
- Run with `--debug` flag for more detailed logs
- For "out of memory" errors, reduce the batch size with `--batch-size=10000` or lower
- Make sure database credentials are correct and the user has sufficient privileges
- Verify that network access is available to both databases

## License

[MIT License](LICENSE) 