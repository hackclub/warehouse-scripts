#!/usr/bin/env python3
"""
Postgres Schema Migration Script

This script migrates data from one Postgres database to another,
remapping the source schema to a target schema. It optionally
supports incremental updates by tracking the last modified timestamp.
"""

import os
import sys
import argparse
import logging
import time
from datetime import datetime, timezone, date, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Migrate data from one Postgres schema to another with schema remapping.')
    parser.add_argument('--source-db-url', help='Source database URL')
    parser.add_argument('--target-db-url', help='Target database URL')
    parser.add_argument('--source-schema', default='public', help='Source schema name (default: public)')
    parser.add_argument('--target-schema', required=True, help='Target schema name')
    parser.add_argument('--incremental', action='store_true', help='Enable incremental updates')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--batch-size', type=int, default=1000, help='Number of rows to process in a batch (default: 1000)')
    
    args = parser.parse_args()
    
    # Use environment variables if not provided as arguments
    if not args.source_db_url:
        args.source_db_url = os.getenv('SOURCE_DB_URL') or os.getenv('HACKATIME_DB_URL')
    if not args.target_db_url:
        args.target_db_url = os.getenv('TARGET_DB_URL') or os.getenv('WAREHOUSE_DB_URL')
        
    if not args.source_db_url or not args.target_db_url:
        parser.error("Database URLs must be provided either as arguments or environment variables")
        
    if args.debug:
        logger.setLevel(logging.DEBUG)
        
    return args

def get_connection(db_url):
    """Create a database connection."""
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)

def get_tables(conn, schema):
    """Get all tables in the specified schema."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        return [table['table_name'] for table in cur.fetchall()]

def get_columns(conn, schema, table):
    """Get column information for a table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT column_name, data_type, column_default, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return cur.fetchall()

def get_primary_keys(conn, schema, table):
    """Get primary key columns for a table."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %s
            AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """, (schema, table))
        return [pk['column_name'] for pk in cur.fetchall()]

def get_sequences(conn, schema):
    """Get all sequences in the specified schema."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema = %s
            ORDER BY sequence_name
        """, (schema,))
        return [seq['sequence_name'] for seq in cur.fetchall()]

def get_sequence_details(conn, schema, sequence_name):
    """Get details for a specific sequence."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get current value and other properties
        cur.execute(f"""
            SELECT * FROM {schema}."{sequence_name}"
        """)
        seq_info = cur.fetchone()
        
        # Get data type
        cur.execute("""
            SELECT data_type
            FROM information_schema.sequences
            WHERE sequence_schema = %s
            AND sequence_name = %s
        """, (schema, sequence_name))
        data_type_info = cur.fetchone()
        
        return {**seq_info, 'data_type': data_type_info['data_type'] if data_type_info else 'bigint'}

def create_schema_if_not_exists(conn, schema):
    """Create schema if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.commit()
    logger.info(f"Ensured schema {schema} exists")

def create_sequence_in_target(conn, source_schema, target_schema, sequence_name, seq_details=None):
    """Create sequence in target schema with same properties as in source schema."""
    # If sequence already exists in target schema, skip creation
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.sequences 
                WHERE sequence_schema = %s AND sequence_name = %s
            )
        """, (target_schema, sequence_name))
        exists = cur.fetchone()[0]
        if exists:
            logger.info(f"Sequence {target_schema}.{sequence_name} already exists, skipping creation")
            return
    
    # Default create sequence with basic options
    create_seq_sql = f"""
        CREATE SEQUENCE {target_schema}."{sequence_name}"
    """
    
    # If we have detailed sequence information, use it
    if seq_details:
        # Convert seq_details to appropriate CREATE SEQUENCE options
        # This is simplified, add more options as needed
        start_value = seq_details.get('last_value', 1)
        increment = seq_details.get('increment_by', 1)
        data_type = seq_details.get('data_type', 'bigint')
        
        create_seq_sql = f"""
            CREATE SEQUENCE {target_schema}."{sequence_name}"
            INCREMENT BY {increment}
            START WITH {start_value} 
            AS {data_type}
        """
    
    with conn.cursor() as cur:
        cur.execute(create_seq_sql)
    conn.commit()
    logger.info(f"Created sequence {target_schema}.{sequence_name}")

def fix_sequence_references(conn, source_schema, target_schema, columns):
    """Fix sequence references in column defaults."""
    fixed_columns = []
    
    for col in columns:
        # Check if column default references a sequence
        if col['column_default'] and 'nextval' in col['column_default']:
            # Extract sequence name from nextval expression
            # Example: nextval('public.aliases_id_seq'::regclass) or nextval('aliases_id_seq'::regclass)
            import re
            seq_match = re.search(r"nextval\('(?:[\w]+\.)?([^']+)'", col['column_default'])
            if seq_match:
                seq_name = seq_match.group(1)
                # Replace schema reference in the default expression
                new_default = col['column_default'].replace(f"{source_schema}.{seq_name}", f"{target_schema}.{seq_name}")
                new_default = new_default.replace(f"'{seq_name}'", f"'{target_schema}.{seq_name}'")
                col_copy = dict(col)
                col_copy['column_default'] = new_default
                fixed_columns.append(col_copy)
                continue
        
        fixed_columns.append(col)
    
    return fixed_columns

def create_table_in_target(conn, source_schema, target_schema, table, columns, primary_keys):
    """Create table in target database with the same structure as source."""
    # Fix sequence references in column defaults
    fixed_columns = fix_sequence_references(conn, source_schema, target_schema, columns)
    
    col_defs = []
    for col in fixed_columns:
        nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
        default = f"DEFAULT {col['column_default']}" if col['column_default'] else ""
        col_defs.append(f"\"{col['column_name']}\" {col['data_type']} {default} {nullable}")
    
    pk_constraint = ""
    if primary_keys:
        pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
        pk_constraint = f", PRIMARY KEY ({pk_cols})"
    
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_schema}."{table}" (
            {', '.join(col_defs)}
            {pk_constraint}
        )
    """
    
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()
    logger.info(f"Created table {target_schema}.{table} if it didn't exist")

def get_last_modified_column(columns):
    """Identify a timestamp column that might indicate last modification."""
    timestamp_columns = [
        col['column_name'] for col in columns 
        if 'timestamp' in col['data_type'].lower() or 'date' in col['data_type'].lower()
    ]
    
    # Prefer columns that likely represent modification time
    for name_pattern in ['updated_at', 'modified_at', 'modified', 'updated', 'changed_at']:
        for col in timestamp_columns:
            if name_pattern in col.lower():
                return col
    
    # If no modification column found, try to find creation column
    for name_pattern in ['created_at', 'creation_date', 'created']:
        for col in timestamp_columns:
            if name_pattern in col.lower():
                return col
    
    # Return the first timestamp column if any exists
    return timestamp_columns[0] if timestamp_columns else None

def get_table_size(conn, schema, table):
    """Get approximate row count for a table."""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT reltuples::bigint AS approximate_row_count
            FROM pg_class
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE pg_namespace.nspname = %s
            AND pg_class.relname = %s
        """, (schema, table))
        result = cur.fetchone()
        if result:
            return result[0]
        return 0

def copy_data(source_conn, target_conn, source_schema, target_schema, table, columns, 
              incremental=False, last_sync_time=None, modified_column=None, 
              batch_size=1000):  # Default to 1,000 rows per batch
    """Copy data from source to target table using simple batch processing with server-side cursor."""
    column_names = [f'"{col["column_name"]}"' for col in columns]
    columns_str = ', '.join(column_names)
    
    # Build WHERE clause for incremental updates
    where_clause = ""
    params = []
    if incremental and last_sync_time and modified_column:
        where_clause = f"WHERE \"{modified_column}\" > %s"
        params = [last_sync_time]
        logger.info(f"Using incremental update with {modified_column} > {last_sync_time}")

    # Get table size for reporting
    table_size = get_table_size(source_conn, source_schema, table)
    logger.info(f"Table {table} has approximately {table_size} rows")
    
    # Check target table row count
    target_row_count = 0
    try:
        with target_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {target_schema}.\"{table}\" LIMIT 1")
            target_row_count = cur.fetchone()[0]
            logger.info(f"Target table {target_schema}.{table} has {target_row_count} rows")
    except Exception as e:
        logger.warning(f"Error checking target table row count: {e}")

    # If target is empty, force a full copy regardless of incremental settings
    if target_row_count == 0:
        logger.info(f"Target table is empty. Forcing full copy for {table}")
        where_clause = ""
        params = []
        last_sync_time = None

    # Create a placeholder string for the values (%s, %s, ...)
    placeholders = ', '.join(['%s'] * len(columns))
    
    # Prepare the insert statement
    insert_sql = f"""
        INSERT INTO {target_schema}."{table}" ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """
    
    # Use a named server-side cursor to avoid loading everything into memory
    logger.info(f"Starting batch copy for {table} with batch size {batch_size}")
    
    total_processed = 0
    start_time = time.time()
    last_report_time = start_time
    
    try:
        # Use a named server-side cursor to fetch batches without loading all rows into memory
        # The server will only send batch_size rows at a time
        with source_conn.cursor(name='server_side_cursor') as source_cursor:
            # Construct and log the select query
            select_sql = f"""
                SELECT {columns_str}
                FROM {source_schema}."{table}"
                {where_clause}
            """
            logger.info(f"Executing query: {select_sql} with params {params}")
            source_cursor.execute(select_sql, params)
            
            # Process in batches
            with target_conn.cursor() as target_cursor:
                while True:
                    batch = source_cursor.fetchmany(batch_size)
                    if not batch:
                        logger.info(f"No more rows to process, fetched total of {total_processed:,} rows")
                        break
                    
                    # Execute batch insert
                    target_cursor.executemany(insert_sql, batch)
                    target_conn.commit()
                    
                    total_processed += len(batch)
                    
                    # Report progress every 30 seconds
                    current_time = time.time()
                    if total_processed % (10 * batch_size) == 0 or current_time - last_report_time > 30:
                        elapsed = current_time - start_time
                        rows_per_sec = int(total_processed / elapsed) if elapsed > 0 else 0
                        percent_done = (total_processed / table_size * 100) if table_size > 0 else 0
                        logger.info(f"Progress: {total_processed:,} rows copied ({percent_done:.1f}%) to {target_schema}.{table} ({rows_per_sec} rows/sec)")
                        last_report_time = current_time
    
    except Exception as e:
        logger.error(f"Error copying data for {table}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    
    # Final count to verify
    with target_conn.cursor() as verify_cursor:
        verify_cursor.execute(f"SELECT COUNT(*) FROM {target_schema}.\"{table}\"")
        final_count = verify_cursor.fetchone()[0]
    
    # Report final statistics
    elapsed_time = time.time() - start_time
    if total_processed > 0:
        rows_per_second = int(total_processed / elapsed_time) if elapsed_time > 0 else 0
        logger.info(f"Copy complete: Processed {total_processed:,} rows in {elapsed_time:.2f} seconds ({rows_per_second} rows/sec)")
    logger.info(f"Final count in {target_schema}.{table}: {final_count:,} rows")
    
    # Return the latest modification time if available
    if modified_column:
        with source_conn.cursor() as cur:
            cur.execute(f"""
                SELECT MAX("{modified_column}") 
                FROM {source_schema}."{table}"
            """)
            latest_time = cur.fetchone()[0]
            logger.info(f"Latest timestamp for {table}.{modified_column}: {latest_time}")
            return latest_time
    
    return None

def get_latest_timestamp(source_conn, source_schema, table, modified_column):
    """Get the latest timestamp from a table for a specific column."""
    if not modified_column:
        return None
    
    with source_conn.cursor() as cur:
        cur.execute(f"""
            SELECT MAX("{modified_column}") 
            FROM {source_schema}."{table}"
        """)
        return cur.fetchone()[0]

def try_direct_transfer(source_conn, target_conn, source_schema, target_schema, table, columns, where_clause, params):
    """
    Try to use direct database-to-database transfer methods for better performance.
    Returns True if successful, False if we need to fall back to regular batch processing.
    """
    try:
        # Check if we're connecting to the same database server
        # If so, we can use a much more efficient method
        source_conn_info = source_conn.get_dsn_parameters()
        target_conn_info = target_conn.get_dsn_parameters()
        
        same_server = (
            source_conn_info.get('host') == target_conn_info.get('host') and
            source_conn_info.get('port') == target_conn_info.get('port')
        )
        
        if same_server:
            logger.info(f"Source and target are on the same server, using direct INSERT")
            
            # In this case, we can use direct INSERT ... SELECT
            column_names = [f'"{col["column_name"]}"' for col in columns]
            columns_str = ', '.join(column_names)
            
            with target_conn.cursor() as cursor:
                # Build the INSERT ... SELECT statement
                sql = f"""
                    INSERT INTO {target_schema}."{table}" ({columns_str})
                    SELECT {columns_str}
                    FROM {source_schema}."{table}"
                    {where_clause}
                    ON CONFLICT DO NOTHING
                """
                
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                
                row_count = cursor.rowcount
                target_conn.commit()
                
                logger.info(f"Directly inserted {row_count} rows into {target_schema}.{table}")
                return True
        
        # Try using dblink (PostgreSQL extension for database-to-database connections)
        # This requires the dblink extension to be installed on the target database
        try:
            with target_conn.cursor() as cursor:
                # Check if dblink extension is available
                cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'dblink'")
                has_dblink = cursor.fetchone() is not None
                
                if has_dblink:
                    logger.info(f"Using dblink for efficient cross-server data transfer")
                    
                    # Get the connection string for the source database
                    source_dsn = " ".join([
                        f"{key}={value}"
                        for key, value in source_conn.get_dsn_parameters().items()
                        if key in ('host', 'port', 'dbname', 'user', 'password')
                    ])
                    
                    # Create a connection to the source database
                    cursor.execute(f"SELECT dblink_connect('source_conn', '{source_dsn}')")
                    
                    # Get column names
                    column_names = [f'"{col["column_name"]}"' for col in columns]
                    columns_str = ', '.join(column_names)
                    
                    # Prepare the query
                    query = f"""
                        SELECT {columns_str}
                        FROM {source_schema}."{table}"
                        {where_clause}
                    """
                    
                    # Use dblink to execute a query on the source database and insert results directly
                    if params:
                        # We need to handle parameters differently with dblink
                        # This is a simplified approach; for complex WHERE clauses, this might need adjustment
                        for i, param in enumerate(params):
                            if isinstance(param, str):
                                query = query.replace(f"%s", f"'{param}'", 1)
                            elif isinstance(param, datetime):
                                query = query.replace(f"%s", f"'{param.isoformat()}'", 1)
                            else:
                                query = query.replace(f"%s", f"{param}", 1)
                    
                    insert_sql = f"""
                        INSERT INTO {target_schema}."{table}" ({columns_str})
                        SELECT * FROM dblink('source_conn', $${query}$$) 
                        AS t({", ".join([f"{col['column_name']} {col['data_type']}" for col in columns])})
                        ON CONFLICT DO NOTHING
                    """
                    
                    cursor.execute(insert_sql)
                    row_count = cursor.rowcount
                    
                    # Close the dblink connection
                    cursor.execute("SELECT dblink_disconnect('source_conn')")
                    
                    target_conn.commit()
                    logger.info(f"Used dblink to copy {row_count} rows to {target_schema}.{table}")
                    return True
        except Exception as e:
            logger.warning(f"Could not use dblink: {e}")
        
        return False
    except Exception as e:
        logger.warning(f"Error in direct transfer attempt: {e}")
        return False

def get_last_target_timestamp(conn, schema, table, modified_column):
    """Get the most recent timestamp from the target table for incremental updates."""
    if not modified_column:
        return None
    
    try:
        with conn.cursor() as cur:
            # Query the maximum timestamp in the target table
            cur.execute(f"""
                SELECT MAX("{modified_column}") 
                FROM {schema}."{table}"
            """)
            result = cur.fetchone()
            
            if result and result[0]:
                timestamp = result[0]
                logger.info(f"Found existing data in {schema}.{table} with newest timestamp: {timestamp}")
                return timestamp
            
            logger.info(f"No existing data found in {schema}.{table} for incremental updates")
            return None
    
    except psycopg2.Error as e:
        # Table might not exist yet, which is fine
        logger.debug(f"Could not query {schema}.{table}: {e}")
        return None

def main():
    """Main function."""
    args = parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Connect to databases
    source_conn = get_connection(args.source_db_url)
    target_conn = get_connection(args.target_db_url)
    
    logger.info(f"Starting migration from {args.source_schema} to {args.target_schema}")
    
    try:
        # Create target schema if it doesn't exist
        create_schema_if_not_exists(target_conn, args.target_schema)
        
        # Get sequences from source schema
        sequences = get_sequences(source_conn, args.source_schema)
        logger.info(f"Found {len(sequences)} sequences in source schema {args.source_schema}")
        
        # Create sequences in target schema
        for seq_name in sequences:
            seq_details = get_sequence_details(source_conn, args.source_schema, seq_name)
            create_sequence_in_target(target_conn, args.source_schema, args.target_schema, seq_name, seq_details)
        
        # Get tables from source schema
        tables = get_tables(source_conn, args.source_schema)
        logger.info(f"Found {len(tables)} tables in source schema {args.source_schema}")
        
        # Process each table
        for table in tables:
            logger.info(f"Processing table: {table}")
            
            # Get columns for this table
            columns = get_columns(source_conn, args.source_schema, table)
            
            # Skip tables with no columns (views, etc.)
            if not columns:
                logger.warning(f"No columns found for {table}, skipping")
                continue
            
            # Get primary keys
            primary_keys = get_primary_keys(source_conn, args.source_schema, table)
            
            # Create table in target
            create_table_in_target(target_conn, args.source_schema, args.target_schema, table, columns, primary_keys)
            
            # Get column for incremental updates (if enabled)
            modified_column = None
            if args.incremental:
                modified_column = get_last_modified_column(columns)
                if modified_column:
                    logger.info(f"Using column {modified_column} for incremental updates on {table}")
                else:
                    logger.info(f"No suitable timestamp column found for incremental updates on {table}")
            
            # Get last sync time by querying the target table
            last_sync_time = None
            if args.incremental and modified_column:
                last_sync_time = get_last_target_timestamp(target_conn, args.target_schema, table, modified_column)
            
            # Copy data from source to target
            copy_data(
                source_conn, target_conn,
                args.source_schema, args.target_schema,
                table, columns,
                incremental=args.incremental,
                last_sync_time=last_sync_time,
                modified_column=modified_column,
                batch_size=args.batch_size
            )
        
        # Fix sequence references
        fix_sequence_references(target_conn, args.source_schema, args.target_schema, columns)
            
    finally:
        # Close connections
        source_conn.close()
        target_conn.close()
        
    logger.info("Migration completed successfully")
    return 0

if __name__ == "__main__":
    main() 