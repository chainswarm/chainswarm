#!/usr/bin/env python3
"""
Script to execute each schema part individually to test ClickHouse compatibility.
This helps avoid query length limitations by executing each part separately.
"""

import os
import sys
import argparse
import clickhouse_connect
from loguru import logger

def execute_schema_file(client, file_path, partition_size):
    """Execute a single schema file with ClickHouse"""
    logger.info(f"Executing schema file: {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            schema_sql = f.read()
        
        # Replace partition size placeholder
        schema_sql = schema_sql.replace('{PARTITION_SIZE}', str(partition_size))
        
        # Split by semicolon
        statements = []
        current_statement = []
        lines = schema_sql.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('--') or not line:
                continue
                
            current_statement.append(line)
            
            # Check if this line ends with a semicolon
            if line.endswith(';'):
                # Join the current statement and add it to statements
                full_statement = ' '.join(current_statement).strip()
                if full_statement and not full_statement.startswith('--'):
                    statements.append(full_statement.rstrip(';'))
                current_statement = []
        
        # Execute each statement
        for i, statement in enumerate(statements):
            if statement:
                try:
                    logger.info(f"Executing statement {i+1}/{len(statements)}")
                    client.command(statement)
                    logger.success(f"Statement {i+1} executed successfully")
                except Exception as e:
                    # Skip errors for views and indexes that might already exist
                    if "already exists" in str(e).lower():
                        logger.debug(f"Object already exists, skipping: {statement[:50]}...")
                    else:
                        logger.error(f"Error executing statement: {e}")
                        logger.error(f"Statement: {statement[:100]}...")
                        raise
        
        logger.success(f"Successfully executed schema file: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error executing schema file {file_path}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Execute ClickHouse schema files individually')
    parser.add_argument('--host', default='localhost', help='ClickHouse host')
    parser.add_argument('--port', default=8123, type=int, help='ClickHouse port')
    parser.add_argument('--user', default='default', help='ClickHouse user')
    parser.add_argument('--password', default='', help='ClickHouse password')
    parser.add_argument('--database', default='default', help='ClickHouse database')
    parser.add_argument('--partition-size', default=324000, type=int, help='Partition size for tables')
    args = parser.parse_args()
    
    # Schema files in order of execution
    schema_files = [
        'schema_part1_core.sql',
        'schema_part2_basic_views.sql',
        'schema_part3_behavior_profiles.sql',
        'schema_part4_classification.sql',
        'schema_part5_suspicious_activity.sql',
        'schema_part6_relationships_activity.sql'
    ]
    
    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database=args.database,
        settings={
            'max_execution_time': 300,
            'max_query_size': 100000
        }
    )
    
    # Execute each schema file
    success = True
    for schema_file in schema_files:
        schema_path = os.path.join(os.path.dirname(__file__), schema_file)
        if not execute_schema_file(client, schema_path, args.partition_size):
            success = False
            logger.error(f"Failed to execute {schema_file}")
            break
    
    if success:
        logger.success("All schema files executed successfully")
    else:
        logger.error("Failed to execute all schema files")
        sys.exit(1)

if __name__ == "__main__":
    main()