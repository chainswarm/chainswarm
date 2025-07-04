import os
import time

from loguru import logger


def init_tables(client, caller_file):
    """Initialize tables for balance series from schema file"""
    start_time = time.time()
    logger.info("Creating balance series tables if not exists")

    # Read schema file
    schema_path = os.path.join(os.path.dirname(caller_file), 'schema.sql')

    try:
        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        # Split by semicolon but preserve semicolons within CREATE TABLE statements
        statements = []
        current_statement = []
        lines = schema_sql.split('\n')

        for line in lines:
            line = line.strip()
            if line.startswith('--') or not line:
                continue

            current_statement.append(line)

            # Check if this line ends with a semicolon and the next non-empty line starts a new statement
            if line.endswith(';'):
                # Join the current statement and add it to statements
                full_statement = ' '.join(current_statement).strip()
                if full_statement and not full_statement.startswith('--'):
                    statements.append(full_statement.rstrip(';'))
                current_statement = []

        # Execute each statement
        for statement in statements:
            if statement:
                try:
                    client.command(statement)
                except Exception as e:
                    # Skip errors for views and indexes that might already exist
                    if "already exists" in str(e).lower():
                        logger.debug(f"Object already exists, skipping: {statement[:50]}...")
                    else:
                        logger.error(f"Error executing statement: {e}")
                        logger.error(f"Statement: {statement[:100]}...")
                        raise

        logger.info(f"Balance series table initialization completed in {time.time() - start_time:.2f}s")

    except FileNotFoundError:
        logger.error(f"Schema file not found: {schema_path}")
        raise
    except Exception as e:
        logger.error(f"Error initializing balance series tables: {e}")
        raise