import uuid
import os
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List
import clickhouse_connect
from decimal import Decimal
from loguru import logger

from packages.indexers.substrate.block_range_partitioner import BlockRangePartitioner
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import get_network_asset


class BalanceTrackingIndexerBase:
    def __init__(self, connection_params: Dict[str, Any], partitioner: BlockRangePartitioner, network: str):
        """Initialize the Balance Tracking Indexer with a database connection
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
        """
        self.network = network
        self.asset = get_network_asset(network)
        self.client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database'],
            settings={
                'async_insert': 0,
                'wait_for_async_insert': 1,
                'max_execution_time': 300,
                'max_query_size': 100000
            }
        )
        self.partitioner = partitioner
        self._init_tables()
        self.version = int(datetime.now().strftime('%Y%m%d%H%M%S'))

    def _init_tables(self):
        """Initialize tables for balance tracking from schema file"""
        # Read schema file
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            # Replace partition size placeholder
            schema_sql = schema_sql.replace('{PARTITION_SIZE}', str(self.partitioner.range_size))
            
            # Split by semicolon but preserve semicolons within CREATE TABLE statements
            # by not splitting on semicolons that are followed by whitespace and then CREATE or ALTER
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
                        self.client.command(statement)
                    except Exception as e:
                        # Skip errors for views and indexes that might already exist
                        if "already exists" in str(e).lower():
                            logger.debug(f"Object already exists, skipping: {statement[:50]}...")
                        else:
                            logger.error(f"Error executing statement: {e}")
                            logger.error(f"Statement: {statement[:100]}...")
                            raise
            
            logger.info("Balance tracking tables initialized from schema.sql")
            
        except FileNotFoundError:
            logger.error(f"Schema file not found: {schema_path}")
            raise
        except Exception as e:
            logger.error(f"Error initializing balance tracking tables: {e}")
            raise

    def record_balance_change(self, block_height: int, block_timestamp: int, address_balances: Dict[str, Dict[str, int]]):
        """Record balance changes for multiple addresses
        
        Args:
            block_height: The block height for this record
            block_timestamp: The block timestamp
            address_balances: Dictionary mapping addresses to their balance information
                             {address: {'free_balance': int, 'reserved_balance': int, 
                                       'staked_balance': int, 'total_balance': int}}
        """
        if not address_balances:
            logger.warning(f"No address balances provided for block {block_height}")
            return

        try:
            # Prepare data for insertion
            balance_data = []
            for address, balances in address_balances.items():
                # Convert raw blockchain values to decimal units
                free_balance = convert_to_decimal_units(balances.get('free_balance', 0), self.network)
                reserved_balance = convert_to_decimal_units(balances.get('reserved_balance', 0), self.network)
                staked_balance = convert_to_decimal_units(balances.get('staked_balance', 0), self.network)
                total_balance = convert_to_decimal_units(balances.get('total_balance', 0), self.network)
                
                # Validate balances
                if free_balance < 0 or reserved_balance < 0 or staked_balance < 0 or total_balance < 0:
                    raise ValueError(f"Negative balance detected for {address} at block {block_height}")

                expected_total = free_balance + reserved_balance + staked_balance
                if total_balance != expected_total:
                    logger.warning(f"Total balance mismatch for {address} at block {block_height}: "
                                  f"{total_balance} != {expected_total}, correcting")
                    total_balance = expected_total

                balance_data.append((
                    block_height,
                    block_timestamp,
                    address,
                    self.asset,
                    free_balance,
                    reserved_balance,
                    staked_balance,
                    total_balance,
                    self.version
                ))
            
            # Insert data
            if balance_data:
                self.client.insert('balance_changes', balance_data, column_names=[
                    'block_height', 'block_timestamp', 'address', 'asset', 'free_balance',
                    'reserved_balance', 'staked_balance', 'total_balance', '_version'
                ])
                logger.success(f"Recorded balance changes at block {block_height} for {len(balance_data)} addresses")

        except Exception as e:
            logger.error(f"Error recording balance changes at block {block_height}: {e}")
            raise

    def calculate_and_record_deltas(self, block_height: int, block_timestamp: int, address_balances: Dict[str, Dict[str, int]]):
        """Calculate and record balance deltas for multiple addresses
        
        Args:
            block_height: The block height for this record
            block_timestamp: The block timestamp
            address_balances: Dictionary mapping addresses to their balance information
        """
        if not address_balances:
            logger.warning(f"No address balances provided for delta calculation at block {block_height}")
            return

        try:
            delta_data = []
            
            for address, current_balances in address_balances.items():
                # Get previous balance
                previous_balances, previous_block_height = self.get_previous_balance(address, block_height)
                
                if previous_balances is None:
                    # No previous balance found, skip delta calculation
                    continue
                
                # Calculate deltas using decimal units
                free_balance_delta = convert_to_decimal_units(current_balances.get('free_balance', 0), self.network) - previous_balances.get('free_balance', Decimal(0))
                reserved_balance_delta = convert_to_decimal_units(current_balances.get('reserved_balance', 0), self.network) - previous_balances.get('reserved_balance', Decimal(0))
                staked_balance_delta = convert_to_decimal_units(current_balances.get('staked_balance', 0), self.network) - previous_balances.get('staked_balance', Decimal(0))
                total_balance_delta = convert_to_decimal_units(current_balances.get('total_balance', 0), self.network) - previous_balances.get('total_balance', Decimal(0))
                
                # Only record if there's a change
                if (free_balance_delta != 0 or reserved_balance_delta != 0 or 
                    staked_balance_delta != 0 or total_balance_delta != 0):
                    delta_data.append((
                        block_height,
                        block_timestamp,
                        address,
                        self.asset,
                        free_balance_delta,
                        reserved_balance_delta,
                        staked_balance_delta,
                        total_balance_delta,
                        previous_block_height,
                        self.version
                    ))
            
            # Insert data
            if delta_data:
                self.client.insert('balance_delta_changes', delta_data, column_names=[
                    'block_height', 'block_timestamp', 'address', 'asset', 'free_balance_delta',
                    'reserved_balance_delta', 'staked_balance_delta', 'total_balance_delta',
                    'previous_block_height', '_version'
                ])
                logger.success(f"Recorded balance deltas at block {block_height} for {len(delta_data)} addresses")

        except Exception as e:
            logger.error(f"Error calculating and recording balance deltas at block {block_height}: {e}")
            raise

    def get_previous_balance(self, address: str, current_block_height: int) -> Tuple[Optional[Dict[str, Decimal]], int]:
        """Get the most recent previous balance for an address
        
        Args:
            address: The address to query
            current_block_height: The current block height
            
        Returns:
            Tuple of (balance_dict, block_height) or (None, 0) if no previous balance found
        """
        try:
            result = self.client.query(f'''
                SELECT
                    block_height,
                    free_balance,
                    reserved_balance,
                    staked_balance,
                    total_balance
                FROM balance_changes
                WHERE address = '{address}' AND asset = '{self.asset}' AND block_height < {current_block_height}
                ORDER BY block_height DESC
                LIMIT 1
            ''')
            
            if result.result_rows:
                row = result.result_rows[0]
                return {
                    'free_balance': row[1],
                    'reserved_balance': row[2],
                    'staked_balance': row[3],
                    'total_balance': row[4]
                }, row[0]
            
            return None, 0
            
        except Exception as e:
            logger.error(f"Error getting previous balance for {address}: {e}")
            return None, 0

    def get_latest_processed_block_height(self) -> int:
        """Get the latest block height for which balance changes have been recorded
        
        Returns:
            The maximum block height across balance_changes and balance_delta_changes tables, or 0 if no records exist
        """
        try:
            result = self.client.query('''
                SELECT MIN(max_height) FROM (
                    SELECT MAX(block_height) as max_height FROM balance_changes
                    UNION ALL
                    SELECT MAX(block_height) as max_height FROM balance_delta_changes
                )
            ''')
            
            if result.result_rows and result.result_rows[0][0] is not None:
                return result.result_rows[0][0]
            return 0
        except Exception as e:
            logger.error(f"Error getting latest processed block height: {e}")
            return 0
    
    def insert_genesis_balances(self, genesis_balances, network):
        """Insert genesis balances into the balance_changes table only (not transfer table)
        
        Args:
            genesis_balances: List of (address, amount) tuples
            network: Network identifier (e.g., 'torus')
        """
        if not genesis_balances:
            logger.warning("No genesis balances provided")
            return
            
        try:
            # Check if genesis balances already exist in balance_changes
            result = self.client.query(f"""
                SELECT COUNT(*) FROM balance_changes
                WHERE block_height = 0 AND asset = '{self.asset}'
            """)
            
            if result.result_rows and result.result_rows[0][0] > 0:
                logger.info(f"Genesis balance records already exist for {network} - skipping insertion")
                return
                
            # Current timestamp for all genesis records
            timestamp = int(datetime.now().timestamp()) * 1000  # Convert to milliseconds
            
            # Prepare data for balance_changes insertion
            balance_data = []
            for address, amount in genesis_balances:
                # Convert genesis balances to decimal units
                free_balance = convert_to_decimal_units(amount, network)
                reserved_balance = Decimal(0)
                staked_balance = Decimal(0)
                total_balance = free_balance + reserved_balance + staked_balance
                
                balance_data.append((
                    0,  # block_height = 0 for genesis
                    timestamp,
                    address,
                    self.asset,
                    free_balance,
                    reserved_balance,
                    staked_balance,
                    total_balance,
                    self.version
                ))
            
            # Insert data in batches to avoid memory issues
            batch_size = 1000
            for i in range(0, len(balance_data), batch_size):
                batch = balance_data[i:i + batch_size]
                self.client.insert('balance_changes', batch, column_names=[
                    'block_height', 'block_timestamp', 'address', 'asset', 'free_balance',
                    'reserved_balance', 'staked_balance', 'total_balance', '_version'
                ])
                logger.info(f"Inserted batch {i//batch_size + 1}/{(len(balance_data) + batch_size - 1)//batch_size} of genesis balance records")
            
            logger.success(f"Successfully inserted {len(balance_data)} genesis balance records for {network}")
            
        except Exception as e:
            logger.error(f"Error inserting genesis records: {e}")
            raise
    
    
    def close(self):
        """Close the ClickHouse connection"""
        if hasattr(self, 'client'):
            self.client.close()