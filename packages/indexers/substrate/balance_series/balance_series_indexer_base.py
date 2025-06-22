import uuid
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional, List
import clickhouse_connect
from decimal import Decimal
from loguru import logger

from packages.indexers.substrate.block_range_partitioner import BlockRangePartitioner
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import get_network_asset


class BalanceSeriesIndexerBase:
    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """Initialize the Balance Series Indexer with a database connection
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
            period_hours: Number of hours in each period (default: 4)
        """
        self.network = network
        self.asset = get_network_asset(network)
        self.period_hours = period_hours
        self.period_ms = period_hours * 60 * 60 * 1000  # Convert hours to milliseconds
        self.first_block_timestamp = None  # Will be set by the consumer if available
        
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
        
        self._init_tables()
        self.version = int(datetime.now().strftime('%Y%m%d%H%M%S'))

    def _init_tables(self):
        """Initialize tables for balance series from schema file"""
        # Read schema file
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        
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
                        self.client.command(statement)
                    except Exception as e:
                        # Skip errors for views and indexes that might already exist
                        if "already exists" in str(e).lower():
                            logger.debug(f"Object already exists, skipping: {statement[:50]}...")
                        else:
                            logger.error(f"Error executing statement: {e}")
                            logger.error(f"Statement: {statement[:100]}...")
                            raise
            
            logger.info("Balance series tables initialized from schema.sql")
            
        except FileNotFoundError:
            logger.error(f"Schema file not found: {schema_path}")
            raise
        except Exception as e:
            logger.error(f"Error initializing balance series tables: {e}")
            raise

    def record_balance_series(self, period_start_timestamp: int, period_end_timestamp: int, 
                             block_height: int, block_hash: str, address_balances: Dict[str, Dict[str, int]]):
        """Record balance series data for multiple addresses at a specific time period
        
        Args:
            period_start_timestamp: Start timestamp of the period (milliseconds)
            period_end_timestamp: End timestamp of the period (milliseconds)
            block_height: Block height at the end of the period
            block_hash: Block hash at the end of the period
            address_balances: Dictionary mapping addresses to their balance information
                             {address: {'free_balance': int, 'reserved_balance': int, 
                                       'staked_balance': int, 'total_balance': int}}
        """
        if not address_balances:
            logger.warning(f"No address balances provided for period {period_start_timestamp}-{period_end_timestamp}")
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
                    raise ValueError(f"Negative balance detected for {address} at period ending {period_end_timestamp}")

                expected_total = free_balance + reserved_balance + staked_balance
                if total_balance != expected_total:
                    logger.warning(f"Total balance mismatch for {address} at period ending {period_end_timestamp}: "
                                  f"{total_balance} != {expected_total}, correcting")
                    total_balance = expected_total
                
                # Get previous period balances for calculating changes
                prev_balances, prev_period = self.get_previous_period_balances(address, period_start_timestamp)
                
                # Calculate changes from previous period
                free_balance_change = Decimal(0)
                reserved_balance_change = Decimal(0)
                staked_balance_change = Decimal(0)
                total_balance_change = Decimal(0)
                total_balance_percent_change = Decimal(0)
                
                if prev_balances:
                    free_balance_change = free_balance - prev_balances.get('free_balance', Decimal(0))
                    reserved_balance_change = reserved_balance - prev_balances.get('reserved_balance', Decimal(0))
                    staked_balance_change = staked_balance - prev_balances.get('staked_balance', Decimal(0))
                    total_balance_change = total_balance - prev_balances.get('total_balance', Decimal(0))
                    
                    # Calculate percentage change
                    prev_total = prev_balances.get('total_balance', Decimal(0))
                    if prev_total > 0:
                        total_balance_percent_change = (total_balance_change / prev_total) * 100
                
                balance_data.append((
                    period_start_timestamp,
                    period_end_timestamp,
                    block_height,
                    block_hash,
                    address,
                    self.asset,
                    free_balance,
                    reserved_balance,
                    staked_balance,
                    total_balance,
                    free_balance_change,
                    reserved_balance_change,
                    staked_balance_change,
                    total_balance_change,
                    total_balance_percent_change,
                    self.version
                ))
            
            # Insert data
            if balance_data:
                self.client.insert('balance_series', balance_data, column_names=[
                    'period_start_timestamp', 'period_end_timestamp', 'block_height', 'block_hash',
                    'address', 'asset', 'free_balance', 'reserved_balance', 'staked_balance', 'total_balance',
                    'free_balance_change', 'reserved_balance_change', 'staked_balance_change', 'total_balance_change',
                    'total_balance_percent_change', '_version'
                ])

        except Exception as e:
            logger.error(f"Error recording balance series for period {period_start_timestamp}-{period_end_timestamp}: {e}")
            raise

    def get_previous_period_balances(self, address: str, current_period_start: int) -> Tuple[Optional[Dict[str, Decimal]], int]:
        """Get the previous period's balances for an address
        
        Args:
            address: The address to query
            current_period_start: The start timestamp of the current period
            
        Returns:
            Tuple of (balance_dict, period_start_timestamp) or (None, 0) if no previous balance found
        """
        try:
            result = self.client.query(f'''
                SELECT
                    period_start_timestamp,
                    free_balance,
                    reserved_balance,
                    staked_balance,
                    total_balance
                FROM balance_series
                WHERE address = '{address}' 
                  AND asset = '{self.asset}' 
                  AND period_start_timestamp < {current_period_start}
                ORDER BY period_start_timestamp DESC
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
            logger.error(f"Error getting previous period balances for {address}: {e}")
            return None, 0

    def get_latest_processed_period(self) -> Tuple[int, int]:
        """Get the latest period for which balance series have been recorded
        
        Returns:
            Tuple of (last_processed_timestamp, last_processed_block_height) or (0, 0) if no records exist
        """
        try:
            # Query the balance_series table for the latest period
            result = self.client.query(f'''
                SELECT period_end_timestamp, block_height
                FROM balance_series
                WHERE asset = '{self.asset}'
                ORDER BY period_end_timestamp DESC
                LIMIT 1
            ''')
            
            if result.result_rows and result.result_rows[0][0] is not None:
                return result.result_rows[0][0], result.result_rows[0][1]
            
            return 0, 0
        except Exception as e:
            logger.error(f"Error getting latest processed period: {e}")
            raise e

    # The update_processing_state method has been removed as we now track state
    # by querying the last record from the balance_series table directly

    def get_next_period_to_process(self) -> Tuple[int, int]:
        """Get the next period to process
        
        Returns:
            Tuple of (next_period_start, next_period_end) timestamps in milliseconds
        """
        try:
            # Get the latest processed period
            last_processed_timestamp, _ = self.get_latest_processed_period()
            
            if last_processed_timestamp > 0:
                # The next period starts at the end of the last processed period
                next_period_start = last_processed_timestamp
                next_period_end = next_period_start + self.period_ms
                return next_period_start, next_period_end
            
            # If no processed periods found, return Unix epoch start
            # The actual initialization will be handled by the consumer
            # which will query the blockchain's first block timestamp
            epoch_start = 0  # Unix epoch in milliseconds
            next_period_end = epoch_start + self.period_ms
            
            return epoch_start, next_period_end
            
        except Exception as e:
            logger.error(f"Error getting next period to process: {e}")
            # Return Unix epoch start if there's an error
            # The actual initialization will be handled by the consumer
            epoch_start = 0  # Unix epoch in milliseconds
            next_period_end = epoch_start + self.period_ms
            return epoch_start, next_period_end

    def calculate_period_boundaries(self, timestamp_ms: int) -> Tuple[int, int]:
        """Calculate the period boundaries for a given timestamp
        
        Args:
            timestamp_ms: Timestamp in milliseconds
            
        Returns:
            Tuple of (period_start_timestamp, period_end_timestamp) in milliseconds
        """
        # Calculate how many periods have passed since epoch
        periods_since_epoch = timestamp_ms // self.period_ms
        
        # Calculate period boundaries
        period_start = periods_since_epoch * self.period_ms
        period_end = period_start + self.period_ms
        
        return period_start, period_end

    def init_genesis_balances(self, block_info):
        """Insert genesis balances into the balance_series table
        
        This is a template method that must be implemented by network-specific indexers.
        Each network should implement its own logic for inserting genesis balances.
        
        Args:
            genesis_balances: List of (address, amount) tuples
            network: Network identifier (e.g., 'torus')
            block_height: Height of the first block (default: 0)
            block_hash: Hash of the first block (default: None, will use 'genesis')
            block_timestamp: Timestamp of the first block in milliseconds (default: None)
                            If provided, use this as the period_start_timestamp
                            instead of 0 (Unix epoch)
        """
        raise NotImplementedError("init_genesis_balances must be implemented by subclasses")
    
    def close(self):
        """Close the ClickHouse connection"""
        if hasattr(self, 'client'):
            self.client.close()
