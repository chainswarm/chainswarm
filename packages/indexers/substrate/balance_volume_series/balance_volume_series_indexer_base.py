import uuid
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional, List, Set
import clickhouse_connect
from decimal import Decimal
from loguru import logger

from packages.indexers.substrate.block_range_partitioner import BlockRangePartitioner
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import get_network_asset


class BalanceVolumeSeriesIndexerBase:
    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """Initialize the Balance Volume Series Indexer with a database connection
        
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
        """Initialize tables for balance volume series from schema file"""
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
            
            logger.info("Balance volume series tables initialized from schema.sql")
            
        except FileNotFoundError:
            logger.error(f"Schema file not found: {schema_path}")
            raise
        except Exception as e:
            logger.error(f"Error initializing balance volume series tables: {e}")
            raise

    def record_volume_series(self, period_start_timestamp: int, period_end_timestamp: int, 
                            block_height: int, block_hash: str, volume_data: Dict[str, Any]):
        """Record volume series data for a specific time period
        
        Args:
            period_start_timestamp: Start timestamp of the period (milliseconds)
            period_end_timestamp: End timestamp of the period (milliseconds)
            block_height: Block height at the end of the period
            block_hash: Block hash at the end of the period
            volume_data: Dictionary containing volume metrics for the period
        """
        if not volume_data:
            logger.warning(f"No volume data provided for period {period_start_timestamp}-{period_end_timestamp}")
            return

        try:
            # Convert raw blockchain values to decimal units where appropriate
            total_volume = convert_to_decimal_units(volume_data.get('total_volume', 0), self.network)
            total_fees = convert_to_decimal_units(volume_data.get('total_fees', 0), self.network)
            avg_fee = convert_to_decimal_units(volume_data.get('avg_fee', 0), self.network)
            
            # Transaction size categories
            micro_tx_volume = convert_to_decimal_units(volume_data.get('micro_tx_volume', 0), self.network)
            small_tx_volume = convert_to_decimal_units(volume_data.get('small_tx_volume', 0), self.network)
            medium_tx_volume = convert_to_decimal_units(volume_data.get('medium_tx_volume', 0), self.network)
            large_tx_volume = convert_to_decimal_units(volume_data.get('large_tx_volume', 0), self.network)
            whale_tx_volume = convert_to_decimal_units(volume_data.get('whale_tx_volume', 0), self.network)
            
            # Validate volume data
            if total_volume < 0 or total_fees < 0:
                raise ValueError(f"Negative volume or fees detected for period ending {period_end_timestamp}")
            
            # Get previous period data for calculating changes
            prev_volume_data, prev_period = self.get_previous_period_volume_data(period_start_timestamp)
            
            # Calculate changes from previous period
            volume_change = Decimal(0)
            volume_percent_change = Decimal(0)
            tx_count_change = 0
            tx_count_percent_change = Decimal(0)
            
            if prev_volume_data:
                prev_total_volume = prev_volume_data.get('total_volume', Decimal(0))
                prev_tx_count = prev_volume_data.get('transaction_count', 0)
                
                volume_change = total_volume - prev_total_volume
                tx_count_change = volume_data.get('transaction_count', 0) - prev_tx_count
                
                # Calculate percentage changes
                if prev_total_volume > 0:
                    volume_percent_change = (volume_change / prev_total_volume) * 100
                
                if prev_tx_count > 0:
                    tx_count_percent_change = (tx_count_change / prev_tx_count) * 100
            
            # Prepare data for insertion
            series_data = [(
                period_start_timestamp,
                period_end_timestamp,
                block_height,
                block_hash,
                self.network,
                self.asset,
                total_volume,
                volume_data.get('transaction_count', 0),
                volume_data.get('unique_senders', 0),
                volume_data.get('unique_receivers', 0),
                volume_data.get('micro_tx_count', 0),
                micro_tx_volume,
                volume_data.get('small_tx_count', 0),
                small_tx_volume,
                volume_data.get('medium_tx_count', 0),
                medium_tx_volume,
                volume_data.get('large_tx_count', 0),
                large_tx_volume,
                volume_data.get('whale_tx_count', 0),
                whale_tx_volume,
                total_fees,
                avg_fee,
                volume_data.get('active_addresses', 0),
                volume_data.get('network_density', 0.0),
                volume_change,
                volume_percent_change,
                tx_count_change,
                tx_count_percent_change,
                self.version
            )]
            
            # Insert data
            self.client.insert('balance_volume_series', series_data, column_names=[
                'period_start_timestamp', 'period_end_timestamp', 'block_height', 'block_hash',
                'network', 'asset', 'total_volume', 'transaction_count', 'unique_senders', 'unique_receivers',
                'micro_tx_count', 'micro_tx_volume', 'small_tx_count', 'small_tx_volume', 
                'medium_tx_count', 'medium_tx_volume', 'large_tx_count', 'large_tx_volume', 
                'whale_tx_count', 'whale_tx_volume', 'total_fees', 'avg_fee', 
                'active_addresses', 'network_density', 'volume_change', 'volume_percent_change',
                'tx_count_change', 'tx_count_percent_change', '_version'
            ])

            logger.success(f"Recorded volume series data for period {period_start_timestamp}-{period_end_timestamp}")

        except Exception as e:
            logger.error(f"Error recording volume series for period {period_start_timestamp}-{period_end_timestamp}: {e}")
            raise

    def get_previous_period_volume_data(self, current_period_start: int) -> Tuple[Optional[Dict[str, Any]], int]:
        """Get the previous period's volume data
        
        Args:
            current_period_start: The start timestamp of the current period
            
        Returns:
            Tuple of (volume_data_dict, period_start_timestamp) or (None, 0) if no previous data found
        """
        try:
            result = self.client.query(f'''
                SELECT
                    period_start_timestamp,
                    total_volume,
                    transaction_count,
                    unique_senders,
                    unique_receivers,
                    active_addresses,
                    network_density
                FROM balance_volume_series
                WHERE network = '{self.network}' 
                  AND asset = '{self.asset}' 
                  AND period_start_timestamp < {current_period_start}
                ORDER BY period_start_timestamp DESC
                LIMIT 1
            ''')
            
            if result.result_rows:
                row = result.result_rows[0]
                return {
                    'total_volume': row[1],
                    'transaction_count': row[2],
                    'unique_senders': row[3],
                    'unique_receivers': row[4],
                    'active_addresses': row[5],
                    'network_density': row[6]
                }, row[0]
            
            return None, 0
            
        except Exception as e:
            logger.error(f"Error getting previous period volume data: {e}")
            return None, 0

    def get_latest_processed_period(self) -> Tuple[int, int]:
        """Get the latest period for which volume series have been recorded
        
        Returns:
            Tuple of (last_processed_timestamp, last_processed_block_height) or (0, 0) if no records exist
        """
        try:
            # Query the balance_volume_series table for the latest period
            result = self.client.query(f'''
                SELECT period_end_timestamp, block_height
                FROM balance_volume_series
                WHERE network = '{self.network}'
                  AND asset = '{self.asset}'
                ORDER BY period_end_timestamp DESC
                LIMIT 1
            ''')
            
            if result.result_rows and result.result_rows[0][0] is not None:
                return result.result_rows[0][0], result.result_rows[0][1]
            
            return 0, 0
        except Exception as e:
            logger.error(f"Error getting latest processed period: {e}")
            raise e

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
            epoch_start = 0  # Unix epoch in milliseconds
            next_period_end = epoch_start + self.period_ms
            
            return epoch_start, next_period_end
            
        except Exception as e:
            logger.error(f"Error getting next period to process: {e}")
            # Return Unix epoch start if there's an error
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
    
    def categorize_transaction_size(self, volume: Decimal) -> str:
        """Categorize a transaction based on its volume
        
        Args:
            volume: Transaction volume in tokens
            
        Returns:
            Category string: 'micro', 'small', 'medium', 'large', 'whale'
        """
        if volume < Decimal('0.1'):
            return 'micro'
        elif volume < Decimal('10'):
            return 'small'
        elif volume < Decimal('100'):
            return 'medium'
        elif volume < Decimal('1000'):
            return 'large'
        else:
            return 'whale'

    def close(self):
        """Close the ClickHouse connection"""
        if hasattr(self, 'client'):
            self.client.close()