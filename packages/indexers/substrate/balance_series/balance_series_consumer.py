import argparse
import traceback
import time
import os
from loguru import logger
from typing import Dict, Set, List, Tuple, Optional
from datetime import datetime, timedelta
from decimal import Decimal

from packages.indexers.base import setup_logger, get_clickhouse_connection_string, create_clickhouse_database, \
    terminate_event
from packages.indexers.substrate import get_substrate_node_url, networks, data, Network
from packages.indexers.substrate.balance_series.balance_series_indexer import BalanceSeriesIndexer
from packages.indexers.substrate.balance_series.balance_series_indexer_torus import TorusBalanceSeriesIndexer
from packages.indexers.substrate.balance_series.balance_series_indexer_bittensor import BittensorBalanceSeriesIndexer
from packages.indexers.substrate.balance_series.balance_series_indexer_polkadot import PolkadotBalanceSeriesIndexer
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager
from packages.indexers.substrate.node.substrate_node import SubstrateNode


def get_balance_series_indexer(network: str, connection_params, period_hours: int = 4):
    """
    Factory function to get the appropriate indexer based on network.
    
    Args:
        network: Network identifier (torus, bittensor, polkadot)
        connection_params: ClickHouse connection parameters
        period_hours: Number of hours in each period (default: 4)
        
    Returns:
        BalanceSeriesIndexer: Appropriate indexer instance for the network
        
    Raises:
        ValueError: If network is invalid
    """
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return TorusBalanceSeriesIndexer(connection_params, network, period_hours)
    elif network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
        return BittensorBalanceSeriesIndexer(connection_params, network, period_hours)
    elif network == Network.POLKADOT.value:
        return PolkadotBalanceSeriesIndexer(connection_params, network, period_hours)
    else:
        raise ValueError(f"Unsupported network: {network}")


class BalanceSeriesConsumer:
    def __init__(
            self,
            block_stream_manager: BlockStreamManager,
            substrate_node: SubstrateNode,
            balance_series_indexer: BalanceSeriesIndexer,
            terminate_event,
            network: str,
            period_hours: int = 4,
            batch_size: int = 100
    ):
        """Initialize the Balance Series Consumer
        
        Args:
            block_stream_manager: BlockStreamManager instance for querying block data
            substrate_node: SubstrateNode instance for querying blockchain data
            balance_series_indexer: BalanceSeriesIndexer instance for storing balance series data
            terminate_event: Event to signal termination
            network: Network identifier (e.g., 'torus', 'polkadot')
            period_hours: Number of hours in each period (default: 4)
            batch_size: Number of addresses to query in a single blockchain request
        """
        self.block_stream_manager = block_stream_manager
        self.substrate_node = substrate_node
        self.balance_series_indexer = balance_series_indexer
        self.terminate_event = terminate_event
        self.network = network
        self.period_hours = period_hours
        self.period_ms = period_hours * 60 * 60 * 1000  # Convert hours to milliseconds
        self.batch_size = batch_size

    def _get_first_block_timestamp(self) -> Optional[int]:
        """Get the timestamp of the first block in the blockchain
        
        First tries to get the timestamp from block_height = 0 (genesis block),
        then falls back to block_height = 1 if that fails.
        
        Returns:
            Timestamp in milliseconds or None if the first block can't be found
        """
        try:
            # First try to get the timestamp from the genesis block (height = 0)
            query = """
                SELECT block_timestamp
                FROM block_stream
                WHERE block_height = 0
                LIMIT 1
            """
            
            result = self.block_stream_manager.client.query(query)
            
            if result.result_rows:
                genesis_timestamp = result.result_rows[0][0]
                logger.info(f"Found genesis block (height 0) timestamp: {genesis_timestamp}")
                return genesis_timestamp
            
            # If genesis block not found, try block_height = 1
            query = """
                SELECT block_timestamp
                FROM block_stream
                WHERE block_height = 1
                LIMIT 1
            """
            
            result = self.block_stream_manager.client.query(query)
            
            if result.result_rows:
                logger.info(f"Genesis block not found, using block height 1 timestamp: {result.result_rows[0][0]}")
                return result.result_rows[0][0]
            
            # If neither block_height = 0 nor block_height = 1 is found, try to find the earliest block
            query = """
                SELECT block_timestamp
                FROM block_stream
                ORDER BY block_height ASC
                LIMIT 1
            """
            
            result = self.block_stream_manager.client.query(query)
            
            if result.result_rows:
                earliest_timestamp = result.result_rows[0][0]
                logger.warning(f"Neither genesis block nor block with height 1 found, using earliest block timestamp: {earliest_timestamp}")
                return earliest_timestamp
            
            logger.error("No blocks found in block_stream table")
            return None
            
        except Exception as e:
            logger.error(f"Error getting first block timestamp: {e}")
            return None

    def run(self):
        """Main processing loop for balance series data"""
        try:
            # Get the latest processed period
            last_processed_timestamp, last_processed_block_height = self.balance_series_indexer.get_latest_processed_period()
            
            # Get the next period to process
            next_period_start, next_period_end = self.balance_series_indexer.get_next_period_to_process()
            
            # If there's no previous period data (timestamp is 0 or very early Unix epoch),
            # initialize based on the blockchain's first block timestamp
            if next_period_start < 28800000:  # 8 hours from Unix epoch, arbitrary threshold to detect default values
                first_block_timestamp = self._get_first_block_timestamp()
                
                if first_block_timestamp:
                    # Calculate the period boundaries based on the first block timestamp
                    period_start, period_end = self.balance_series_indexer.calculate_period_boundaries(first_block_timestamp)
                    next_period_start = period_start
                    next_period_end = period_end
                    logger.info(f"Initialized period based on first block timestamp: {first_block_timestamp}")
                else:
                    logger.warning("Could not find first block timestamp, using default period")
            
            logger.info(f"Starting balance series consumer from period {next_period_start}-{next_period_end}")
            logger.info(f"Last processed period ended at timestamp {last_processed_timestamp}, block {last_processed_block_height}")
            logger.info(f"Period length: {self.period_hours} hours ({self.period_ms} ms)")
            logger.info(f"Batch size for blockchain queries: {self.batch_size}")
            
            while not self.terminate_event.is_set():
                try:
                    # Get the current time
                    current_time = int(datetime.now().timestamp() * 1000)
                    
                    # Check if the next period has already ended
                    if next_period_end <= current_time:
                        # Process the period
                        logger.info(f"Processing period {next_period_start}-{next_period_end}")
                        self._process_period(next_period_start, next_period_end)
                        
                        # Update to the next period
                        next_period_start = next_period_end
                        next_period_end = next_period_start + self.period_ms
                    else:
                        # Wait until the next period ends
                        wait_time = (next_period_end - current_time) / 1000  # Convert to seconds
                        logger.info(f"Waiting {wait_time:.2f} seconds for period {next_period_start}-{next_period_end} to end")
                        
                        # Sleep in small increments to check for termination
                        sleep_increment = 10  # seconds
                        for _ in range(int(wait_time / sleep_increment) + 1):
                            if self.terminate_event.is_set():
                                break
                            time.sleep(min(sleep_increment, wait_time))
                            wait_time -= sleep_increment
                    
                except Exception as e:
                    if self.terminate_event.is_set():
                        logger.info("Termination requested, stopping processing")
                        break
                        
                    logger.error(f"Error processing period {next_period_start}-{next_period_end}: {e}", error=e, trb=traceback.format_exc())
                    time.sleep(5)  # Brief pause before continuing
                    
            logger.info("Termination event received, shutting down")
                    
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Fatal error: {e}", error=e, trb=traceback.format_exc())
        finally:
            self._cleanup()
            logger.info("Balance series consumer stopped")

    def _process_period(self, period_start: int, period_end: int):
        """Process a single time period
        
        Args:
            period_start: Start timestamp of the period (milliseconds)
            period_end: End timestamp of the period (milliseconds)
        """
        try:
            # Find the block closest to the period end time
            end_block = self._find_block_for_timestamp(period_end)
            
            if not end_block:
                error_msg = f"No block found for period end timestamp {period_end}"
                logger.error(error_msg)
                raise ValueError(error_msg)  # Fail fast instead of returning
            
            block_height = end_block['block_height']
            block_hash = end_block['block_hash']
            block_timestamp = end_block['timestamp']
            
            logger.info(f"Found block {block_height} (hash: {block_hash}) at timestamp {block_timestamp} for period end {period_end}")
            
            # Get all active addresses during this period
            active_addresses = self._get_active_addresses_in_period(period_start, period_end)
            
            # Add addresses from previous period to ensure we track balances even if they weren't active
            previous_addresses = self._get_addresses_from_previous_period(period_start)
            all_addresses = active_addresses.union(previous_addresses)
            
            logger.info(f"Processing {len(all_addresses)} addresses for period {period_start}-{period_end}")
            
            if not all_addresses:
                error_msg = f"No addresses found for period {period_start}-{period_end}"
                logger.error(error_msg)
                raise ValueError(error_msg)  # Fail fast instead of continuing
            
            # Query balances for all addresses at the end block
            address_balances = self._query_blockchain_balances(all_addresses, block_hash)
            
            # Record balance series data
            self.balance_series_indexer.record_balance_series(
                period_start, period_end, block_height, block_hash, address_balances
            )
            
            # Update processing state
            self.balance_series_indexer.update_processing_state(period_end, block_height, period_end)
            
            logger.success(f"Successfully processed period {period_start}-{period_end} with {len(address_balances)} addresses")
            
        except Exception as e:
            logger.error(f"Error processing period {period_start}-{period_end}: {e}")
            raise

    def _find_block_for_timestamp(self, timestamp: int) -> Optional[Dict]:
        """Find the block closest to the given timestamp
        
        Args:
            timestamp: Timestamp in milliseconds
            
        Returns:
            Block information or None if no block found
        """
        try:
            # Query the block_stream table to find the block closest to the timestamp
            query = f"""
                SELECT block_height, block_hash, block_timestamp
                FROM block_stream
                WHERE block_timestamp <= {timestamp}
                ORDER BY block_timestamp DESC
                LIMIT 1
            """
            
            result = self.block_stream_manager.client.query(query)
            
            if result.result_rows:
                row = result.result_rows[0]
                return {
                    'block_height': row[0],
                    'block_hash': row[1],
                    'timestamp': row[2]  # Keep the key as 'timestamp' for backward compatibility
                }
            
            # If no block found with timestamp <= target, try to find the earliest block
            query = f"""
                SELECT block_height, block_hash, block_timestamp
                FROM block_stream
                ORDER BY block_timestamp ASC
                LIMIT 1
            """
            
            result = self.block_stream_manager.client.query(query)
            
            if result.result_rows:
                row = result.result_rows[0]
                logger.warning(f"No block found before timestamp {timestamp}, using earliest block {row[0]}")
                return {
                    'block_height': row[0],
                    'block_hash': row[1],
                    'timestamp': row[2]  # Keep the key as 'timestamp' for backward compatibility
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding block for timestamp {timestamp}: {e}")
            raise  # Fail fast instead of returning None

    def _get_active_addresses_in_period(self, period_start: int, period_end: int) -> Set[str]:
        """Get all addresses that were active during the given period
        
        Args:
            period_start: Start timestamp of the period (milliseconds)
            period_end: End timestamp of the period (milliseconds)
            
        Returns:
            Set of addresses
        """
        try:
            # Query the block_stream table to find all addresses involved in transactions during the period
            query = f"""
                SELECT DISTINCT arrayJoin(addresses) as address
                FROM block_stream
                WHERE block_timestamp >= {period_start} AND block_timestamp < {period_end}
            """
            
            result = self.block_stream_manager.client.query(query)
            addresses = {row[0] for row in result.result_rows}
            
            logger.info(f"Found {len(addresses)} active addresses in period {period_start}-{period_end}")
            return addresses
            
        except Exception as e:
            logger.error(f"Error getting active addresses in period {period_start}-{period_end}: {e}")
            raise  # Fail fast instead of returning empty set

    def _get_addresses_from_previous_period(self, current_period_start: int) -> Set[str]:
        """Get all addresses from the previous period
        
        Args:
            current_period_start: Start timestamp of the current period (milliseconds)
            
        Returns:
            Set of addresses
        """
        try:
            # Query the balance_series table to find all addresses from the previous period
            query = f"""
                SELECT DISTINCT address
                FROM balance_series
                WHERE period_start_timestamp < {current_period_start}
                  AND asset = '{self.balance_series_indexer.asset}'
                ORDER BY period_start_timestamp DESC
                LIMIT 10000  -- Limit to prevent memory issues with very large datasets
            """
            
            result = self.balance_series_indexer.client.query(query)
            addresses = {row[0] for row in result.result_rows}
            
            logger.info(f"Found {len(addresses)} addresses from previous periods")
            return addresses
            
        except Exception as e:
            logger.error(f"Error getting addresses from previous period: {e}")
            raise  # Fail fast instead of returning empty set

    def _query_blockchain_balances(self, addresses: Set[str], block_hash: str) -> Dict[str, Dict[str, int]]:
        """Query balances for multiple addresses from the blockchain
        
        Args:
            addresses: Set of addresses to query
            block_hash: Block hash to query at
            
        Returns:
            Dictionary mapping addresses to their balance information
        """
        result = {}
        address_list = list(addresses)
        
        for i in range(0, len(address_list), self.batch_size):
            # Check for termination before processing batch
            if self.terminate_event.is_set():
                logger.info("Termination requested during balance query")
                break
                
            batch = address_list[i:i + self.batch_size]
            logger.info(f"Querying balances for {len(batch)} addresses (batch {i//self.batch_size + 1}/{(len(address_list) + self.batch_size - 1)//self.batch_size})")
            
            for address in batch:
                # The get_balances_at_block method now has infinite retry built-in
                # It will only return if successful or if termination is requested
                try:
                    account_data = self.substrate_node.get_balances_at_block(
                        block_hash=block_hash,
                        params=[address]
                    )
                    
                    if account_data:
                        free = int(account_data.get('data', {}).get('free', 0))
                        reserved = int(account_data.get('data', {}).get('reserved', 0))
                        staked = int(account_data.get('data', {}).get('staked', 0))
                        total = free + reserved + staked
                        
                        result[address] = {
                            'free_balance': free,
                            'reserved_balance': reserved,
                            'staked_balance': staked,
                            'total_balance': total
                        }
                except Exception as e:
                    # This should only happen if termination was requested
                    if self.terminate_event.is_set():
                        logger.info(f"Termination requested during balance query for {address}")
                        break
                    else:
                        # This shouldn't happen with infinite retry, but fail fast if it does
                        logger.error(f"Unexpected error querying balance for {address}: {e}")
                        raise  # Fail fast instead of continuing
            
            # Brief pause between batches
            if i + self.batch_size < len(address_list) and not self.terminate_event.is_set():
                time.sleep(0.1)
        
        logger.info(f"Successfully queried balances for {len(result)} addresses")
        return result

    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'balance_series_indexer'):
                self.balance_series_indexer.close()
                logger.info("Closed balance series indexer connection")
        except Exception as e:
            logger.error(f"Error closing balance series indexer: {e}")
        
        try:
            if hasattr(self, 'block_stream_manager'):
                self.block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Balance Series Consumer')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to track balances for (polkadot, torus, or bittensor)'
    )
    parser.add_argument(
        '--period-hours',
        type=int,
        default=4,
        help='Number of hours in each period (default: 4)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of addresses to query in a single blockchain request'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-balance-series'
    setup_logger(service_name)

    # Initialize components
    balance_series_indexer = None
    block_stream_manager = None
    substrate_node = None

    try:
        clickhouse_params = get_clickhouse_connection_string(args.network)
        create_clickhouse_database(clickhouse_params)

        balance_series_indexer = get_balance_series_indexer(args.network, clickhouse_params, args.period_hours)
        block_stream_manager = BlockStreamManager(clickhouse_params, args.network, terminate_event)
        
        node_url = get_substrate_node_url(args.network)
        substrate_node = SubstrateNode(args.network, node_url, terminate_event)

        consumer = BalanceSeriesConsumer(
            block_stream_manager,
            substrate_node,
            balance_series_indexer,
            terminate_event,
            args.network,
            args.period_hours,
            args.batch_size
        )

        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error", error=e, trb=traceback.format_exc())
    finally:
        try:
            if balance_series_indexer:
                balance_series_indexer.close()
                logger.info("Closed balance series indexer connection")
        except Exception as e:
            logger.error(f"Error closing balance series indexer: {e}")

        try:
            if block_stream_manager:
                block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")

        logger.info("Balance series consumer stopped")