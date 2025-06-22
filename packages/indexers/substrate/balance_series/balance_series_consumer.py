import argparse
import traceback
import time
from loguru import logger
from typing import Dict, Set, Optional, Any
from datetime import datetime

from packages.indexers.base import setup_logger, get_clickhouse_connection_string, create_clickhouse_database, \
    terminate_event
from packages.indexers.substrate import get_substrate_node_url, networks, data, Network
from packages.indexers.substrate.balance_series.balance_series_indexer_base import BalanceSeriesIndexerBase
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
            balance_series_indexer: BalanceSeriesIndexerBase,
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

    def run(self):
        """Main processing loop for balance series data"""
        try:

            block_info = self.block_stream_manager.get_blocks_by_block_height_range(1, 1)
            if not block_info or len(block_info) == 0:
                raise ValueError("No block information found for height 1, cannot initialize balance series consumer")

            self.balance_series_indexer.init_genesis_balances(block_info[0])

            # Get the latest processed period
            last_processed_timestamp, last_processed_block_height = self.balance_series_indexer.get_latest_processed_period()
            next_period_start, next_period_end = self.balance_series_indexer.get_next_period_to_process()

            logger.info(f"Starting balance series consumer from period {next_period_start}-{next_period_end} at block height {last_processed_block_height}")


            while not self.terminate_event.is_set():
                try:
                    current_time = int(datetime.now().timestamp() * 1000)

                    if next_period_end <= current_time:
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
            end_block = self.block_stream_manager.get_block_by_nearest_timestamp(period_end)

            if not end_block:
                error_msg = f"No block found for period end timestamp {period_end}"
                logger.error(error_msg)
                raise ValueError(error_msg)  # Fail fast instead of returning

            block_height = end_block['block_height']
            block_hash = end_block['block_hash']
            block_timestamp = end_block['timestamp']

            logger.info(f"Found block {block_height} (hash: {block_hash}) at timestamp {block_timestamp} for period end {period_end}")

            # Get all active addresses during this period
            active_addresses = self.block_stream_manager.get_blocks_by_block_timestamp_range(period_start, period_end, only_with_addresses=True)
            if not active_addresses:
                logger.info(f"No active addresses found for period {period_start}-{period_end}")
                return

            all_addresses = set()
            for block in active_addresses:
                all_addresses.update(block['addresses'])

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

            logger.success(f"Successfully processed period {period_start}-{period_end} with {len(address_balances)} addresses and block {block_height}")

        except Exception as e:
            logger.error(f"Error processing period {period_start}-{period_end}: {e}")
            raise

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
