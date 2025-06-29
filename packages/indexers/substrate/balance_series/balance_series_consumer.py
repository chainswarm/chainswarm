import argparse
import traceback
import time
from loguru import logger
from typing import Dict, Set
from datetime import datetime

from packages.indexers.base import (
    get_clickhouse_connection_string, create_clickhouse_database, terminate_event,
    setup_enhanced_logger, ErrorContextManager, log_service_start, log_service_stop, classify_error
)

from packages.indexers.base.metrics import setup_metrics, IndexerMetrics
from packages.indexers.substrate import get_substrate_node_url, networks,  Network
from packages.indexers.substrate.balance_series.balance_series_indexer_base import BalanceSeriesIndexerBase
from packages.indexers.substrate.balance_series.balance_series_indexer_torus import TorusBalanceSeriesIndexer
from packages.indexers.substrate.balance_series.balance_series_indexer_bittensor import BittensorBalanceSeriesIndexer
from packages.indexers.substrate.balance_series.balance_series_indexer_polkadot import PolkadotBalanceSeriesIndexer
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager
from packages.indexers.substrate.node.substrate_node import SubstrateNode


def get_balance_series_indexer(network: str, connection_params, period_hours: int = 4, indexer_metrics: IndexerMetrics = None):
    """
    Factory function to get the appropriate indexer based on network.

    Args:
        network: Network identifier (torus, bittensor, polkadot)
        connection_params: ClickHouse connection parameters
        period_hours: Number of hours in each period (default: 4)
        indexer_metrics: Optional IndexerMetrics instance for recording metrics

    Returns:
        BalanceSeriesIndexer: Appropriate indexer instance for the network

    Raises:
        ValueError: If network is invalid
    """
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return TorusBalanceSeriesIndexer(connection_params, network, period_hours, indexer_metrics)
    elif network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
        return BittensorBalanceSeriesIndexer(connection_params, network, period_hours, indexer_metrics)
    elif network == Network.POLKADOT.value:
        return PolkadotBalanceSeriesIndexer(connection_params, network, period_hours, indexer_metrics)
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
        
        # Setup enhanced logging
        self.service_name = f"substrate-{network}-balance-series-consumer"
        setup_enhanced_logger(self.service_name)
        self.error_ctx = ErrorContextManager(self.service_name)
        
        # Metrics will be passed from main function
        self.metrics_registry = None
        self.indexer_metrics = None
        
        # Consumer-specific metrics (will be initialized when metrics are set)
        self.period_processing_duration = None
        self.addresses_processed_total = None
        self.periods_processed_total = None
        self.consumer_errors_total = None
        
        # Log service startup
        log_service_start(
            self.service_name,
            network=network,
            period_hours=period_hours,
            batch_size=batch_size
        )
    
    def set_metrics(self, metrics_registry, indexer_metrics):
        """Set metrics after initialization"""
        self.metrics_registry = metrics_registry
        self.indexer_metrics = indexer_metrics
        
        # Initialize consumer-specific metrics
        self.period_processing_duration = self.metrics_registry.create_histogram(
            'consumer_period_processing_duration_seconds',
            'Time spent processing periods',
            ['network', 'indexer']
        )
        
        self.addresses_processed_total = self.metrics_registry.create_counter(
            'consumer_addresses_processed_total',
            'Total addresses processed',
            ['network', 'indexer']
        )
        
        self.periods_processed_total = self.metrics_registry.create_counter(
            'consumer_periods_processed_total',
            'Total periods processed',
            ['network', 'indexer']
        )
        
        self.consumer_errors_total = self.metrics_registry.create_counter(
            'consumer_errors_total',
            'Total consumer processing errors',
            ['network', 'indexer', 'error_type']
        )

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

            # ENHANCED: Business decision logging
            self.error_ctx.log_business_decision(
                "resume_from_last_processed_period",
                "found_existing_processed_data",
                last_processed_timestamp=last_processed_timestamp,
                last_processed_block_height=last_processed_block_height,
                next_period_start=next_period_start,
                next_period_end=next_period_end
            )


            while not self.terminate_event.is_set():
                try:
                    current_time = int(datetime.now().timestamp() * 1000)

                    if next_period_end <= current_time:
                        # REMOVED: Verbose processing logs - metrics handle this
                        self._process_period(next_period_start, next_period_end)

                        # Update to the next period
                        next_period_start = next_period_end
                        next_period_end = next_period_start + self.period_ms
                    else:
                        # Wait until the next period ends
                        wait_time = (next_period_end - current_time) / 1000  # Convert to seconds
                        
                        # Only log if waiting more than 5 minutes (strategic logging)
                        if wait_time > 300:
                            logger.info(
                                "Waiting for period to complete",
                                extra={
                                    "wait_time_seconds": round(wait_time, 2),
                                    "period_start": next_period_start,
                                    "period_end": next_period_end,
                                    "current_time": current_time
                                }
                            )

                        # Sleep in small increments to check for termination
                        sleep_increment = 10  # seconds
                        for _ in range(int(wait_time / sleep_increment) + 1):
                            if self.terminate_event.is_set():
                                break
                            time.sleep(min(sleep_increment, wait_time))
                            wait_time -= sleep_increment

                except Exception as e:
                    if self.terminate_event.is_set():
                        break

                    # ENHANCED: Error logging with context
                    self.error_ctx.log_error(
                        "Period processing failed",
                        error=e,
                        operation="period_processing_loop",
                        period_start=next_period_start,
                        period_end=next_period_end,
                        current_time=current_time,
                        error_category=classify_error(e)
                    )
                    time.sleep(5)  # Brief pause before continuing

            # Log service shutdown
            log_service_stop(
                self.service_name,
                reason="terminate_event_received"
            )

        except KeyboardInterrupt:
            log_service_stop(
                self.service_name,
                reason="keyboard_interrupt"
            )
        except Exception as e:
            self.error_ctx.log_error(
                "Fatal consumer error",
                error=e,
                operation="consumer_main_loop",
                error_category=classify_error(e)
            )
        finally:
            self._cleanup()

    def _process_period(self, period_start: int, period_end: int):
        """Process a single time period

        Args:
            period_start: Start timestamp of the period (milliseconds)
            period_end: End timestamp of the period (milliseconds)
        """

        start_time = time.time()
        labels = {'network': self.network, 'indexer': 'balance_series'}

        try:
            # Find the block closest to the period end time
            end_block = self.block_stream_manager.get_block_by_nearest_timestamp(period_end)

            if not end_block:
                if self.consumer_errors_total:
                    self.consumer_errors_total.labels(**labels, error_type='no_block_found').inc()
                
                # ENHANCED: Error logging with context
                self.error_ctx.log_error(
                    "No block found for period end timestamp",
                    error=ValueError("Block not found"),
                    operation="find_period_end_block",
                    period_start=period_start,
                    period_end=period_end,
                    error_category="data_availability_error"
                )
                raise ValueError(f"No block found for period end timestamp {period_end}")

            block_height = end_block['block_height']
            block_hash = end_block['block_hash']
            block_timestamp = end_block['timestamp']

            # REMOVED: Verbose block found logging - not needed

            # Get all active addresses during this period
            active_addresses = self.block_stream_manager.get_blocks_by_block_timestamp_range(period_start, period_end, only_with_addresses=True)
            if not active_addresses:
                # ENHANCED: Business decision logging for empty periods
                self.error_ctx.log_business_decision(
                    "skip_empty_period",
                    "no_active_addresses_found",
                    period_start=period_start,
                    period_end=period_end,
                    block_height=block_height
                )
                return

            all_addresses = set()
            for block in active_addresses:
                all_addresses.update(block['addresses'])

            # REMOVED: Verbose processing logs - metrics handle counts

            if not all_addresses:
                if self.consumer_errors_total:
                    self.consumer_errors_total.labels(**labels, error_type='no_addresses_found').inc()
                
                # ENHANCED: Error logging with context
                self.error_ctx.log_error(
                    "No addresses found despite active blocks",
                    error=ValueError("Address extraction failed"),
                    operation="extract_active_addresses",
                    period_start=period_start,
                    period_end=period_end,
                    active_blocks_count=len(active_addresses),
                    error_category="data_processing_error"
                )
                raise ValueError(f"No addresses found for period {period_start}-{period_end}")

            # Query balances for all addresses at the end block
            address_balances = self._query_blockchain_balances(all_addresses, block_hash)

            # Record balance series data
            self.balance_series_indexer.record_balance_series(
                period_start, period_end, block_height, address_balances
            )

            # Record metrics
            processing_time = time.time() - start_time
            if self.period_processing_duration:
                self.period_processing_duration.labels(**labels).observe(processing_time)
            if self.addresses_processed_total:
                self.addresses_processed_total.labels(**labels).inc(len(address_balances))
            if self.periods_processed_total:
                self.periods_processed_total.labels(**labels).inc()
            if self.indexer_metrics:
                self.indexer_metrics.record_block_processed(block_height, processing_time)

            # REMOVED: Success logging - metrics handle this

        except Exception as e:
            if self.consumer_errors_total:
                self.consumer_errors_total.labels(**labels, error_type='processing_error').inc()
            
            # ENHANCED: Error logging with context
            self.error_ctx.log_error(
                "Period processing failed",
                error=e,
                operation="process_period",
                period_start=period_start,
                period_end=period_end,
                block_height=block_height if 'block_height' in locals() else None,
                addresses_count=len(all_addresses) if 'all_addresses' in locals() else None,
                processing_time=time.time() - start_time,
                error_category=classify_error(e)
            )
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
            # REMOVED: Verbose batch logging - not needed

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
                        break
                    else:
                        # ENHANCED: Error logging with context
                        self.error_ctx.log_error(
                            "Unexpected balance query error",
                            error=e,
                            operation="query_blockchain_balances",
                            address=address,
                            block_hash=block_hash,
                            batch_index=i//self.batch_size + 1,
                            error_category=classify_error(e)
                        )
                        raise  # Fail fast instead of continuing

            # Brief pause between batches
            if i + self.batch_size < len(address_list) and not self.terminate_event.is_set():
                time.sleep(0.1)

        # REMOVED: Success logging - not needed
        return result

    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'balance_series_indexer'):
                self.balance_series_indexer.close()
                # REMOVED: Success logging - not needed
        except Exception as e:
            self.error_ctx.log_error(
                "Error closing balance series indexer",
                error=e,
                operation="cleanup",
                component="balance_series_indexer",
                error_category=classify_error(e)
            )

        try:
            if hasattr(self, 'block_stream_manager'):
                self.block_stream_manager.close()
                # REMOVED: Success logging - not needed
        except Exception as e:
            self.error_ctx.log_error(
                "Error closing block stream manager",
                error=e,
                operation="cleanup",
                component="block_stream_manager",
                error_category=classify_error(e)
            )


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
    setup_enhanced_logger(service_name)

    # Initialize components
    balance_series_indexer = None
    block_stream_manager = None
    substrate_node = None

    try:
        clickhouse_params = get_clickhouse_connection_string(args.network)
        create_clickhouse_database(clickhouse_params)

        # Setup metrics first
        service_name = f'substrate-{args.network}-balance-series'
        metrics_registry = setup_metrics(service_name)
        indexer_metrics = IndexerMetrics(metrics_registry, args.network, 'balance_series')
        
        balance_series_indexer = get_balance_series_indexer(args.network, clickhouse_params, args.period_hours, indexer_metrics)
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

        
        # Set metrics after consumer creation
        consumer.set_metrics(metrics_registry, indexer_metrics)
        consumer.run()
        
    except Exception as e:
        error_ctx = ErrorContextManager(service_name)
        error_ctx.log_error(
            "Fatal startup error",
            error=e,
            operation="main_startup",
            error_category=classify_error(e)
        )
    finally:
        try:
            if balance_series_indexer:
                balance_series_indexer.close()
                # REMOVED: Success logging - not needed
        except Exception as e:
            logger.error(f"Error closing balance series indexer: {e}")

        try:
            if block_stream_manager:
                block_stream_manager.close()
                # REMOVED: Success logging - not needed
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")

        # REMOVED: Final stop logging - not needed
