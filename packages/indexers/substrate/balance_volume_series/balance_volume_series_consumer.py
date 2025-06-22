import argparse
import traceback
import time
from loguru import logger
from typing import Dict, Set, Optional, Any, List
from datetime import datetime
from decimal import Decimal
import collections

from packages.indexers.base import setup_logger, get_clickhouse_connection_string, create_clickhouse_database, \
    terminate_event
from packages.indexers.substrate import get_substrate_node_url, networks, data, Network
from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer_base import BalanceVolumeSeriesIndexerBase
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager
from packages.indexers.substrate.node.substrate_node import SubstrateNode
from packages.indexers.base.decimal_utils import convert_to_decimal_units


from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer import get_balance_volume_series_indexer


class BalanceVolumeSeriesConsumer:
    def __init__(
            self,
            block_stream_manager: BlockStreamManager,
            substrate_node: SubstrateNode,
            balance_volume_series_indexer: BalanceVolumeSeriesIndexerBase,
            terminate_event,
            network: str,
            period_hours: int = 4,
            batch_size: int = 100
    ):
        """Initialize the Balance Volume Series Consumer

        Args:
            block_stream_manager: BlockStreamManager instance for querying block data
            substrate_node: SubstrateNode instance for querying blockchain data
            balance_volume_series_indexer: BalanceVolumeSeriesIndexer instance for storing volume series data
            terminate_event: Event to signal termination
            network: Network identifier (e.g., 'torus', 'polkadot')
            period_hours: Number of hours in each period (default: 4)
            batch_size: Number of blocks to query in a single batch
        """
        self.block_stream_manager = block_stream_manager
        self.substrate_node = substrate_node
        self.balance_volume_series_indexer = balance_volume_series_indexer
        self.terminate_event = terminate_event
        self.network = network
        self.period_hours = period_hours
        self.period_ms = period_hours * 60 * 60 * 1000  # Convert hours to milliseconds
        self.batch_size = batch_size

    def run(self):
        """Main processing loop for balance volume series data"""
        try:
            # Get the latest processed period
            last_processed_timestamp, last_processed_block_height = self.balance_volume_series_indexer.get_latest_processed_period()
            next_period_start, next_period_end = self.balance_volume_series_indexer.get_next_period_to_process()

            logger.info(f"Starting balance volume series consumer from period {next_period_start}-{next_period_end} at block height {last_processed_block_height}")

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
            logger.info("Balance volume series consumer stopped")

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
                raise ValueError(error_msg)

            block_height = end_block['block_height']
            block_hash = end_block['block_hash']
            block_timestamp = end_block['timestamp']

            logger.info(f"Found block {block_height} (hash: {block_hash}) at timestamp {block_timestamp} for period end {period_end}")

            # Get all blocks in this period
            period_blocks = self.block_stream_manager.get_blocks_by_block_timestamp_range(
                period_start, period_end
            )

            if not period_blocks:
                logger.info(f"No blocks found for period {period_start}-{period_end}")
                return

            logger.info(f"Processing {len(period_blocks)} blocks for period {period_start}-{period_end}")

            # Process all the transfers in this period to calculate volume metrics
            volume_data = self._calculate_volume_metrics(period_blocks)

            # Record volume series data
            self.balance_volume_series_indexer.record_volume_series(
                period_start, period_end, block_height, block_hash, volume_data
            )

            logger.success(f"Successfully processed period {period_start}-{period_end} with {volume_data.get('transaction_count', 0)} transactions")

        except Exception as e:
            logger.error(f"Error processing period {period_start}-{period_end}: {e}")
            raise

    def _calculate_volume_metrics(self, blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate volume metrics from transfer events in blocks

        Args:
            blocks: List of blocks with events

        Returns:
            Dictionary with volume metrics
        """
        try:
            # Initialize counters
            total_volume_raw = 0
            total_fees_raw = 0
            transaction_count = 0
            senders = set()
            receivers = set()
            active_addresses = set()
            
            # Transaction size categories
            micro_tx_count = 0
            micro_tx_volume_raw = 0
            small_tx_count = 0
            small_tx_volume_raw = 0
            medium_tx_count = 0
            medium_tx_volume_raw = 0
            large_tx_count = 0
            large_tx_volume_raw = 0
            whale_tx_count = 0
            whale_tx_volume_raw = 0
            
            # Process each block
            for block in blocks:
                block_height = block.get('block_height')
                
                # Get transfers for this block
                transfers = self._extract_transfers_from_block(block)
                
                for transfer in transfers:
                    transaction_count += 1
                    
                    # Extract transfer data
                    sender = transfer.get('from_address')
                    receiver = transfer.get('to_address')
                    amount_raw = transfer.get('amount', 0)
                    fee_raw = transfer.get('fee', 0)
                    
                    # Skip empty or invalid transfers
                    if not sender or not receiver or amount_raw <= 0:
                        continue
                        
                    # Update totals
                    total_volume_raw += amount_raw
                    total_fees_raw += fee_raw
                    
                    # Update address sets
                    senders.add(sender)
                    receivers.add(receiver)
                    active_addresses.add(sender)
                    active_addresses.add(receiver)
                    
                    # Categorize transaction by size
                    amount_decimal = convert_to_decimal_units(amount_raw, self.network)
                    category = self.balance_volume_series_indexer.categorize_transaction_size(amount_decimal)
                    
                    if category == 'micro':
                        micro_tx_count += 1
                        micro_tx_volume_raw += amount_raw
                    elif category == 'small':
                        small_tx_count += 1
                        small_tx_volume_raw += amount_raw
                    elif category == 'medium':
                        medium_tx_count += 1
                        medium_tx_volume_raw += amount_raw
                    elif category == 'large':
                        large_tx_count += 1
                        large_tx_volume_raw += amount_raw
                    elif category == 'whale':
                        whale_tx_count += 1
                        whale_tx_volume_raw += amount_raw
            
            # Calculate network density (if there are active addresses)
            network_density = 0.0
            if len(active_addresses) > 1:
                # Calculate as ratio of actual connections to possible connections
                # In a fully connected network, each address can connect to all others
                possible_connections = len(active_addresses) * (len(active_addresses) - 1) / 2
                actual_connections = len(senders) * len(receivers)
                network_density = min(1.0, actual_connections / possible_connections) if possible_connections > 0 else 0.0
            
            # Calculate average fee
            avg_fee_raw = total_fees_raw / transaction_count if transaction_count > 0 else 0
            
            # Return aggregated data
            return {
                'total_volume': total_volume_raw,
                'transaction_count': transaction_count,
                'unique_senders': len(senders),
                'unique_receivers': len(receivers),
                'micro_tx_count': micro_tx_count,
                'micro_tx_volume': micro_tx_volume_raw,
                'small_tx_count': small_tx_count,
                'small_tx_volume': small_tx_volume_raw,
                'medium_tx_count': medium_tx_count,
                'medium_tx_volume': medium_tx_volume_raw,
                'large_tx_count': large_tx_count,
                'large_tx_volume': large_tx_volume_raw,
                'whale_tx_count': whale_tx_count,
                'whale_tx_volume': whale_tx_volume_raw,
                'total_fees': total_fees_raw,
                'avg_fee': avg_fee_raw,
                'active_addresses': len(active_addresses),
                'network_density': network_density
            }
            
        except Exception as e:
            logger.error(f"Error calculating volume metrics: {e}")
            raise
    
    def _extract_transfers_from_block(self, block: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract transfer events from a block
        
        This is a generic implementation and may need to be overridden by network-specific classes
        to handle different transfer event formats.
        
        Args:
            block: Block data containing events
            
        Returns:
            List of transfer dictionaries with from_address, to_address, amount, fee
        """
        transfers = []
        
        try:
            # Access events from the block
            events = block.get('events', [])
            
            for event in events:
                # Check if this is a transfer event
                # Different networks might have different event formats
                module = event.get('module_id', '').lower()
                event_id = event.get('event_id', '').lower()
                
                # Check for various transfer event types
                is_transfer = (
                    (module == 'balances' and event_id == 'transfer') or
                    (module == 'currencies' and event_id == 'transferred') or
                    # Add more transfer event types as needed
                    False
                )
                
                if is_transfer:
                    # Extract transfer data
                    params = event.get('params', {})
                    
                    from_address = params.get('from', '')
                    to_address = params.get('to', '')
                    amount = int(params.get('amount', 0))
                    
                    # Fee information might be in the extrinsic
                    fee = 0  # Default fee
                    extrinsic_idx = event.get('extrinsic_idx')
                    if extrinsic_idx is not None:
                        # Find the extrinsic fee if available
                        extrinsics = block.get('extrinsics', [])
                        for extrinsic in extrinsics:
                            if extrinsic.get('idx') == extrinsic_idx:
                                fee = int(extrinsic.get('fee', 0))
                                break
                    
                    transfers.append({
                        'from_address': from_address,
                        'to_address': to_address,
                        'amount': amount,
                        'fee': fee
                    })
            
            return transfers
            
        except Exception as e:
            logger.error(f"Error extracting transfers from block {block.get('block_height')}: {e}")
            return []

    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'balance_volume_series_indexer'):
                self.balance_volume_series_indexer.close()
                logger.info("Closed balance volume series indexer connection")
        except Exception as e:
            logger.error(f"Error closing balance volume series indexer: {e}")

        try:
            if hasattr(self, 'block_stream_manager'):
                self.block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Balance Volume Series Consumer')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to track transfer volumes for (polkadot, torus, or bittensor)'
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
        help='Number of blocks to process in a single batch'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-balance-volume-series'
    setup_logger(service_name)

    # Initialize components
    balance_volume_series_indexer = None
    block_stream_manager = None
    substrate_node = None

    try:
        clickhouse_params = get_clickhouse_connection_string(args.network)
        create_clickhouse_database(clickhouse_params)

        balance_volume_series_indexer = get_balance_volume_series_indexer(args.network, clickhouse_params, args.period_hours)
        block_stream_manager = BlockStreamManager(clickhouse_params, args.network, terminate_event)

        node_url = get_substrate_node_url(args.network)
        substrate_node = SubstrateNode(args.network, node_url, terminate_event)

        consumer = BalanceVolumeSeriesConsumer(
            block_stream_manager,
            substrate_node,
            balance_volume_series_indexer,
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
            if balance_volume_series_indexer:
                balance_volume_series_indexer.close()
                logger.info("Closed balance volume series indexer connection")
        except Exception as e:
            logger.error(f"Error closing balance volume series indexer: {e}")

        try:
            if block_stream_manager:
                block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")

        logger.info("Balance volume series consumer stopped")