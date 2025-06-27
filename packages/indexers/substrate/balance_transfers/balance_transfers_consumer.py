import argparse
import traceback
import time
from loguru import logger
from packages.indexers.base import setup_logger, get_clickhouse_connection_string, create_clickhouse_database, \
    terminate_event, setup_metrics, get_metrics_registry
from packages.indexers.substrate import get_substrate_node_url, networks, Network
from packages.indexers.substrate.balance_transfers.balance_transfers_indexer import BalanceTransfersIndexer
from packages.indexers.substrate.balance_transfers.balance_transfers_indexer_torus import TorusBalanceTransfersIndexer
from packages.indexers.substrate.balance_transfers.balance_transfers_indexer_bittensor import BittensorBalanceTransfersIndexer
from packages.indexers.substrate.balance_transfers.balance_transfers_indexer_polkadot import PolkadotBalanceTransfersIndexer
from packages.indexers.substrate.block_range_partitioner import get_partitioner
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager


def get_balance_transfers_indexer(network: str, connection_params, partitioner):
    """
    Factory function to get the appropriate transfers indexer based on network.
    
    Args:
        network: Network identifier (torus, bittensor, polkadot)
        connection_params: ClickHouse connection parameters
        partitioner: BlockRangePartitioner instance
        
    Returns:
        BalanceTransfersIndexer: Appropriate indexer instance for the network
        
    Raises:
        ValueError: If network is invalid
    """
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return TorusBalanceTransfersIndexer(connection_params, partitioner, network)
    elif network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
        return BittensorBalanceTransfersIndexer(connection_params, partitioner, network)
    elif network == Network.POLKADOT.value:
        return PolkadotBalanceTransfersIndexer(connection_params, partitioner, network)
    else:
        raise ValueError(f"Unsupported network: {network}")


class BalanceTransfersConsumer:
    def __init__(
            self,
            block_stream_manager: BlockStreamManager,
            balance_transfers_indexer: BalanceTransfersIndexer,
            terminate_event,
            network: str,
            batch_size: int = 100
    ):
        """Initialize the Balance Transfers Consumer
        
        Args:
            block_stream_manager: BlockStreamManager instance for querying block data
            balance_transfers_indexer: BalanceTransfersIndexer instance for storing transfers
            terminate_event: Event to signal termination
            network: Network identifier (e.g., 'torus', 'polkadot')
            batch_size: Number of blocks to process in a single batch
        """
        self.block_stream_manager = block_stream_manager
        self.balance_transfers_indexer = balance_transfers_indexer
        self.terminate_event = terminate_event
        self.network = network
        self.batch_size = batch_size
        
        # Get metrics registry for additional consumer-level metrics
        service_name = f"substrate-{network}-balance-transfers"
        self.metrics_registry = get_metrics_registry(service_name)
        if self.metrics_registry:
            # Consumer-specific metrics
            self.batch_processing_duration = self.metrics_registry.create_histogram(
                'consumer_batch_processing_duration_seconds',
                'Time spent processing batches',
                ['network', 'indexer']
            )
            self.blocks_fetched_total = self.metrics_registry.create_counter(
                'consumer_blocks_fetched_total',
                'Total blocks fetched from block stream',
                ['network', 'indexer']
            )
            self.empty_batches_total = self.metrics_registry.create_counter(
                'consumer_empty_batches_total',
                'Total empty batches encountered',
                ['network', 'indexer']
            )
            self.consumer_errors_total = self.metrics_registry.create_counter(
                'consumer_errors_total',
                'Total consumer processing errors',
                ['network', 'indexer', 'error_type']
            )

    def run(self):
        """Main processing loop with improved termination handling and batch processing"""
        try:
            last_processed_height = self.balance_transfers_indexer.get_latest_processed_block_height()
            
            if last_processed_height > 0:
                start_height = last_processed_height + 1
            else:
                start_height = 1

            # Use the batch_size from constructor
            logger.info(f"Starting balance transfers consumer from block {start_height}")
            logger.info(f"Batch size for block processing: {self.batch_size}")
            
            while not self.terminate_event.is_set():
                try:
                    latest_block_height = self.block_stream_manager.get_latest_block_height()
                    
                    if start_height > latest_block_height:
                        logger.info(f"Waiting for new blocks. Current target: {start_height}, Latest height: {latest_block_height}")
                        time.sleep(10)
                        continue
                    
                    # Calculate end height for the batch (don't exceed latest block height)
                    end_height = min(start_height + self.batch_size - 1, latest_block_height)
                    
                    logger.info(f"Fetching blocks from {start_height} to {end_height}")
                    
                    # Get blocks in batch
                    blocks = self.block_stream_manager.get_blocks_by_block_height_range(start_height, end_height)
                    
                    if not blocks:
                        logger.warning(f"No blocks returned for range {start_height}-{end_height}")
                        # Record empty batch metric
                        if self.metrics_registry:
                            self.empty_batches_total.labels(network=self.network, indexer="balance_transfers").inc()
                        # Move to the next batch even if no blocks were found
                        start_height = end_height + 1
                        continue

                    # Record blocks fetched metric
                    if self.metrics_registry:
                        self.blocks_fetched_total.labels(network=self.network, indexer="balance_transfers").inc(len(blocks))

                    # Process blocks for transfers
                    logger.info(f"Processing {len(blocks)} blocks for transfer extraction")
                    self._process_blocks(blocks)
                    
                    # Update blocks behind metric
                    if self.metrics_registry and hasattr(self.balance_transfers_indexer, 'metrics') and self.balance_transfers_indexer.metrics:
                        blocks_behind = latest_block_height - end_height
                        self.balance_transfers_indexer.metrics.update_blocks_behind(max(0, blocks_behind))
                    
                    # Update start height to the next block after this batch
                    start_height = end_height + 1
                    
                except Exception as e:
                    if self.terminate_event.is_set():
                        logger.info("Termination requested, stopping processing")
                        break
                    
                    # Record error metric
                    if self.metrics_registry:
                        self.consumer_errors_total.labels(
                            network=self.network,
                            indexer="balance_transfers",
                            error_type="processing_error"
                        ).inc()
                        
                    logger.error(f"Error processing blocks starting from {start_height}: {e}", error=e, trb=traceback.format_exc())
                    time.sleep(5)  # Brief pause before continuing
                    # We don't break the loop - continue processing
                    
            logger.info("Termination event received, shutting down")
                    
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Fatal error: {e}", error=e, trb=traceback.format_exc())
        finally:
            self._cleanup()
            logger.info("Balance transfers consumer stopped")

    def _process_blocks(self, blocks):
        """Process a batch of blocks for transfer extraction
        
        Args:
            blocks: List of blocks to process
        """
        batch_start_time = time.time()
        try:
            # Check for termination before starting
            if self.terminate_event.is_set():
                logger.info(f"Skipping block processing due to termination request")
                return
                
            # Index transfers from the blocks
            self.balance_transfers_indexer.index_blocks(blocks)
            
            # Record batch processing metrics
            if self.metrics_registry:
                batch_duration = time.time() - batch_start_time
                self.batch_processing_duration.labels(
                    network=self.network,
                    indexer="balance_transfers"
                ).observe(batch_duration)
            
            logger.success(f"Processed {len(blocks)} blocks for transfer extraction")
            
        except Exception as e:
            # Record batch processing error
            if self.metrics_registry:
                self.consumer_errors_total.labels(
                    network=self.network,
                    indexer="balance_transfers",
                    error_type="batch_processing_error"
                ).inc()
            
            logger.error(f"Error processing blocks: {e}")
            raise

    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'balance_transfers_indexer'):
                self.balance_transfers_indexer.close()
                logger.info("Closed balance transfers indexer connection")
        except Exception as e:
            logger.error(f"Error closing balance transfers indexer: {e}")
        
        try:
            if hasattr(self, 'block_stream_manager'):
                self.block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Balance Transfers Consumer')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to extract transfers for (polkadot, torus, or bittensor)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of blocks to process in a single batch'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-balance-transfers'
    setup_logger(service_name)
    
    # Setup metrics
    metrics_registry = setup_metrics(service_name, start_server=True)
    logger.info(f"Metrics server started for {service_name}")

    # Initialize components
    balance_transfers_indexer = None
    block_stream_manager = None

    try:
        clickhouse_params = get_clickhouse_connection_string(args.network)
        create_clickhouse_database(clickhouse_params)

        partitioner = get_partitioner(args.network)
        balance_transfers_indexer = get_balance_transfers_indexer(args.network, clickhouse_params, partitioner)
        block_stream_manager = BlockStreamManager(clickhouse_params, args.network, terminate_event)

        consumer = BalanceTransfersConsumer(
            block_stream_manager,
            balance_transfers_indexer,
            terminate_event,
            args.network,
            args.batch_size
        )

        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error", error=e, trb=traceback.format_exc())
    finally:
        try:
            if balance_transfers_indexer:
                balance_transfers_indexer.close()
                logger.info("Closed balance transfers indexer connection")
        except Exception as e:
            logger.error(f"Error closing balance transfers indexer: {e}")

        try:
            if block_stream_manager:
                block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")

        logger.info("Balance transfers consumer stopped")