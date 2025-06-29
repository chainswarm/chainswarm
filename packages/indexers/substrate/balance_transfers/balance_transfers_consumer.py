import argparse
import traceback
import time
from loguru import logger
from packages.indexers.base import (
    setup_enhanced_logger, get_clickhouse_connection_string, create_clickhouse_database,
    terminate_event, setup_metrics, get_metrics_registry, ErrorContextManager,
    log_service_start, log_service_stop, classify_error
)
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
        
        # Setup enhanced logging
        self.service_name = f"substrate-{network}-balance-transfers-consumer"
        setup_enhanced_logger(self.service_name)
        self.error_ctx = ErrorContextManager(self.service_name)
        
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
        
        # Log service startup
        log_service_start(
            self.service_name,
            network=network,
            batch_size=batch_size,
            continuous_mode=True
        )

    def run(self):
        """Main processing loop with enhanced error logging and reduced verbosity"""
        try:
            last_processed_height = self.balance_transfers_indexer.get_latest_processed_block_height()
            
            if last_processed_height > 0:
                start_height = last_processed_height + 1
                self.error_ctx.log_business_decision(
                    "resume_from_last_processed",
                    "existing_processed_blocks_found",
                    last_processed_height=last_processed_height,
                    start_height=start_height
                )
            else:
                start_height = 1
                self.error_ctx.log_business_decision(
                    "start_from_genesis",
                    "no_processed_blocks_found",
                    start_height=start_height
                )

            # Track consecutive empty responses for smart logging
            consecutive_empty_responses = 0
            
            while not self.terminate_event.is_set():
                try:
                    latest_block_height = self.block_stream_manager.get_latest_block_height()
                    
                    if start_height > latest_block_height:
                        # Only log if we're significantly ahead (unusual situation)
                        if start_height - latest_block_height > 10:
                            logger.warning(
                                "Consumer significantly ahead of chain tip",
                                extra={
                                    "current_target": start_height,
                                    "latest_height": latest_block_height,
                                    "blocks_ahead": start_height - latest_block_height,
                                    "possible_causes": ["chain_sync_lag", "indexer_too_fast"]
                                }
                            )
                        time.sleep(10)
                        continue
                    
                    # Calculate end height for the batch (don't exceed latest block height)
                    end_height = min(start_height + self.batch_size - 1, latest_block_height)
                    
                    # Get blocks in batch
                    blocks = self.block_stream_manager.get_blocks_by_block_height_range(start_height, end_height)
                    
                    if not blocks:
                        consecutive_empty_responses += 1
                        
                        # Record empty batch metric
                        if self.metrics_registry:
                            self.empty_batches_total.labels(network=self.network, indexer="balance_transfers").inc()
                        
                        # Only log warning after multiple consecutive empty responses
                        if consecutive_empty_responses >= 3:
                            logger.warning(
                                "Multiple consecutive empty block responses",
                                extra={
                                    "consecutive_count": consecutive_empty_responses,
                                    "height_range": f"{start_height}-{end_height}",
                                    "latest_chain_height": latest_block_height,
                                    "possible_causes": ["block_stream_lag", "database_sync_issues"]
                                }
                            )
                            consecutive_empty_responses = 0  # Reset counter
                            
                        # Move to the next batch even if no blocks were found
                        start_height = end_height + 1
                        continue
                    
                    # Reset empty response counter on successful fetch
                    consecutive_empty_responses = 0

                    # Record blocks fetched metric
                    if self.metrics_registry:
                        self.blocks_fetched_total.labels(network=self.network, indexer="balance_transfers").inc(len(blocks))

                    # Process blocks for transfers (metrics handle the counts)
                    self._process_blocks(blocks)
                    
                    # Update blocks behind metric
                    if self.metrics_registry and hasattr(self.balance_transfers_indexer, 'metrics') and self.balance_transfers_indexer.metrics:
                        blocks_behind = latest_block_height - end_height
                        self.balance_transfers_indexer.metrics.update_blocks_behind(max(0, blocks_behind))

                    # Update start height to the next block after this batch
                    start_height = end_height + 1
                    
                except Exception as e:
                    if self.terminate_event.is_set():
                        break

                    # Record error metric
                    if self.metrics_registry:
                        self.consumer_errors_total.labels(
                            network=self.network,
                            indexer="balance_transfers",
                            error_type=classify_error(e)
                        ).inc()
                    
                    # ENHANCED: Error logging with full context
                    self.error_ctx.log_error(
                        "Block processing batch failed",
                        error=e,
                        operation="batch_processing",
                        start_height=start_height,
                        end_height=end_height,
                        batch_size=self.batch_size,
                        latest_chain_height=latest_block_height,
                        processing_state=self._get_processing_state(),
                        error_category=classify_error(e)
                    )
                    time.sleep(5)  # Brief pause before continuing
                    
            # Log service shutdown
            log_service_stop(
                self.service_name,
                reason="terminate_event_received",
                last_processed_height=start_height - 1
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

    def _process_blocks(self, blocks):
        """Process a batch of blocks for transfer extraction
        
        Args:
            blocks: List of blocks to process
        """
        batch_start_time = time.time()

        try:
            # Check for termination before starting
            if self.terminate_event.is_set():
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
            
            # REMOVED: Success logging - metrics handle this
            
        except Exception as e:
            # Record batch processing error
            if self.metrics_registry:
                self.consumer_errors_total.labels(
                    network=self.network,
                    indexer="balance_transfers",
                    error_type=classify_error(e)
                ).inc()
            
            # ENHANCED: Error logging with context
            self.error_ctx.log_error(
                "Block batch processing failed",
                error=e,
                operation="block_batch_processing",
                block_count=len(blocks),
                first_block_height=min(block['block_height'] for block in blocks) if blocks else None,
                last_block_height=max(block['block_height'] for block in blocks) if blocks else None,
                batch_duration=time.time() - batch_start_time,
                error_category=classify_error(e)
            )
            raise
    
    def _get_processing_state(self) -> dict:
        """Get current processing state for error context."""
        try:
            return {
                "last_processed_height": self.balance_transfers_indexer.get_latest_processed_block_height(),
                "terminate_event_set": self.terminate_event.is_set(),
                "batch_size": self.batch_size,
                "network": self.network,
                "indexer_healthy": self._test_indexer_health()
            }
        except Exception:
            return {"error": "unable_to_get_processing_state"}
    
    def _test_indexer_health(self) -> bool:
        """Test if the indexer is healthy."""
        try:
            # Simple health check - try to get latest processed height
            self.balance_transfers_indexer.get_latest_processed_block_height()
            return True
        except Exception:
            return False

    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'balance_transfers_indexer'):
                self.balance_transfers_indexer.close()
                # REMOVED: Success logging - not needed
        except Exception as e:
            self.error_ctx.log_error(
                "Error closing balance transfers indexer",
                error=e,
                operation="cleanup",
                component="balance_transfers_indexer",
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
    setup_enhanced_logger(service_name)
    
    # Setup metrics
    metrics_registry = setup_metrics(service_name, start_server=True)
    # REMOVED: Metrics server startup logging - not needed

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
                # REMOVED: Success logging - not needed
        except Exception as e:
            logger.error(f"Error closing balance transfers indexer: {e}")

        try:
            if block_stream_manager:
                block_stream_manager.close()
                # REMOVED: Success logging - not needed
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")

        # REMOVED: Final stop logging - not needed