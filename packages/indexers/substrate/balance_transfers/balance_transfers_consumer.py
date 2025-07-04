import argparse
import traceback
import time
import signal
from loguru import logger
from packages.indexers.base import (
    get_clickhouse_connection_string, create_clickhouse_database, terminate_event,
    setup_metrics, setup_logger, IndexerMetrics,
)
from packages.indexers.substrate import get_substrate_node_url, networks, Network
from packages.indexers.substrate.balance_transfers.balance_transfers_indexer import BalanceTransfersIndexer
from packages.indexers.substrate.balance_transfers.balance_transfers_indexer_torus import TorusBalanceTransfersIndexer
from packages.indexers.substrate.balance_transfers.balance_transfers_indexer_bittensor import BittensorBalanceTransfersIndexer
from packages.indexers.substrate.balance_transfers.balance_transfers_indexer_polkadot import PolkadotBalanceTransfersIndexer
from packages.indexers.substrate.block_range_partitioner import get_partitioner, BlockRangePartitioner
from packages.indexers.substrate.block_stream.block_stream_indexer import BlockStreamIndexer
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager
from packages.indexers.substrate.node.substrate_node import SubstrateNode
from packages.indexers.substrate.assets.asset_manager import AssetManager


def get_balance_transfers_indexer(partitioner, indexer_metrics: IndexerMetrics, network: str, connection_params, asset_manager: AssetManager):
    """
    Factory function to get the appropriate transfers indexer based on network.
    
    Args:
        partitioner: BlockRangePartitioner instance
        indexer_metrics: IndexerMetrics instance for recording metrics (required)
        network: Network identifier (torus, bittensor, polkadot)
        connection_params: ClickHouse connection parameters
        asset_manager: AssetManager instance for managing assets
        
    Returns:
        BalanceTransfersIndexer: Appropriate indexer instance for the network
        
    Raises:
        ValueError: If network is invalid
    """
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return TorusBalanceTransfersIndexer(connection_params, partitioner, network, indexer_metrics, asset_manager)
    elif network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
        return BittensorBalanceTransfersIndexer(connection_params, partitioner, network, indexer_metrics, asset_manager)
    elif network == Network.POLKADOT.value:
        return PolkadotBalanceTransfersIndexer(connection_params, partitioner, network, indexer_metrics, asset_manager)
    else:
        raise ValueError(f"Unsupported network: {network}")


class BalanceTransfersConsumer:
    def __init__(
            self,
            block_stream_manager: BlockStreamManager,
            balance_transfers_indexer: BalanceTransfersIndexer,
            metrics_registry,
            indexer_metrics,
            terminate_event,
            network: str,
            batch_size: int = 100
    ):
        """Initialize the Balance Transfers Consumer
        
        Args:
            block_stream_manager: BlockStreamManager instance for querying block data
            balance_transfers_indexer: BalanceTransfersIndexer instance for storing transfers
            metrics_registry: Metrics registry for consumer-level metrics
            indexer_metrics: IndexerMetrics instance for recording metrics
            terminate_event: Event to signal termination
            network: Network identifier (e.g., 'torus', 'polkadot')
            batch_size: Number of blocks to process in a single batch
        """
        self.block_stream_manager = block_stream_manager
        self.balance_transfers_indexer = balance_transfers_indexer
        self.metrics_registry = metrics_registry
        self.indexer_metrics = indexer_metrics
        self.terminate_event = terminate_event
        self.network = network
        self.batch_size = batch_size
        
        # Consumer-specific metrics
        self.batch_processing_duration = metrics_registry.create_histogram(
            'consumer_batch_processing_duration_seconds',
            'Time spent processing batches',
            ['network', 'indexer']
        )
        self.blocks_fetched_total = metrics_registry.create_counter(
            'consumer_blocks_fetched_total',
            'Total blocks fetched from block stream',
            ['network', 'indexer']
        )
        self.empty_batches_total = metrics_registry.create_counter(
            'consumer_empty_batches_total',
            'Total empty batches encountered',
            ['network', 'indexer']
        )
        self.consumer_errors_total = metrics_registry.create_counter(
            'consumer_errors_total',
            'Total consumer processing errors',
            ['network', 'indexer', 'error_type']
        )
        self.blocks_behind_latest = metrics_registry.create_gauge(
            'consumer_blocks_behind_latest',
            'Number of blocks behind the latest block',
            ['network', 'indexer']
        )

    def run(self):
        """Main processing loop with simplified logging"""
        
        if self.terminate_event.is_set():
            return
            
        try:
            last_processed_height = self.balance_transfers_indexer.get_latest_processed_block_height()
            
            if last_processed_height > 0:
                start_height = last_processed_height + 1
                logger.info(
                    "Starting consumer from last processed block height",
                    business_decision="resume_from_last_processed_block",
                    reason="continuous_mode_startup",
                    extra={
                        "last_processed_height": last_processed_height,
                        "start_height": start_height,
                    }
                )
            else:
                start_height = 1
                logger.info(
                    "Starting consumer from genesis block",
                    business_decision="start_from_genesis",
                    reason="no_processed_blocks_found",
                    extra={
                        "start_height": start_height
                    }
                )

            # Check terminate_event at the start of each iteration
            while not self.terminate_event.is_set():
                if self.terminate_event.is_set():
                    return

                try:
                    latest_block_height = self.block_stream_manager.get_latest_block_height()
                    
                    if start_height > latest_block_height:
                        # Only log if significantly ahead (unusual situation)
                        if start_height - latest_block_height > 10:
                            logger.warning(
                                "Consumer significantly ahead of chain tip",
                                extra={
                                    "current_height": start_height,
                                    "chain_height": latest_block_height,
                                    "blocks_ahead": start_height - latest_block_height,
                                    "possible_causes": ["chain_sync_lag", "indexer_too_fast"]
                                }
                            )
                        for _ in range(10):
                            if self.terminate_event.is_set():
                                return
                            time.sleep(1)
                        continue
                    
                    # Calculate end height for the batch (don't exceed latest block height)
                    end_height = min(start_height + self.batch_size - 1, latest_block_height)
                    
                    if self.terminate_event.is_set():
                        return
                        
                    batch_start_time = time.time()
                    blocks = self.block_stream_manager.get_blocks_by_block_height_range(start_height, end_height)

                    # Record blocks fetched metric
                    if blocks:
                        self.blocks_fetched_total.labels(network=self.network, indexer="balance_transfers").inc(len(blocks))

                    if not self.terminate_event.is_set() and blocks:
                        if self.terminate_event.is_set():
                            return
                            
                        # Index blocks
                        try:
                            self.balance_transfers_indexer.index_blocks(blocks)
                            
                            # Record batch processing metrics
                            batch_duration = time.time() - batch_start_time
                            self.batch_processing_duration.labels(
                                network=self.network,
                                indexer="balance_transfers"
                            ).observe(batch_duration)

                            # Update blocks behind metric
                            blocks_behind = max(0, latest_block_height - end_height)
                            self.blocks_behind_latest.labels(
                                network=self.network,
                                indexer="balance_transfers"
                            ).set(blocks_behind)

                            if self.terminate_event.is_set():
                                return
                                
                            start_height = end_height + 1

                        except Exception as e:
                            self.consumer_errors_total.labels(
                                network=self.network,
                                indexer="balance_transfers",
                                error_type="indexing_error"
                            ).inc()

                            logger.error(
                                "Error indexing blocks",
                                error=e,
                                traceback=traceback.format_exc(),
                                extra={
                                    "start_height": start_height,
                                    "end_height": end_height,
                                    "blocks_count": len(blocks),
                                    "chain_height": latest_block_height
                                }
                            )

                            time.sleep(5)
                    elif not self.terminate_event.is_set():
                        self.empty_batches_total.labels(network=self.network, indexer="balance_transfers").inc()
                        logger.warning(
                            "No blocks returned from blockchain",
                            extra={
                                "start_height": start_height,
                                "end_height": end_height,
                                "range_size": end_height - start_height + 1,
                                "chain_height": latest_block_height if 'latest_block_height' in locals() else None,
                                "possible_causes": ["blockchain_sync_lag", "network_issues"]
                            }
                        )
                        for _ in range(5):
                            if self.terminate_event.is_set():
                                return
                            time.sleep(1)
                
                except Exception as e:
                    if self.terminate_event.is_set():
                        return

                    self.consumer_errors_total.labels(
                        network=self.network,
                        indexer="balance_transfers",
                        error_type="processing_error"
                    ).inc()

                    logger.error(
                        "Error processing block range",
                        error=e,
                        traceback=traceback.format_exc(),
                        extra={
                            "start_height": start_height if 'start_height' in locals() else None,
                            "chain_height": latest_block_height if 'latest_block_height' in locals() else None,
                        }
                    )

                    for _ in range(5):
                        if self.terminate_event.is_set():
                            return
                        time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(
                "Fatal error in consumer main loop",
                error=e,
                traceback=traceback.format_exc()
            )

    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'balance_transfers_indexer'):
                self.balance_transfers_indexer.close()
        except Exception as e:
            logger.error(
                "Error closing balance transfers indexer",
                error=e,
                traceback=traceback.format_exc(),
                extra={
                    "operation": "cleanup",
                    "component": "balance_transfers_indexer"
                }
            )
        
        try:
            if hasattr(self, 'block_stream_manager'):
                self.block_stream_manager.close()
        except Exception as e:
            logger.error(
                "Error closing block stream manager",
                error=e,
                traceback=traceback.format_exc(),
                extra={
                    "operation": "cleanup",
                    "component": "block_stream_manager"
                }
            )
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Balance Transfers Consumer')
    parser.add_argument('--batch-size', type=int, default=100, help='Number of blocks to process in a batch')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to extract transfers for (polkadot, torus, or bittensor)'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-balance-transfers'
    setup_logger(service_name)

    logger.info(
        "Starting Balance Transfers Consumer",
        extra={
            "service": service_name,
            "network": args.network,
            "batch_size": args.batch_size
        }
    )

    def signal_handler(sig, frame):
        logger.info(
            "Shutdown signal received",
            extra={
                "signal": sig,
                "service": service_name,
                "graceful_shutdown": True
            }
        )
        terminate_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    connection_params = get_clickhouse_connection_string(args.network)
    create_clickhouse_database(connection_params)

    metrics_registry = setup_metrics(service_name, start_server=True)
    metrics = IndexerMetrics(metrics_registry, args.network, "balance_transfers")

    asset_manager = AssetManager(args.network, connection_params)
    asset_manager.init_tables()
    logger.info(f"Initialized AssetManager and native assets for {args.network}")

    partitioner = get_partitioner(args.network)
    substrate_node = SubstrateNode(args.network, get_substrate_node_url(args.network), terminate_event)
    block_stream_indexer = BlockStreamIndexer(partitioner, metrics, connection_params, args.network)
    balance_transfers_indexer = get_balance_transfers_indexer(partitioner, metrics, args.network, connection_params, asset_manager)

    block_stream_manager = BlockStreamManager(block_stream_indexer, substrate_node, partitioner, connection_params, args.network, terminate_event)

    consumer = BalanceTransfersConsumer(
        block_stream_manager,
        balance_transfers_indexer,
        metrics_registry,
        metrics,
        terminate_event,
        args.network,
        args.batch_size
    )
    
    try:
        consumer.run()
    except Exception as e:
        logger.error(
            "Fatal startup error",
            error=e,
            traceback=traceback.format_exc(),
            extra={"operation": "main_startup"}
        )