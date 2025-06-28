import os
import time
import argparse
import traceback
from loguru import logger
from neo4j import GraphDatabase
from typing import Dict, Any, List

from packages.indexers.base import terminate_event, get_clickhouse_connection_string, get_memgraph_connection_string, \
    setup_logger
from packages.indexers.base.metrics import setup_metrics, IndexerMetrics
from packages.indexers.substrate import networks, data, Network
from packages.indexers.substrate.block_range_partitioner import get_partitioner
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager
from packages.indexers.substrate.money_flow import populate_genesis_balances
from packages.indexers.substrate.money_flow.money_flow_indexer import BaseMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_torus import TorusMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_bittensor import BittensorMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_polkadot import PolkadotMoneyFlowIndexer


def get_money_flow_indexer(network: str, graph_database, indexer_metrics: IndexerMetrics = None):
    """
    Factory function to get the appropriate indexer based on network.
    
    Args:
        network: Network identifier (torus, bittensor, polkadot)
        graph_database: Neo4j driver instance
        indexer_metrics: Optional IndexerMetrics instance for recording metrics
        
    Returns:
        BaseMoneyFlowIndexer: Appropriate indexer instance for the network
        
    Raises:
        ValueError: If network is invalid
    """
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return TorusMoneyFlowIndexer(graph_database, network, indexer_metrics)
    elif network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
        return BittensorMoneyFlowIndexer(graph_database, network, indexer_metrics)
    elif network == Network.POLKADOT.value:
        return PolkadotMoneyFlowIndexer(graph_database, network, indexer_metrics)
    else:
        raise ValueError(f"Unsupported network: {network}")


class MoneyFlowConsumer:
    def __init__(
            self,
            block_stream_manager: BlockStreamManager,
            money_flow_indexer: BaseMoneyFlowIndexer,
            terminate_event,
            network: str,
            batch_size: int = 10
    ):
        self.block_stream_manager = block_stream_manager
        self.money_flow_indexer = money_flow_indexer
        self.terminate_event = terminate_event
        self.network = network
        self.batch_size = batch_size
        self.partitioner = get_partitioner(network)
        
        # Metrics will be set from main function
        self.metrics_registry = None
        self.indexer_metrics = None
        
        # Consumer-specific metrics (will be initialized when metrics are set)
        self.batch_processing_duration = None
        self.blocks_processed_total = None
        self.consumer_errors_total = None
        self.community_detection_duration = None
        self.page_rank_duration = None
        self.embeddings_update_duration = None
    
    def set_metrics(self, metrics_registry, indexer_metrics):
        """Set metrics after initialization"""
        self.metrics_registry = metrics_registry
        self.indexer_metrics = indexer_metrics
        
        # Initialize consumer-specific metrics
        self.batch_processing_duration = self.metrics_registry.create_histogram(
            'consumer_batch_processing_duration_seconds',
            'Time spent processing batches',
            ['network', 'indexer']
        )
        
        self.blocks_processed_total = self.metrics_registry.create_counter(
            'consumer_blocks_processed_total',
            'Total blocks processed',
            ['network', 'indexer']
        )
        
        self.consumer_errors_total = self.metrics_registry.create_counter(
            'consumer_errors_total',
            'Total consumer processing errors',
            ['network', 'indexer', 'error_type']
        )
        
        self.community_detection_duration = self.metrics_registry.create_histogram(
            'consumer_community_detection_duration_seconds',
            'Time spent on community detection',
            ['network', 'indexer']
        )
        
        self.page_rank_duration = self.metrics_registry.create_histogram(
            'consumer_page_rank_duration_seconds',
            'Time spent on page rank calculation',
            ['network', 'indexer']
        )
        
        self.embeddings_update_duration = self.metrics_registry.create_histogram(
            'consumer_embeddings_update_duration_seconds',
            'Time spent updating embeddings',
            ['network', 'indexer']
        )

    def run(self):
        """Main processing loop with improved termination handling"""
        try:
            # Get the last processed block height
            last_block_height = self.get_last_processed_block()
            current_height = last_block_height + 1 if last_block_height > 0 else 1
            
            logger.info(f"Starting money flow consumer from block {current_height}")
            
            while not self.terminate_event.is_set():
                try:
                    # Get the latest block height from the block stream
                    latest_block_height = self.block_stream_manager.get_latest_block_height()
                    
                    if current_height > latest_block_height:
                        logger.info(f"Waiting for new blocks. Current height: {current_height}, Latest height: {latest_block_height}")
                        time.sleep(10)
                        continue
                    
                    # Calculate batch end (don't exceed latest height or batch size)
                    end_height = min(current_height + self.batch_size - 1, latest_block_height)
                    
                    # Fetch blocks with address interactions from the block stream
                    logger.info(f"Fetching blocks with address interactions from {current_height} to {end_height}")

                    batch_start_time = time.time()
                    blocks_with_addresses = self.block_stream_manager.get_blocks_by_block_height_range(current_height, end_height, only_with_addresses=True)
                    
                    # Only proceed if we weren't terminated during block fetching
                    if not self.terminate_event.is_set() and blocks_with_addresses:
                        logger.info(f"Processing {len(blocks_with_addresses)} blocks with address interactions")
                        
                        # Process blocks
                        for block in blocks_with_addresses:
                            # Check for termination before processing each block
                            if self.terminate_event.is_set():
                                logger.info(f"Termination requested, stopping block processing at {block['block_height']}")
                                break
                            self.process_block(block)
                        
                        # Record batch processing metrics
                        if self.batch_processing_duration and self.blocks_processed_total:
                            batch_duration = time.time() - batch_start_time
                            labels = {'network': self.network, 'indexer': 'money_flow'}
                            self.batch_processing_duration.labels(**labels).observe(batch_duration)
                            self.blocks_processed_total.labels(**labels).inc(len(blocks_with_addresses))
                        
                        # Update current height if we weren't terminated
                        if not self.terminate_event.is_set():
                            current_height = end_height + 1
                    elif not self.terminate_event.is_set():
                        logger.warning(f"No blocks returned for range {current_height} to {end_height}")
                        self.money_flow_indexer.update_global_state(end_height)
                        current_height = end_height + 1

                except Exception as e:
                    if self.terminate_event.is_set():
                        logger.info("Termination requested, stopping processing")
                        break
                    
                    # Record error metric
                    if self.consumer_errors_total:
                        labels = {'network': self.network, 'indexer': 'money_flow', 'error_type': 'processing_error'}
                        self.consumer_errors_total.labels(**labels).inc()
                        
                    logger.error(f"Error processing blocks: {e}", error=e, trb=traceback.format_exc())
                    time.sleep(5)  # Brief pause before continuing
                    # We don't break the loop - continue processing
            
            logger.info("Termination event received, shutting down")
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Fatal error: {e}", error=e, trb=traceback.format_exc())
        finally:
            self._cleanup()

    def process_block(self, block: Dict[str, Any]):
        """Process a single block with termination handling"""
        try:
            # Check for termination before starting
            if self.terminate_event.is_set():
                logger.info(f"Skipping block {block.get('block_height')} due to termination request")
                return

            block_height = block.get("block_height")
            if not block_height:
                raise ValueError("Block height is missing")

            self.money_flow_indexer.index_blocks([block])

            # Run periodic tasks
            once_per_block = 16 * 60 * 60 / self.partitioner.block_time_seconds
            if block_height % once_per_block == 0 and not self.terminate_event.is_set():
                labels = {'network': self.network, 'indexer': 'money_flow'}
                
                # Run community detection with termination check
                start_time = time.time()
                logger.info(f"Running community detection")
                self.money_flow_indexer.community_detection()
                end_time = time.time()
                duration = end_time - start_time
                logger.success(f"Community detection took {duration} seconds")
                if self.community_detection_duration:
                    self.community_detection_duration.labels(**labels).observe(duration)

                # Run page rank with termination check
                if not self.terminate_event.is_set():
                    start_time = time.time()
                    logger.info(f"Running page rank with communities")
                    self.money_flow_indexer.page_rank_with_community()
                    end_time = time.time()
                    duration = end_time - start_time
                    logger.success(f"Page rank with communities took {duration} seconds")
                    if self.page_rank_duration:
                        self.page_rank_duration.labels(**labels).observe(duration)

                # Update embeddings with termination check
                if not self.terminate_event.is_set():
                    start_time = time.time()
                    logger.info(f"Updating embeddings")
                    self.money_flow_indexer.update_embeddings()
                    end_time = time.time()
                    duration = end_time - start_time
                    logger.success(f"Updating embeddings took {duration} seconds")
                    if self.embeddings_update_duration:
                        self.embeddings_update_duration.labels(**labels).observe(duration)

                    logger.success(f"Updating embeddings took {end_time - start_time} seconds")

        except Exception as e:
            logger.error(f"Error processing block {block_height}: {e}")
            raise
    
    def get_last_processed_block(self) -> int:
        """Get the last processed block height from the graph database"""
        try:
            with self.money_flow_indexer.graph_database.session() as session:
                result = session.run("""
                MATCH (g:GlobalState { name: "last_block_height" })
                RETURN g.block_height AS last_block_height
                """)
                record = result.single()
                if record:
                    return record["last_block_height"]
                return 0
        except Exception as e:
            logger.error(f"Error getting last processed block: {e}")
            return 0
            
    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'block_stream_manager'):
                self.block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Money Flow Consumer using Block Stream')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to stream blocks from (polkadot, torus, or bittensor)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=256,
        help='Number of blocks to process in a batch'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-money-flow'
    setup_logger(service_name)

    # Setup metrics
    metrics_registry = setup_metrics(service_name)
    indexer_metrics = IndexerMetrics(metrics_registry, args.network, 'money_flow')

    # Get connection parameters
    clickhouse_params = get_clickhouse_connection_string(args.network)
    graph_db_url, graph_db_user, graph_db_password = get_memgraph_connection_string(args.network)

    # Initialize components
    graph_database = GraphDatabase.driver(
        graph_db_url,
        auth=(graph_db_user, graph_db_password),
        max_connection_lifetime=3600,
        connection_acquisition_timeout=60
    )
    
    # Create the appropriate indexer for the network
    money_flow_indexer = get_money_flow_indexer(args.network, graph_database, indexer_metrics)
    money_flow_indexer.create_indexes()
    
    block_stream_manager = BlockStreamManager(clickhouse_params, args.network, terminate_event)
    
    # Create and run consumer
    consumer = MoneyFlowConsumer(
        block_stream_manager,
        money_flow_indexer,
        terminate_event,
        args.network,
        args.batch_size
    )
    
    # Set metrics after consumer creation
    consumer.set_metrics(metrics_registry, indexer_metrics)
    
    try:
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error", error=e, trb=traceback.format_exc())
    finally:
        graph_database.close()
        logger.info("Money flow consumer stopped")