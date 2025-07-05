import os
import signal
import time
import argparse
import traceback
from loguru import logger
from neo4j import GraphDatabase
from typing import Dict, Any, List

from packages.indexers.base import (
    terminate_event, get_clickhouse_connection_string, get_memgraph_connection_string,
    setup_logger, create_clickhouse_database
)
from packages.indexers.base.metrics import setup_metrics, IndexerMetrics, MetricsRegistry
from packages.indexers.substrate import networks, data, Network, get_substrate_node_url
from packages.indexers.substrate.block_range_partitioner import get_partitioner
from packages.indexers.substrate.block_stream.block_stream_indexer import BlockStreamIndexer
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager
from packages.indexers.substrate.money_flow.money_flow_indexer import BaseMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_torus import TorusMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_bittensor import BittensorMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_polkadot import PolkadotMoneyFlowIndexer
from packages.indexers.substrate.node.substrate_node import SubstrateNode
from packages.indexers.substrate.assets.asset_manager import AssetManager


def get_money_flow_indexer(network: str, graph_database, indexer_metrics: IndexerMetrics, asset_manager: AssetManager):
    """
    Factory function to get the appropriate indexer based on network.
    
    Args:
        network: Network identifier (torus, bittensor, polkadot)
        graph_database: Neo4j driver instance
        indexer_metrics: IndexerMetrics instance for recording metrics
        asset_manager: AssetManager instance for managing assets
        
    Returns:
        BaseMoneyFlowIndexer: Appropriate indexer instance for the network
        
    Raises:
        ValueError: If network is invalid
    """
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return TorusMoneyFlowIndexer(graph_database, network, indexer_metrics, asset_manager)
    elif network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
        return BittensorMoneyFlowIndexer(graph_database, network, indexer_metrics, asset_manager)
    elif network == Network.POLKADOT.value:
        return PolkadotMoneyFlowIndexer(graph_database, network, indexer_metrics, asset_manager)
    else:
        raise ValueError(f"Unsupported network: {network}")


class MoneyFlowConsumer:
    def __init__(
            self,
            block_stream_manager: BlockStreamManager,
            money_flow_indexer: BaseMoneyFlowIndexer,
            metrics_registry: MetricsRegistry,
            indexer_metrics: IndexerMetrics,
            terminate_event,
            network: str,
            batch_size: int = 10
    ):
        self.block_stream_manager = block_stream_manager
        self.money_flow_indexer = money_flow_indexer
        self.metrics_registry = metrics_registry
        self.indexer_metrics = indexer_metrics
        self.terminate_event = terminate_event
        self.network = network
        self.batch_size = batch_size
        self.partitioner = get_partitioner(network)

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
        """Main processing loop"""
        try:
            # Get the last processed block height
            last_block_height = self.get_last_processed_block()
            current_height = last_block_height + 1 if last_block_height > 0 else 1
            
            # Business decision logging
            logger.info(
                "Resuming from last processed block",
                business_decision="resume_from_last_processed_block",
                reason="found_existing_processed_data" if last_block_height > 0 else "starting_from_genesis",
                extra={
                    "last_block_height": last_block_height,
                    "current_height": current_height
                }
            )
            
            # Track milestone logging (every 1,000 blocks processed)
            last_milestone_logged = 0
            milestone_interval = 1000
            
            while not self.terminate_event.is_set():
                try:
                    # Get the latest block height from the block stream
                    latest_block_height = self.block_stream_manager.get_latest_block_height()
                    
                    if current_height > latest_block_height:
                        # Only log if significantly ahead (unusual situation)
                        if current_height - latest_block_height > 10:
                            logger.warning(
                                "Consumer significantly ahead of chain tip",
                                extra={
                                    "current_height": current_height,
                                    "latest_height": latest_block_height,
                                    "blocks_ahead": current_height - latest_block_height,
                                    "possible_causes": ["chain_sync_lag", "indexer_too_fast"]
                                }
                            )
                        time.sleep(10)
                        continue
                    
                    # Calculate batch end (don't exceed latest height or batch size)
                    end_height = min(current_height + self.batch_size - 1, latest_block_height)
                    
                    batch_start_time = time.time()
                    blocks_with_addresses = self.block_stream_manager.get_blocks_by_block_height_range(current_height, end_height, only_with_addresses=True)
                    
                    # Only proceed if we weren't terminated during block fetching
                    if not self.terminate_event.is_set() and blocks_with_addresses:
                        # Process blocks
                        for block in blocks_with_addresses:
                            # Check for termination before processing each block
                            if self.terminate_event.is_set():
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
                            
                        # Log milestone progress
                        if current_height - last_milestone_logged >= milestone_interval:
                            logger.info(
                                "Money flow processing milestone reached",
                                extra={
                                    "blocks_processed": current_height - 1,
                                    "latest_chain_height": latest_block_height,
                                    "blocks_behind": max(0, latest_block_height - (current_height - 1)),
                                    "milestone_interval": milestone_interval,
                                    "batch_duration": round(batch_duration, 2) if 'batch_duration' in locals() else None
                                }
                            )
                            last_milestone_logged = current_height
                            
                    elif not self.terminate_event.is_set():
                        # Strategic warning for empty ranges
                        logger.info(
                            "No blocks with addresses found in range",
                            extra={
                                "start_height": current_height,
                                "end_height": end_height,
                                "range_size": end_height - current_height + 1,
                                "possible_causes": ["low_network_activity", "block_stream_lag"]
                            }
                        )
                        self.money_flow_indexer.update_global_state(end_height)
                        current_height = end_height + 1

                except Exception as e:
                    if self.terminate_event.is_set():
                        break
                    
                    # Record error metric
                    if self.consumer_errors_total:
                        error_type = type(e).__name__
                        labels = {'network': self.network, 'indexer': 'money_flow', 'error_type': error_type}
                        self.consumer_errors_total.labels(**labels).inc()
                    
                    # Error logging with context
                    logger.error(
                        "Block processing batch failed",
                        error=e,
                        traceback=traceback.format_exc(),
                        extra={
                            "operation": "batch_processing_loop",
                            "current_height": current_height,
                            "end_height": end_height if 'end_height' in locals() else None,
                            "batch_size": self.batch_size,
                            "latest_chain_height": latest_block_height if 'latest_block_height' in locals() else None
                        }
                    )
                    time.sleep(5)  # Brief pause before continuing
            
            logger.info("Money Flow Consumer stopped", extra={"reason": "terminate_event_received"})
            
        except KeyboardInterrupt:
            logger.info("Money Flow Consumer stopped", extra={"reason": "keyboard_interrupt"})
        except Exception as e:
            logger.error(
                "Fatal consumer error",
                error=e,
                traceback=traceback.format_exc(),
                extra={"operation": "consumer_main_loop"}
            )
        finally:
            self._cleanup()

    def process_block(self, block: Dict[str, Any]):
        """Process a single block with termination handling"""
        try:
            # Check for termination before starting
            if self.terminate_event.is_set():
                return

            block_height = block.get("block_height")
            if not block_height:
                raise ValueError("Block height is missing")

            self.money_flow_indexer.index_blocks([block])

            # Run periodic tasks
            once_per_block = 16 * 60 * 60 / self.partitioner.block_time_seconds
            if block_height % once_per_block == 0 and not self.terminate_event.is_set():
                labels = {'network': self.network, 'indexer': 'money_flow'}
                
                # Strategic logging for periodic tasks
                logger.info(
                    "Starting periodic graph analysis tasks",
                    extra={
                        "block_height": block_height,
                        "tasks": ["community_detection", "page_rank", "embeddings_update"]
                    }
                )
                
                # Run community detection with termination check
                start_time = time.time()
                self.money_flow_indexer.community_detection()
                duration = time.time() - start_time
                if self.community_detection_duration:
                    self.community_detection_duration.labels(**labels).observe(duration)

                # Run page rank with termination check
                if not self.terminate_event.is_set():
                    start_time = time.time()
                    self.money_flow_indexer.page_rank_with_community()
                    duration = time.time() - start_time
                    if self.page_rank_duration:
                        self.page_rank_duration.labels(**labels).observe(duration)

                # Update embeddings with termination check
                if not self.terminate_event.is_set():
                    start_time = time.time()
                    self.money_flow_indexer.update_embeddings()
                    duration = time.time() - start_time
                    if self.embeddings_update_duration:
                        self.embeddings_update_duration.labels(**labels).observe(duration)
                
                # Strategic completion logging
                logger.info(
                    "Completed periodic graph analysis tasks",
                    extra={
                        "block_height": block_height,
                        "total_duration": round(time.time() - start_time, 2)
                    }
                )

        except Exception as e:
            # Error logging with context
            logger.error(
                "Block processing failed",
                error=e,
                traceback=traceback.format_exc(),
                extra={
                    "operation": "process_block",
                    "block_height": block_height if 'block_height' in locals() else None
                }
            )
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
            logger.error(
                "Failed to get last processed block",
                error=e,
                traceback=traceback.format_exc(),
                extra={"operation": "get_last_processed_block"}
            )
            return 0
            
    def _cleanup(self):
        """Clean up resources"""
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
    parser = argparse.ArgumentParser(description='Money Flow Consumer using Block Stream')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of blocks to process in a batch')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to stream blocks from (polkadot, torus, or bittensor)'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-money-flow'
    setup_logger(service_name)

    logger.info(
        "Starting Money Flow Consumer",
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

    # Get connection parameters
    clickhouse_params = get_clickhouse_connection_string(args.network)
    create_clickhouse_database(clickhouse_params)
    graph_db_url, graph_db_user, graph_db_password = get_memgraph_connection_string(args.network)

    # Setup metrics
    metrics_registry = setup_metrics(service_name, start_server=True)
    indexer_metrics = IndexerMetrics(metrics_registry, args.network, 'money_flow')

    partitioner = get_partitioner(args.network)
    substrate_node = SubstrateNode(args.network, get_substrate_node_url(args.network), terminate_event)
    block_stream_indexer = BlockStreamIndexer(partitioner, indexer_metrics, clickhouse_params, args.network)

    # Initialize components
    graph_database = GraphDatabase.driver(
        graph_db_url,
        auth=(graph_db_user, graph_db_password),
        max_connection_lifetime=3600,
        connection_acquisition_timeout=60
    )
    
    # Create AssetManager and initialize native assets
    asset_manager = AssetManager(args.network, clickhouse_params)
    asset_manager.init_tables()
    logger.info(f"Initialized AssetManager and native assets for {args.network}")
    
    # Create the appropriate indexer for the network
    money_flow_indexer = get_money_flow_indexer(args.network, graph_database, indexer_metrics, asset_manager)
    money_flow_indexer.create_indexes()
    
    block_stream_manager = BlockStreamManager(block_stream_indexer, substrate_node, partitioner, clickhouse_params, args.network, terminate_event)
    
    # Create and run consumer
    consumer = MoneyFlowConsumer(
        block_stream_manager,
        money_flow_indexer,
        metrics_registry,
        indexer_metrics,
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
    finally:
        graph_database.close()