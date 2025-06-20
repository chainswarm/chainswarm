import os
import time
import argparse
import traceback
from loguru import logger
from neo4j import GraphDatabase
from typing import Dict, Any, List

from packages.indexers.base import terminate_event, get_clickhouse_connection_string, get_memgraph_connection_string, \
    setup_logger
from packages.indexers.substrate import networks, data, Network
from packages.indexers.substrate.block_range_partitioner import get_partitioner
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager
from packages.indexers.substrate.money_flow import populate_genesis_balances
from packages.indexers.substrate.money_flow.money_flow_indexer import BaseMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_torus import TorusMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_bittensor import BittensorMoneyFlowIndexer
from packages.indexers.substrate.money_flow.money_flow_indexer_polkadot import PolkadotMoneyFlowIndexer


def get_money_flow_indexer(network: str, graph_database):
    """
    Factory function to get the appropriate indexer based on network.
    
    Args:
        network: Network identifier (torus, bittensor, polkadot)
        graph_database: Neo4j driver instance
        
    Returns:
        BaseMoneyFlowIndexer: Appropriate indexer instance for the network
        
    Raises:
        ValueError: If network is invalid
    """
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return TorusMoneyFlowIndexer(graph_database, network)
    elif network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
        return BittensorMoneyFlowIndexer(graph_database, network)
    elif network == Network.POLKADOT.value:
        return PolkadotMoneyFlowIndexer(graph_database, network)
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
                    blocks_with_addresses = self.block_stream_manager.get_blocks_with_addresses_by_range(current_height, end_height)
                    
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
            once_per_block = 4 * 60 * 60 / self.partitioner.block_time_seconds
            if block_height % once_per_block == 0 and not self.terminate_event.is_set():
                # Run community detection with termination check
                start_time = time.time()
                logger.info(f"Running community detection")
                self.money_flow_indexer.community_detection()
                end_time = time.time()
                logger.success(f"Community detection took {end_time - start_time} seconds")

                # Run page rank with termination check
                if not self.terminate_event.is_set():
                    start_time = time.time()
                    logger.info(f"Running page rank with communities")
                    self.money_flow_indexer.page_rank_with_community()
                    end_time = time.time()
                    logger.success(f"Page rank with communities took {end_time - start_time} seconds")

                """
                ADD ANY OTHER PERIODIC TASKS HERE
                https://memgraph.com/docs/advanced-algorithms/available-algorithms/degree_centrality
                https://memgraph.com/docs/advanced-algorithms/available-algorithms/betweenness_centrality
                https://memgraph.com/docs/advanced-algorithms/available-algorithms/katz_centrality
                https://memgraph.com/docs/advanced-algorithms/available-algorithms/kmeans_clustering
                """


                # Update embeddings with termination check
                if not self.terminate_event.is_set():
                    start_time = time.time()
                    logger.info(f"Updating embeddings")
                    self.money_flow_indexer.update_embeddings()
                    end_time = time.time()
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
    money_flow_indexer = get_money_flow_indexer(args.network, graph_database)
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
    
    try:
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error", error=e, trb=traceback.format_exc())
    finally:
        graph_database.close()
        logger.info("Money flow consumer stopped")