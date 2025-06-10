import argparse
import traceback
import time
import signal
from loguru import logger
from packages.indexers.base import setup_logger, get_clickhouse_connection_string, create_clickhouse_database, \
    terminate_event
from packages.indexers.substrate import networks, get_substrate_node_url
from packages.indexers.substrate.block_range_partitioner import get_partitioner
from packages.indexers.substrate.block_stream.block_stream_indexer import BlockStreamIndexer
from packages.indexers.substrate.node.substrate_node import SubstrateNode


class BlockStreamConsumer:
    def __init__(
            self,
            substrate_node: SubstrateNode,
            block_stream_indexer: BlockStreamIndexer,
            terminate_event,
            network: str,
            batch_size: int = 10,
            service_name: str = None,
            start_height: int = None,
            end_height: int = None,
            sleep_time: int = 10
    ):
        init_start_time = time.time()
        logger.info(f"Initializing BlockStreamConsumer for network: {network}")
        
        self.substrate_node = substrate_node
        self.block_stream_indexer = block_stream_indexer
        self.terminate_event = terminate_event
        self.network = network
        self.batch_size = batch_size
        self.service_name = service_name or f'substrate-{network}-block-stream'
        self.start_height = start_height
        self.end_height = end_height
        self.sleep_time = sleep_time
        self.partitioner = block_stream_indexer.partitioner if hasattr(block_stream_indexer, 'partitioner') else None

    def run(self):
        """Main processing loop with improved error handling and termination awareness"""

        # Check terminate_event immediately
        if self.terminate_event.is_set():
            return
            
        try:
            # If start_height is provided, use it; otherwise, get the last processed block height
            if self.start_height is not None:
                current_height = self.start_height
                logger.info(f"Using provided start height: {current_height}")
            else:
                # Get the last processed block height directly from the block_stream table
                last_block_height = self.block_stream_indexer.get_last_block_height()
                current_height = last_block_height + 1 if last_block_height > 0 else 1
                logger.info(f"Starting block stream from block {current_height} in continuous mode")

            # Check terminate_event at the start of each iteration
            while not self.terminate_event.is_set():
                # Check terminate_event again
                if self.terminate_event.is_set():
                    logger.info("Termination event set in main loop, exiting")
                    return  # Return instead of break to exit immediately
                try:
                    chain_height = self.substrate_node.get_current_block_height()

                    # If we've reached the end_height and it's not None (continuous mode), we're done
                    if self.end_height is not None and current_height > self.end_height:
                        logger.info(f"Reached end height {self.end_height}, stopping")
                        break

                    if current_height > chain_height:
                        logger.info(f"Waiting for new blocks. Current height: {current_height}, Chain height: {chain_height}")
                        for _ in range(self.sleep_time):
                            if self.terminate_event.is_set():
                                logger.info(f"Termination requested while waiting for new blocks")
                                return
                            time.sleep(1)
                        continue
                    
                    # Calculate batch end (don't exceed chain height, end_height, or batch size)
                    if self.end_height is not None:
                        end_height = min(current_height + self.batch_size - 1, chain_height, self.end_height)
                    else:
                        # Continuous mode - just don't exceed chain height
                        end_height = min(current_height + self.batch_size - 1, chain_height)
                    
                    if self.terminate_event.is_set():
                        logger.info(f"Termination detected before fetching blocks, exiting")
                        return
                        
                    blocks = self.substrate_node.get_blocks_by_height_range(current_height, end_height)

                    if not self.terminate_event.is_set() and blocks:
                        if self.terminate_event.is_set():
                            logger.info(f"Termination detected after fetching blocks but before indexing, exiting")
                            return
                            
                        # Index blocks
                        try:
                            self.block_stream_indexer.index_blocks(blocks)
                            
                            if self.partitioner:
                                partition_id = self.partitioner(current_height)
                                partition_start, partition_end = self.partitioner.get_partition_range(partition_id)
                                
                                if self.end_height is None:
                                    logger.info(f"Indexed {len(blocks)} blocks in range of {current_height} to {end_height} (continuous mode, chain height: {chain_height})")
                                else:
                                    logger.info(f"Indexed {len(blocks)} blocks in range of {current_height} to {end_height} (partition {partition_id}: {partition_start}-{partition_end})")
                            else:
                                logger.info(f"Indexed {len(blocks)} blocks in range of {current_height} to {end_height}")

                            if self.terminate_event.is_set():
                                logger.info(f"Termination detected after indexing blocks, exiting")
                                return
                                
                            current_height = end_height + 1
                        except Exception as e:
                            logger.error(f"Error indexing blocks: {e}", error=e, trb=traceback.format_exc())
                            time.sleep(5)
                    elif not self.terminate_event.is_set():
                        logger.warning(f"No blocks returned for range {current_height} to {end_height}")
                        for _ in range(5):
                            if self.terminate_event.is_set():
                                logger.info(f"Termination requested after no blocks returned")
                                return
                            time.sleep(1)
                
                except Exception as e:
                    if self.terminate_event.is_set():
                        logger.info("Termination requested during exception handling, stopping processing")
                        return
                        
                    logger.error(f"Error processing blocks: {e}", error=e, trb=traceback.format_exc())
                    for _ in range(5):
                        if self.terminate_event.is_set():
                            logger.info(f"Termination requested after error")
                            return
                        time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Fatal error: {e}", error=e, trb=traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Block Stream Consumer')
    parser.add_argument('--batch-size', type=int, default=16, help='Number of blocks to process in a batch')
    parser.add_argument('--start-height', type=int, help='Starting block height for this consumer')
    parser.add_argument('--end-height', type=int, help='Ending block height for this consumer')
    parser.add_argument('--partition', type=int, help='Partition ID for this consumer')
    parser.add_argument('--sleep-time', type=int, default=10, help='Sleep time in seconds when waiting for new blocks')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to stream blocks from (polkadot, torus, or bittensor)'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-block-stream'
    if args.partition is not None:
        service_name = f"{service_name}-partition-{args.partition}"
        
    setup_logger(service_name)

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, setting terminate event and waiting for graceful shutdown")
        terminate_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    connection_params = get_clickhouse_connection_string(args.network)
    create_clickhouse_database(connection_params)
    
    partitioner = get_partitioner(args.network)
    substrate_node = SubstrateNode(args.network, get_substrate_node_url(args.network), terminate_event)
    block_stream_indexer = BlockStreamIndexer(connection_params, partitioner)
    
    if args.partition is not None and args.start_height is None:
        chain_height = substrate_node.get_current_block_height()
        start_height, end_height = partitioner.get_partition_range(args.partition)
        last_indexed_height = block_stream_indexer.get_last_block_height_for_partition(args.partition)
        
        if last_indexed_height > start_height:
            start_height = last_indexed_height + 1
            logger.info(f"Resuming indexing for partition {args.partition}, blocks 1-{last_indexed_height} already exist, continuing from block {start_height}")
        else:
            logger.info(f"No existing blocks found in partition {args.partition}, starting from beginning: {start_height}")
            
        args.start_height = start_height
        args.end_height = end_height
        
        logger.info(f"Using partition {args.partition} range: {start_height} to {end_height}")
    
    consumer = BlockStreamConsumer(
        substrate_node,
        block_stream_indexer,
        terminate_event,
        args.network,
        args.batch_size,
        service_name,
        args.start_height,
        args.end_height,
        args.sleep_time
    )
    
    try:
        logger.info("Starting block stream indexing")
        start_time = time.time()
        consumer.run()
        elapsed = time.time() - start_time
        logger.info(f"Indexing completed in {elapsed:.2f}s")
    except Exception as e:
        logger.error(f"Fatal error", error=e, trb=traceback.format_exc())