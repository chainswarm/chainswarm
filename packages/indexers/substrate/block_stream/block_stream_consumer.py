import argparse
import traceback
import time
import signal
from loguru import logger
from packages.indexers.base import (
    get_clickhouse_connection_string, create_clickhouse_database, terminate_event,
    setup_metrics, get_metrics_registry, setup_logger,IndexerMetrics,
)
from packages.indexers.substrate import networks, get_substrate_node_url
from packages.indexers.substrate.block_range_partitioner import get_partitioner
from packages.indexers.substrate.block_stream.block_stream_indexer import BlockStreamIndexer
from packages.indexers.substrate.node.substrate_node import SubstrateNode


class BlockStreamConsumer:
    def __init__(
            self,
            substrate_node: SubstrateNode,
            block_stream_indexer: BlockStreamIndexer,
            metrics_registry,
            indexer_metrics,
            terminate_event,
            network: str,
            batch_size: int = 10,
            start_height: int = None,
            end_height: int = None,
            sleep_time: int = 10
    ):
        self.substrate_node = substrate_node
        self.block_stream_indexer = block_stream_indexer
        self.metrics_registry = metrics_registry
        self.indexer_metrics = indexer_metrics
        self.terminate_event = terminate_event
        self.network = network
        self.batch_size = batch_size
        self.start_height = start_height
        self.end_height = end_height
        self.sleep_time = sleep_time
        self.partitioner = block_stream_indexer.partitioner if hasattr(block_stream_indexer, 'partitioner') else None

        self.batch_processing_duration = metrics_registry.create_histogram(
            'consumer_batch_processing_duration_seconds',
            'Time spent processing batches',
            ['network', 'indexer']
        )
        self.blocks_fetched_total = metrics_registry.create_counter(
            'consumer_blocks_fetched_total',
            'Total blocks fetched from blockchain',
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
            # If start_height is provided, use it; otherwise, get the last processed block height
            if self.start_height is not None:
                current_height = self.start_height
                logger.info(
                    "Starting consumer from provided start height",
                    business_decision="use_provided_start_height",
                    reason="manual_override_or_partition_mode",
                    extra={
                        "start_height": current_height,
                        "end_height": self.end_height,
                    }
                )
            else:
                # Get the last processed block height directly from the block_stream table
                last_block_height = self.block_stream_indexer.get_last_block_height()
                current_height = last_block_height + 1 if last_block_height > 0 else 1
                logger.info(
                    "Starting consumer from last processed block height",
                    business_decision="resume_from_last_processed_block",
                    reason="continuous_mode_startup",
                    extra={
                        "last_block_height": last_block_height,
                        "current_height": current_height,
                    }
                )

            # Check terminate_event at the start of each iteration
            while not self.terminate_event.is_set():
                if self.terminate_event.is_set():
                    return

                try:
                    chain_height = self.substrate_node.get_current_block_height()
                    if self.end_height is not None and current_height > self.end_height:
                        logger.info(
                            "Reached end height, stopping consumer",
                            business_decision="reached_end_height",
                            reason="partition_mode_completion",
                            extra={
                                "current_height": current_height,
                                "end_height": self.end_height
                            }
                        )
                        break

                    if current_height > chain_height:
                        # Only log if significantly ahead (unusual situation)
                        if current_height - chain_height > 10:
                            logger.warning(
                                "Consumer significantly ahead of chain tip",
                                extra={
                                    "current_height": current_height,
                                    "chain_height": chain_height,
                                    "blocks_ahead": current_height - chain_height,
                                    "possible_causes": ["chain_sync_lag", "indexer_too_fast"]
                                }
                            )
                        for _ in range(self.sleep_time):
                            if self.terminate_event.is_set():
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
                        return
                        
                    batch_start_time = time.time()
                    blocks = self.substrate_node.get_blocks_by_height_range(current_height, end_height)

                    # Record blocks fetched metric
                    if blocks:
                        self.blocks_fetched_total.labels(network=self.network, indexer="block_stream").inc(len(blocks))

                    if not self.terminate_event.is_set() and blocks:
                        if self.terminate_event.is_set():
                            return
                            
                        # Index blocks
                        try:
                            self.block_stream_indexer.index_blocks(blocks)
                            
                            # Record batch processing metrics
                            batch_duration = time.time() - batch_start_time
                            self.batch_processing_duration.labels(
                                network=self.network,
                                indexer="block_stream"
                            ).observe(batch_duration)

                            # Update blocks behind metric
                            blocks_behind = max(0, chain_height - end_height)
                            self.blocks_behind_latest.labels(
                                network=self.network,
                                indexer="block_stream"
                            ).set(blocks_behind)

                            if self.terminate_event.is_set():
                                return
                                
                            current_height = end_height + 1

                        except Exception as e:
                            self.consumer_errors_total.labels(
                                network=self.network,
                                indexer="block_stream"
                            ).inc()

                            logger.error(
                                "Error indexing blocks",
                                error=e,
                                traceback=traceback.format_exc(),
                                extra={
                                    "current_height": current_height,
                                    "end_height": end_height,
                                    "blocks_count": len(blocks),
                                    "chain_height": chain_height
                                }
                            )

                            time.sleep(5)
                    elif not self.terminate_event.is_set():
                        self.empty_batches_total.labels(network=self.network, indexer="block_stream").inc()
                        logger.warning(
                            "No blocks returned from blockchain",
                            extra={
                                "start_height": current_height,
                                "end_height": end_height,
                                "range_size": end_height - current_height + 1,
                                "chain_height": chain_height if 'chain_height' in locals() else None,
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
                        indexer="block_stream"
                    ).inc()

                    logger.error(
                        "Error processing block range",
                        error=e,
                        traceback=traceback.format_exc(),
                        extra={
                            "current_height": current_height if 'current_height' in locals() else None,
                            "chain_height": chain_height if 'chain_height' in locals() else None,
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

    logger.info(
        "Starting Block Stream Consumer",
        extra={
            "service": service_name,
            "network": args.network,
            "batch_size": args.batch_size,
            "start_height": args.start_height,
            "end_height": args.end_height,
            "partition": args.partition,
            "sleep_time": args.sleep_time
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
    metrics = IndexerMetrics(metrics_registry, args.network, "block_stream")

    partitioner = get_partitioner(args.network)
    substrate_node = SubstrateNode(args.network, get_substrate_node_url(args.network), terminate_event)
    block_stream_indexer = BlockStreamIndexer(connection_params, partitioner, metrics, args.network)

    if args.partition is not None and args.start_height is None:
        chain_height = substrate_node.get_current_block_height()
        start_height, end_height = partitioner.get_partition_range(args.partition)
        last_indexed_height = block_stream_indexer.get_last_block_height_for_partition(args.partition)
        
        if last_indexed_height > start_height:
            start_height = last_indexed_height + 1
            logger.info(
                "Resuming partition indexing from last indexed block",
                extra={
                    "partition_id": args.partition,
                    "last_indexed_height": last_indexed_height,
                    "start_height": start_height,
                    "end_height": end_height
                }
            )
        else:
            logger.info(
                "Starting partition indexing from beginning",
                extra={
                    "partition_id": args.partition,
                    "start_height": start_height,
                    "end_height": end_height
                }
            )

        args.start_height = start_height
        args.end_height = end_height

    consumer = BlockStreamConsumer(
        substrate_node,
        block_stream_indexer,
        metrics_registry,
        metrics,
        terminate_event,
        args.network,
        args.batch_size,
        args.start_height,
        args.end_height,
        args.sleep_time
    )
    
    try:
        consumer.run()

        if args.end_height is not None:
            log_business_decision(
                "partition_indexing_completed",
                "reached_end_height",
                start_height=args.start_height,
                end_height=args.end_height,
                partition_id=args.partition
            )
    except Exception as e:
        log_error_with_context(
            "Fatal startup error",
            e,
            operation="main_startup"
        )