import argparse
import traceback
import time
import signal
from loguru import logger
from packages.indexers.base import (
    get_clickhouse_connection_string, create_clickhouse_database, terminate_event,
    setup_metrics, get_metrics_registry, setup_enhanced_logger, ErrorContextManager,
    log_service_start, log_service_stop, classify_error
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
            terminate_event,
            network: str,
            batch_size: int = 10,
            service_name: str = None,
            start_height: int = None,
            end_height: int = None,
            sleep_time: int = 10
    ):
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
        
        # Setup enhanced logging
        setup_enhanced_logger(self.service_name)
        self.error_ctx = ErrorContextManager(self.service_name)
        
        # Get metrics registry for additional consumer-level metrics
        service_name = f"substrate-{network}-block-stream"
        self.metrics_registry = get_metrics_registry(service_name)
        # REMOVED: Verbose metrics logging - not needed
        if self.metrics_registry:
            # Consumer-specific metrics
            self.batch_processing_duration = self.metrics_registry.create_histogram(
                'consumer_batch_processing_duration_seconds',
                'Time spent processing batches',
                ['network', 'indexer']
            )
            self.blocks_fetched_total = self.metrics_registry.create_counter(
                'consumer_blocks_fetched_total',
                'Total blocks fetched from blockchain',
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
            self.blocks_behind_latest = self.metrics_registry.create_gauge(
                'consumer_blocks_behind_latest',
                'Number of blocks behind the latest block',
                ['network', 'indexer']
            )
        # REMOVED: Warning log - metrics handle availability
        
        # Log service startup
        log_service_start(
            self.service_name,
            network=network,
            batch_size=batch_size,
            start_height=start_height,
            end_height=end_height,
            mode="partition" if start_height and end_height else "continuous"
        )

    def run(self):
        """Main processing loop with improved error handling and termination awareness"""

        # Check terminate_event immediately
        if self.terminate_event.is_set():
            return
            
        try:
            # If start_height is provided, use it; otherwise, get the last processed block height
            if self.start_height is not None:
                current_height = self.start_height
                # ENHANCED: Business decision logging
                self.error_ctx.log_business_decision(
                    "use_provided_start_height",
                    "partition_mode_or_manual_override",
                    start_height=current_height,
                    end_height=self.end_height
                )
            else:
                # Get the last processed block height directly from the block_stream table
                last_block_height = self.block_stream_indexer.get_last_block_height()
                current_height = last_block_height + 1 if last_block_height > 0 else 1
                # ENHANCED: Business decision logging
                self.error_ctx.log_business_decision(
                    "resume_from_last_processed_block",
                    "continuous_mode_startup",
                    last_block_height=last_block_height,
                    current_height=current_height
                )

            # Check terminate_event at the start of each iteration
            while not self.terminate_event.is_set():
                # Check terminate_event again
                if self.terminate_event.is_set():
                    return  # Return instead of break to exit immediately
                try:
                    chain_height = self.substrate_node.get_current_block_height()

                    # If we've reached the end_height and it's not None (continuous mode), we're done
                    if self.end_height is not None and current_height > self.end_height:
                        # ENHANCED: Business decision logging
                        self.error_ctx.log_business_decision(
                            "reached_end_height",
                            "partition_mode_completion",
                            end_height=self.end_height,
                            current_height=current_height
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
                    if self.metrics_registry and blocks:
                        self.blocks_fetched_total.labels(network=self.network, indexer="block_stream").inc(len(blocks))

                    if not self.terminate_event.is_set() and blocks:
                        if self.terminate_event.is_set():
                            return
                            
                        # Index blocks
                        try:
                            self.block_stream_indexer.index_blocks(blocks)
                            
                            # Record batch processing metrics
                            if self.metrics_registry:
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
                            
                            # REMOVED: Verbose success logging - metrics handle this
                            
                            if self.terminate_event.is_set():
                                return
                                
                            current_height = end_height + 1
                        except Exception as e:
                            # Record indexing error metric
                            if self.metrics_registry:
                                self.consumer_errors_total.labels(
                                    network=self.network,
                                    indexer="block_stream",
                                    error_type=classify_error(e)
                                ).inc()
                            
                            # ENHANCED: Error logging with context
                            self.error_ctx.log_error(
                                "Block indexing failed",
                                error=e,
                                operation="index_blocks",
                                current_height=current_height,
                                end_height=end_height,
                                blocks_count=len(blocks) if blocks else 0,
                                error_category=classify_error(e)
                            )
                            time.sleep(5)
                    elif not self.terminate_event.is_set():
                        # Record empty batch metric
                        if self.metrics_registry:
                            self.empty_batches_total.labels(network=self.network, indexer="block_stream").inc()
                        
                        # ENHANCED: Strategic warning for empty ranges
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
                    
                    # Record processing error metric
                    if self.metrics_registry:
                        self.consumer_errors_total.labels(
                            network=self.network,
                            indexer="block_stream",
                            error_type=classify_error(e)
                        ).inc()
                    
                    # ENHANCED: Error logging with context
                    self.error_ctx.log_error(
                        "Block processing failed",
                        error=e,
                        operation="block_processing_loop",
                        current_height=current_height if 'current_height' in locals() else None,
                        chain_height=chain_height if 'chain_height' in locals() else None,
                        error_category=classify_error(e)
                    )
                    for _ in range(5):
                        if self.terminate_event.is_set():
                            return
                        time.sleep(1)

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
        
    setup_enhanced_logger(service_name)
    
    # Setup metrics server for Prometheus to scrape
    metrics_registry = setup_metrics(service_name, start_server=True)
    # REMOVED: Verbose metrics server logging - not needed

    def signal_handler(sig, frame):
        # ENHANCED: Strategic signal logging
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
    
    partitioner = get_partitioner(args.network)
    substrate_node = SubstrateNode(args.network, get_substrate_node_url(args.network), terminate_event)
    block_stream_indexer = BlockStreamIndexer(connection_params, partitioner, args.network)
    
    if args.partition is not None and args.start_height is None:
        chain_height = substrate_node.get_current_block_height()
        start_height, end_height = partitioner.get_partition_range(args.partition)
        last_indexed_height = block_stream_indexer.get_last_block_height_for_partition(args.partition)
        
        if last_indexed_height > start_height:
            start_height = last_indexed_height + 1
            # ENHANCED: Business decision logging
            error_ctx = ErrorContextManager(service_name)
            error_ctx.log_business_decision(
                "resume_partition_indexing",
                "found_existing_indexed_blocks",
                partition_id=args.partition,
                last_indexed_height=last_indexed_height,
                resume_from_height=start_height,
                partition_end=end_height
            )
        else:
            # ENHANCED: Business decision logging
            error_ctx = ErrorContextManager(service_name)
            error_ctx.log_business_decision(
                "start_partition_from_beginning",
                "no_existing_indexed_blocks",
                partition_id=args.partition,
                start_height=start_height,
                end_height=end_height
            )
            
        args.start_height = start_height
        args.end_height = end_height
        
        # REMOVED: Verbose partition range logging - business decision logs handle this
    
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
        # REMOVED: Verbose startup logging - service lifecycle logs handle this
        consumer.run()
        
        # Log completion for partition mode
        if args.end_height is not None:
            error_ctx = ErrorContextManager(service_name)
            error_ctx.log_business_decision(
                "partition_indexing_completed",
                "reached_end_height",
                start_height=args.start_height,
                end_height=args.end_height,
                partition_id=args.partition
            )
    except Exception as e:
        error_ctx = ErrorContextManager(service_name)
        error_ctx.log_error(
            "Fatal startup error",
            error=e,
            operation="main_startup",
            error_category=classify_error(e)
        )