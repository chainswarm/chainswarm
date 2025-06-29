import traceback
import time
from datetime import datetime
from loguru import logger
from typing import Dict, Any, List
import clickhouse_connect
import json
import re
import os

from packages.indexers.base import IndexerMetrics
from packages.indexers.substrate.block_processor import BlockDataProcessor
from packages.indexers.substrate.block_range_partitioner import BlockRangePartitioner


class BlockStreamIndexer:
    def __init__(self, partitioner: BlockRangePartitioner, metrics: IndexerMetrics, connection_params: Dict[str, Any], network: str):

        self.partitioner = partitioner
        self.metrics = metrics
        self.client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database'],
            settings={
                'max_execution_time': connection_params.get('max_execution_time', 3600),
                'async_insert': 0,
                'wait_for_async_insert': 1
            }
        )
        self.network = network
        self._init_tables()


    def _init_tables(self):
        """Initialize tables with nested structures for transactions, addresses, and events"""
        start_time = time.time()
        logger.info(f"Creating block_stream table if not exists")
        
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        # Replace partition size placeholder
        schema_sql = schema_sql.replace('{partition_size}', str(self.partitioner.range_size))
        self.client.command(schema_sql)
        
        logger.info(f"Block stream table initialization completed in {time.time() - start_time:.2f}s")

    def index_blocks(self, blocks: List[Dict[str, Any]]):
        if not blocks:
            return

        batch_start_time = time.time()
        try:
            min_height = min(int(b['block_height']) for b in blocks)
            max_height = max(int(b['block_height']) for b in blocks)
            partition_id = self.partitioner(min_height)

            insert_start_time = time.time()
            self._insert_batch(blocks)
            insert_elapsed = time.time() - insert_start_time
            
            # Record metrics for successful batch processing
            batch_duration = time.time() - batch_start_time
            processing_rate = len(blocks) / batch_duration if batch_duration > 0 else 0

            # Record each block processed
            for block in blocks:
                block_height = int(block['block_height'])
                self.metrics.record_block_processed(block_height, batch_duration / len(blocks))

            # Record database operation
            self.metrics.record_database_operation("insert", "block_stream", insert_elapsed, True)

            # Record processing rate
            self.metrics.update_processing_rate(processing_rate)

            logger.success(
                f"Indexed batch from {min_height} to {max_height} in {batch_duration:.2f}s "
                f"({len(blocks)} blocks, {insert_elapsed:.2f}s insert time, "
                f"{processing_rate:.2f} blocks/s)"
            )

        except Exception as e:
            batch_duration = time.time() - batch_start_time
            self.metrics.record_failed_event("batch_processing_error")
                
            logger.error(
                f"Failed indexing batch starting at {min_height if 'min_height' in locals() else 'unknown'}",
                error=e,
                tracebakc=traceback.format_exc())
            raise

    def _insert_batch(self, blocks: List[Dict[str, Any]]):
        """Insert blocks with nested structures"""
        try:
            rows = []
            for block in blocks:
                processed_block = BlockDataProcessor.process_block(block)

                transaction_ids = []
                transaction_hashes = []
                transaction_signers = []
                transaction_modules = []
                transaction_functions = []
                transaction_statuses = []

                addresses = set()

                event_idxs = []
                event_extrinsic_ids = []
                event_module_ids = []
                event_ids = []
                event_attributes = []

                transaction_extrinsic_ids = []

                for idx, ext in enumerate(processed_block.get('extrinsics', [])):
                    extrinsic_id = f"{block['block_height']}-{idx}"
                    transaction_ids.append(extrinsic_id)
                    transaction_extrinsic_ids.append(extrinsic_id)

                    transaction_hashes.append('')

                    signer = ''
                    for arg in ext.get('args', []):
                        if arg.get('name') == 'dest' or arg.get('name') == 'target':
                            signer = str(arg.get('value', ''))
                            break
                    transaction_signers.append(signer)

                    transaction_modules.append(ext.get('module', ''))
                    transaction_functions.append(ext.get('call', ''))
                    transaction_statuses.append('Success' if not ext.get('error') else 'Failed')

                    if signer:
                        addresses.add(signer)

                # Process events
                for idx, evt in enumerate(processed_block.get('events', [])):
                    # Generate event_idx
                    event_idx = f"{block['block_height']}-{idx}"
                    event_idxs.append(event_idx)

                    extrinsic_index = evt.get('extrinsic_index')
                    if extrinsic_index is None:
                        extrinsic_index = 0

                    extrinsic_id = '0'
                    if len(transaction_extrinsic_ids) > 0 and 0 <= extrinsic_index < len(transaction_extrinsic_ids):
                        extrinsic_id = transaction_extrinsic_ids[extrinsic_index]
                    elif len(transaction_extrinsic_ids) > 0:
                        extrinsic_id = transaction_extrinsic_ids[0]

                    event_extrinsic_ids.append(extrinsic_id)

                    event_module_ids.append(evt.get('module', ''))
                    event_ids.append(evt.get('event', ''))
                    attrs = evt.get('attributes', [])
                    event_attributes.append(json.dumps(attrs))

                    self._extract_addresses_from_event(attrs, addresses)

                block_height_version = block.get('block_height')
                row = {
                    'block_height': int(block['block_height']),
                    'block_hash': block['block_hash'],
                    'block_timestamp': int(block['timestamp']),

                    'transactions.extrinsic_id': transaction_ids,
                    'transactions.extrinsic_hash': transaction_hashes,
                    'transactions.signer': transaction_signers,
                    'transactions.call_module': transaction_modules,
                    'transactions.call_function': transaction_functions,
                    'transactions.status': transaction_statuses,

                    'addresses': list(addresses),

                    'events.event_idx': event_idxs,
                    'events.extrinsic_id': event_extrinsic_ids,
                    'events.module_id': event_module_ids,
                    'events.event_id': event_ids,
                    'events.attributes': event_attributes,

                    '_version': block_height_version
                }

                rows.append(row)

            if rows:
                column_names = [
                    'block_height', 'block_hash', 'block_timestamp',
                    'transactions.extrinsic_id', 'transactions.extrinsic_hash', 'transactions.signer',
                    'transactions.call_module', 'transactions.call_function', 'transactions.status',
                    'addresses',
                    'events.event_idx', 'events.extrinsic_id', 'events.module_id', 'events.event_id',
                    'events.attributes',
                    '_version'
                ]

                tuple_rows = []
                for row in rows:
                    tuple_row = (
                        row['block_height'],
                        row['block_hash'],
                        row['block_timestamp'],
                        row['transactions.extrinsic_id'],
                        row['transactions.extrinsic_hash'],
                        row['transactions.signer'],
                        row['transactions.call_module'],
                        row['transactions.call_function'],
                        row['transactions.status'],
                        row['addresses'],
                        row['events.event_idx'],
                        row['events.extrinsic_id'],
                        row['events.module_id'],
                        row['events.event_id'],
                        row['events.attributes'],
                        row['_version']
                    )
                    tuple_rows.append(tuple_row)

                self.client.insert('block_stream', tuple_rows, column_names=column_names)

        except Exception as e:
            raise e

    def _extract_addresses_from_event(self, attributes: Any, addresses: set):
        """
        Extract addresses from event attributes by checking if values look like Substrate addresses.

        This method recursively checks all values in the attributes (whether dict, list, or scalar)
        and attempts to identify Substrate addresses based on format and validation.
        """
        ss58_pattern = re.compile(r'^[1-9A-HJ-NP-Za-km-z]{46,48}$')

        def check_value(value):
            """Check if a value is a potential Substrate address and add it to addresses if it is"""
            if isinstance(value, str):
                if ss58_pattern.match(value):
                    # It looks like a potential address, add it
                    addresses.add(value)
            elif isinstance(value, dict):
                # Recursively check all values in the dictionary
                for k, v in value.items():
                    check_value(v)
            elif isinstance(value, list):
                # Recursively check all items in the list
                for item in value:
                    check_value(item)

        check_value(attributes)

    def get_last_block_height(self) -> int:
        """Get the last processed block height directly from the block_stream table"""
        start_time = time.time()
        try:
            result = self.client.query('''
                                       SELECT MAX(block_height)
                                       FROM block_stream
                                       ''')

            # Record successful database operation
            duration = time.time() - start_time
            self.metrics.record_database_operation("select", "block_stream", duration, True)

            if result.result_rows and result.result_rows[0][0] is not None:
                height = result.result_rows[0][0]
                return height

            return 0
        except Exception as e:
            # Record database error
            duration = time.time() - start_time
            self.metrics.record_database_operation("select", "block_stream", duration, False)
            self.metrics.record_failed_event("database_query_error")
                
            logger.error(f"Failed to get last block height",
                         error=e,
                         traceback=traceback.format_exc())
            return 0

    def get_last_block_height_for_partition(self, partition_id: int) -> int:
        """
        Get the last processed block height for a specific partition.

        Args:
            partition_id: The partition ID to check

        Returns:
            The last processed block height for the partition, or start_height if no blocks are found
        """
        start_time = time.time()
        try:
            # Get the partition range
            start_height, end_height = self.partitioner.get_partition_range(partition_id)

            # Query the maximum block height within the partition range
            result = self.client.query(f'''
                SELECT MAX(block_height)
                FROM block_stream
                WHERE block_height >= {start_height} AND block_height <= {end_height}
            ''')

            # Record successful database operation
            duration = time.time() - start_time
            self.metrics.record_database_operation("select", "block_stream", duration, True)

            if result.result_rows and result.result_rows[0][0] is not None:
                height = result.result_rows[0][0]
                return height

            return start_height

        except Exception as e:
            duration = time.time() - start_time
            self.metrics.record_database_operation("select", "block_stream", duration, False)
            self.metrics.record_failed_event("database_query_error")
                
            logger.error(f"Failed to get last block height for partition {partition_id}",
                         error=e,
                         traceback=traceback.format_exc())

            start_height, _ = self.partitioner.get_partition_range(partition_id)
            return start_height
