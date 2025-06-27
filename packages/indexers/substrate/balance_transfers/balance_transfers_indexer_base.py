import os
import time
from datetime import datetime
from typing import Dict, Any, List
import clickhouse_connect
from decimal import Decimal
from loguru import logger
from packages.indexers.substrate.block_range_partitioner import BlockRangePartitioner
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import get_network_asset
from packages.indexers.base import IndexerMetrics, get_metrics_registry


class BalanceTransfersIndexerBase:
    def __init__(self, connection_params: Dict[str, Any], partitioner: BlockRangePartitioner, network: str):
        """Initialize the Balance Transfers Indexer with a database connection
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
        """
        self.network = network
        self.asset = get_network_asset(network)
        self.client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database'],
            settings={
                'async_insert': 0,
                'wait_for_async_insert': 1,
                'max_execution_time': 300,
                'max_query_size': 100000
            }
        )
        self.partitioner = partitioner
        
        # Initialize metrics
        service_name = f"substrate-{network}-balance-transfers"
        metrics_registry = get_metrics_registry(service_name)
        if metrics_registry:
            self.metrics = IndexerMetrics(metrics_registry, network, "balance_transfers")
        else:
            logger.warning(f"No metrics registry found for {service_name}")
            self.metrics = None

        self._init_tables()

    def _init_tables(self):
        """Initialize tables for balance transfers from schema file"""
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')

        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            logger.debug(f"Raw schema length: {len(schema_sql)} characters")

            # Replace partition size placeholder if it exists
            schema_sql = schema_sql.replace('{PARTITION_SIZE}', str(self.partitioner.range_size))

            # Define logical chunks based on SQL sections
            chunks = []
            current_chunk = []
            lines = schema_sql.split('\n')

            for line_num, line in enumerate(lines, 1):
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith('--'):
                    continue

                current_chunk.append(line)

                # End chunk on semicolon
                if line.endswith(';'):
                    chunk_sql = ' '.join(current_chunk)
                    if chunk_sql and not chunk_sql.startswith('--'):
                        chunks.append(chunk_sql)
                        logger.debug(f"Parsed chunk {len(chunks)}: {chunk_sql[:50]}...")
                    current_chunk = []

            logger.info(f"Parsed {len(chunks)} chunks from schema")

            # Show all chunks for debugging
            for i, chunk in enumerate(chunks):
                logger.debug(f"Chunk {i + 1}: {chunk}")

            # Execute chunks
            skipped_count = 0
            created_count = 0
            error_count = 0

            for i, chunk in enumerate(chunks):
                try:
                    logger.info(f"Executing chunk {i + 1}/{len(chunks)}")
                    logger.debug(f"SQL: {chunk}")

                    result = self.client.command(chunk)
                    created_count += 1
                    logger.info(f"✓ Chunk {i + 1} executed successfully")

                except Exception as e:
                    error_str = str(e).lower()
                    logger.error(f"Error in chunk {i + 1}: {e}")
                    logger.error(f"Chunk was: {chunk}")

                    if ("already exists" in error_str or
                            "table already exists" in error_str or
                            "view already exists" in error_str or
                            "index already exists" in error_str):
                        skipped_count += 1
                        logger.warning(f"○ Chunk {i + 1} skipped (already exists)")
                    else:
                        error_count += 1
                        logger.error(f"✗ Real error in chunk {i + 1}")
                        raise

            logger.info(
                f"Schema execution complete: {created_count} created, {skipped_count} skipped, {error_count} errors")

        except FileNotFoundError:
            logger.error(f"Schema file not found: {schema_path}")
            raise
        except Exception as e:
            logger.error(f"Error initializing balance transfers tables: {e}")
            raise

    def get_latest_processed_block_height(self) -> int:
        """Get the latest block height for which balance transfers have been recorded
        
        Returns:
            The maximum block height from balance_transfers table, or 0 if no records exist
        """

        start_time = time.time()

        try:
            result = self.client.query('''
                SELECT MAX(block_height) as max_height FROM balance_transfers
            ''')
            
            # Record successful database operation
            if self.metrics:
                duration = time.time() - start_time
                self.metrics.record_database_operation("select", "balance_transfers", duration, True)

            if result.result_rows and result.result_rows[0][0] is not None:
                return result.result_rows[0][0]
            return 0
        except Exception as e:
            # Record database error
            if self.metrics:
                duration = time.time() - start_time
                self.metrics.record_database_operation("select", "balance_transfers", duration, False)
                self.metrics.record_failed_event("database_query_error")
            return 0
    
    def _validate_event_structure(self, event: Dict, required_attrs: List[str]):
        """Ensure event has expected structure"""
        if not isinstance(event, dict):
            raise ValueError(f"Invalid event format: {type(event)}")
        if 'attributes' not in event:
            raise ValueError("Event missing attributes")
        for attr in required_attrs:
            if attr not in event['attributes']:
                raise ValueError(f"Missing attribute {attr} in event: {event}")
    
    def _group_events(self, events):
        """Group events by extrinsic_idx and then by module.event_name, preserving extrinsic order"""
        grouped_by_extrinsic = {}
        extrinsic_order = []  # Maintain the order of extrinsics as they first appear
        for event in events:
            extrinsic_idx = event.get('extrinsic_idx')
            if extrinsic_idx not in grouped_by_extrinsic:
                grouped_by_extrinsic[extrinsic_idx] = {}
                extrinsic_order.append(extrinsic_idx)
            key = f"{event['module_id']}.{event['event_id']}"
            if key not in grouped_by_extrinsic[extrinsic_idx]:
                grouped_by_extrinsic[extrinsic_idx][key] = []
            grouped_by_extrinsic[extrinsic_idx][key].append(event)
        return grouped_by_extrinsic, extrinsic_order
    
    def _process_events(self, events: List[Dict]):
        """
        Process only transfer events
        """
        # Event tracking
        balance_transfers = []

        # Group events by extrinsic for proper handling
        grouped_by_extrinsic, extrinsic_order = self._group_events(events)

        for extrinsic_idx in extrinsic_order:
            events_by_type = grouped_by_extrinsic[extrinsic_idx]

            if 'System.ExtrinsicFailed' in events_by_type:
                continue

            # Handle transfers
            for event in events_by_type.get('Balances.Transfer', []):
                self._validate_event_structure(event, ['from', 'to', 'amount'])
                from_account = event['attributes']['from']
                to_account = event['attributes']['to']
                amount = convert_to_decimal_units(event['attributes']['amount'], self.network)

                # Track transfer fees
                fee_amount = Decimal(0)
                fee_events = events_by_type.get('TransactionPayment.TransactionFeePaid', [])
                for fee_event in fee_events:
                    self._validate_event_structure(fee_event, ['who', 'actual_fee'])
                    fee_account = fee_event['attributes']['who']
                    fee_tip = convert_to_decimal_units(fee_event['attributes'].get('tip', 0), self.network)
                    fee_amount = convert_to_decimal_units(fee_event['attributes']['actual_fee'], self.network) + fee_tip
                    if fee_account == from_account:
                        break

                block_version = str(event.get('block_height'))

                # Record the transfer
                balance_transfers.append((
                    event['extrinsic_id'],
                    event['event_idx'],
                    event['block_height'],
                    from_account,
                    to_account,
                    self.asset,
                    amount,
                    fee_amount,
                    block_version
                ))

        return balance_transfers
    
    def index_blocks(self, blocks: List[Dict[str, Any]], clean_old_records=False):
        """Process blocks in a batch and perform bulk inserts after all blocks are processed"""
        if not blocks:
            return

        batch_start_time = time.time()

        try:
            sorted_blocks = sorted(blocks, key=lambda x: x['block_height'])

            # Aggregation containers
            all_balance_transfers = []
            total_events_processed = 0
            total_transfers_extracted = 0

            for block in sorted_blocks:
                block_start_time = time.time()
                block_height = block['block_height']
                block_timestamp = block['timestamp']

                if block_height == 0:
                    continue

                # Process events and get transfers
                events = block.get('events', [])
                total_events_processed += len(events)
                
                balance_transfers = self._process_events(events)

                # Process network-specific events
                network_specific_transfers = self._process_network_specific_events(events)
                
                # Combine the results
                if network_specific_transfers:
                    balance_transfers.extend(network_specific_transfers)

                # Add timestamp to balance transfers
                updated_balance_transfers = []
                for transfer in balance_transfers:
                    updated_transfer = (
                        transfer[0],   # extrinsic_id
                        transfer[1],   # event_idx
                        transfer[2],   # block_height
                        block_timestamp,  # Add timestamp
                        transfer[3],   # from_address
                        transfer[4],   # to_address
                        transfer[5],   # asset
                        transfer[6],   # amount
                        transfer[7],   # fee
                        transfer[8]    # version
                    )
                    updated_balance_transfers.append(updated_transfer)

                # Aggregate transfers
                all_balance_transfers.extend(updated_balance_transfers)
                total_transfers_extracted += len(updated_balance_transfers)

                # Record block processing metrics
                if self.metrics:
                    block_processing_time = time.time() - block_start_time
                    self.metrics.record_block_processed(block_height, block_processing_time)

            # Bulk insert transfers
            insert_start_time = time.time()
            if all_balance_transfers:
                self.client.insert('balance_transfers',
                                  all_balance_transfers,
                                  column_names=['extrinsic_id', 'event_idx', 'block_height', 'block_timestamp',
                                               'from_address', 'to_address', 'asset', 'amount', 'fee', '_version'],
                                  settings={'async_insert': 0})

                # Record database insert metrics
                if self.metrics:
                    insert_duration = time.time() - insert_start_time
                    self.metrics.record_database_operation("insert", "balance_transfers", insert_duration, True)

            # Record batch metrics
            if self.metrics:
                batch_duration = time.time() - batch_start_time
                processing_rate = len(sorted_blocks) / batch_duration if batch_duration > 0 else 0
                self.metrics.update_processing_rate(processing_rate)
                
                # Record event processing metrics
                self.metrics.record_event_processed("total_events", total_events_processed)
                self.metrics.record_event_processed("balance_transfers", total_transfers_extracted)

            logger.success(f"Bulk inserted {len(sorted_blocks)} blocks with {len(all_balance_transfers)} transfers")

        except Exception as e:
            # Record error metrics
            if self.metrics:
                batch_duration = time.time() - batch_start_time
                self.metrics.record_failed_event("batch_processing_error")
                
            logger.success(f"Bulk inserted {len(sorted_blocks)} blocks with {len(all_balance_transfers)} transfers")
            raise
    
    def _process_network_specific_events(self, events: List[Dict]):
        """
        Process network-specific events. To be overridden by subclasses.
        
        Args:
            events: List of events to process
            
        Returns:
            List of balance transfers in the format:
            (extrinsic_id, event_idx, block_height, from_account, to_account, asset, amount, fee_amount, version)
        """
        # Base implementation does nothing
        return []
    
    def close(self):
        """Close the ClickHouse connection"""
        if hasattr(self, 'client'):
            self.client.close()