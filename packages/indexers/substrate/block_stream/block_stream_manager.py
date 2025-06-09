import json
import time
import traceback
from typing import Dict, Any, List
from loguru import logger
import clickhouse_connect
from packages.indexers.substrate import get_substrate_node_url
from packages.indexers.substrate.block_range_partitioner import get_partitioner
from packages.indexers.substrate.block_stream.block_stream_indexer import BlockStreamIndexer
from packages.indexers.substrate.node.substrate_node import SubstrateNode


class BlockStreamManager:
    """
    Manager class for querying block data from ClickHouse and returning it in a format
    compatible with the Kafka implementation.
    """
    
    def __init__(self, connection_params: Dict[str, Any], network: str, terminate_event):
        """
        Initialize the BlockStreamManager with ClickHouse connection parameters.
        
        Args:
            connection_params: Dictionary containing ClickHouse connection parameters
            network: Optional network identifier for parallel indexing
            terminate_event: Event to signal termination
        """

        logger.info(f"Initializing BlockStreamManager")
        
        self.total_partition_determination_time = 0
        self.connection_params = connection_params
        self.client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database'],
            settings={
                'max_execution_time': connection_params.get('max_execution_time', 3600)
            }
        )
        self.network = network
        self.terminate_event = terminate_event
        self.partitioner = get_partitioner(network)
        self.substrate_node = SubstrateNode(network, get_substrate_node_url(network), terminate_event)
        self.block_stream_indexer = BlockStreamIndexer(connection_params, self.partitioner)

    def get_blocks_by_range(self, start_height: int, end_height: int, only_with_addresses: bool = False) -> List[Dict[str, Any]]:
        """
        Get blocks within a specified height range for indexing operations.
        
        Args:
            start_height: Starting block height (inclusive)
            end_height: Ending block height (inclusive)
            only_with_addresses: If True, only return blocks that have addresses
            
        Returns:
            List of block dictionaries in the same format as the Kafka implementation
        """
        try:
            # Query blocks with all nested data
            address_filter = "AND arrayExists(x -> x != '', addresses)" if only_with_addresses else ""
            
            query = f"""
                SELECT
                    block_height,
                    block_hash,
                    block_timestamp,
                    transactions.extrinsic_id,
                    transactions.extrinsic_hash,
                    transactions.signer,
                    transactions.call_module,
                    transactions.call_function,
                    transactions.status,
                    addresses,
                    events.event_idx,
                    events.extrinsic_id as event_extrinsic_id,
                    events.module_id,
                    events.event_id,
                    events.attributes
                FROM block_stream
                WHERE block_height >= {start_height} AND block_height <= {end_height} {address_filter}
                ORDER BY block_height
            """
            
            result = self.client.query(query)
            
            # Process results into the expected format
            blocks = []
            current_block = None
            current_block_height = None
            
            for row in result.result_rows:
                block_height = row[0]
                
                # If we're starting a new block
                if current_block_height != block_height:
                    # Add the previous block to the list if it exists
                    if current_block is not None:
                        blocks.append(current_block)
                    
                    # Start a new block
                    current_block_height = block_height
                    current_block = {
                        'block_height': block_height,
                        'block_hash': row[1],
                        'timestamp': row[2],
                        'extrinsics': [],
                        'events': []
                    }
                
                extrinsic_ids = row[3]
                extrinsic_hashes = row[4]
                signers = row[5]
                call_modules = row[6]
                call_functions = row[7]
                statuses = row[8]
                
                for i in range(len(extrinsic_ids)):
                    extrinsic = {
                        'extrinsic_id': extrinsic_ids[i],
                        'extrinsic_hash': extrinsic_hashes[i],
                        'signer': signers[i],
                        'call_module': call_modules[i],
                        'call_function': call_functions[i],
                        'status': statuses[i]
                    }
                    
                    # Only add if not already in the list
                    if not any(e['extrinsic_id'] == extrinsic['extrinsic_id'] for e in current_block['extrinsics']):
                        current_block['extrinsics'].append(extrinsic)
                
                # Process events
                event_idxs = row[10]
                event_extrinsic_ids = row[11]
                module_ids = row[12]
                event_ids = row[13]
                attributes_json = row[14]
                
                for i in range(len(event_idxs)):
                    try:
                        attributes = json.loads(attributes_json[i])
                    except json.JSONDecodeError:
                        attributes = {}
                    
                    # Parse event_idx to get the index
                    parts = event_idxs[i].split('-')
                    if len(parts) == 2:
                        event_index = int(parts[1])
                    else:
                        event_index = i
                        
                    event = {
                        'event_idx': event_idxs[i],
                        'extrinsic_id': event_extrinsic_ids[i],
                        'module_id': module_ids[i],
                        'event_id': event_ids[i],
                        'attributes': attributes,
                        'block_height': block_height,
                        'event_index': event_index
                    }
                    
                    # Only add if not already in the list
                    if not any(e['event_idx'] == event['event_idx'] for e in current_block['events']):
                        current_block['events'].append(event)
            
            # Add the last block if it exists
            if current_block is not None:
                blocks.append(current_block)
            
            return blocks
            
        except Exception as e:
            logger.error(f"Error querying blocks by range: {e}")
            raise

    def get_blocks_with_addresses_by_range(self, start_height: int, end_height: int) -> List[Dict[str, Any]]:
        """
        Get blocks within a specified height range that have address interactions.
        Used by indexing components that need to process blocks with addresses.
        
        Args:
            start_height: Starting block height (inclusive)
            end_height: Ending block height (inclusive)
            
        Returns:
            List of block dictionaries that have addresses
        """
        return self.get_blocks_by_range(start_height, end_height, only_with_addresses=True)

    def get_latest_block_height(self) -> int:
        """
        Get the latest block height stored in the database.
        
        Returns:
            The latest block height or 0 if no blocks are found
        """
        try:
            result = self.client.query("""
                SELECT MAX(block_height) FROM block_stream
            """)
            
            if result.result_rows and result.result_rows[0][0] is not None:
                return result.result_rows[0][0]
            return 0
        except Exception as e:
            logger.error(f"Error getting latest block height: {e}")
            return 0
    
    def close(self):
        """Close the ClickHouse connection"""
        if hasattr(self, 'client'):
            self.client.close()
    
    def get_partition_progress(self, partition_id: int) -> Dict[str, Any]:
        """Get the progress of a specific partition by querying the block_stream table"""

        start_height, end_height = self.partitioner.get_partition_range(partition_id)
        chain_height = self.substrate_node.get_current_block_height()
        
        # If end_height is infinity (last partition), use chain_height
        if end_height == float('inf'):
            end_height = chain_height
        
        count_result = self.client.query(f'''
            SELECT COUNT(*) as block_count
            FROM block_stream
            WHERE block_height >= {start_height} AND block_height <= {end_height}
        ''')
        
        max_result = self.client.query(f'''
            SELECT MAX(block_height) as last_indexed_height
            FROM block_stream
            WHERE block_height >= {start_height} AND block_height <= {end_height}
        ''')
        
        min_result = self.client.query(f'''
            SELECT MIN(block_height) as first_indexed_height
            FROM block_stream
            WHERE block_height >= {start_height} AND block_height <= {end_height}
        ''')
        
        # Calculate the expected number of blocks in this partition
        expected_blocks = min(end_height, chain_height) - start_height + 1
        
        # Get the actual count of blocks
        block_count = count_result.result_rows[0][0] if count_result.result_rows else 0
        
        # Get the last indexed height
        last_indexed_height = max_result.result_rows[0][0] if max_result.result_rows and max_result.result_rows[0][0] is not None else (start_height - 1)
        
        # Get the first indexed height
        first_indexed_height = min_result.result_rows[0][0] if min_result.result_rows and min_result.result_rows[0][0] is not None else None
        
        # Determine if there are gaps in the indexing
        has_gaps = False
        expected_range_count = last_indexed_height - first_indexed_height + 1
        if block_count < expected_range_count:
            has_gaps = True
        
        # A partition is only complete if:
        # 1. We have indexed all blocks up to the min of end_height and chain_height
        # 2. The block count matches the expected number of blocks
        # 3. There are no gaps
        
        # Calculate the effective end height (don't go beyond chain height)
        effective_end_height = min(end_height, chain_height)
        
        # Determine the status of the partition
        if block_count == expected_blocks and last_indexed_height == effective_end_height and not has_gaps:
            status = 'completed'
        elif block_count == 0:
            status = 'not_started'
        elif has_gaps:
            status = 'incomplete_with_gaps'
        else:
            status = 'incomplete'
        
        # Calculate remaining blocks
        remaining_blocks = effective_end_height - last_indexed_height if last_indexed_height >= start_height else effective_end_height - start_height + 1
        
        # Calculate remaining ranges if there are gaps
        remaining_ranges = []
        if has_gaps and first_indexed_height is not None and last_indexed_height is not None:
            # This is a simplified approach - for a complete solution, we would need to query
            # for the exact missing blocks and construct ranges from them
            missing_blocks = expected_range_count - block_count
            if missing_blocks > 0:
                remaining_ranges.append(f"Approximately {missing_blocks} blocks missing in range {first_indexed_height}-{last_indexed_height}")
        
        # If we haven't started indexing the end of the partition yet
        if last_indexed_height < effective_end_height:
            remaining_ranges.append(f"{last_indexed_height + 1}-{effective_end_height}")
        
        return {
            'partition_id': partition_id,
            'start_height': start_height,
            'end_height': end_height,
            'last_indexed_height': last_indexed_height,
            'first_indexed_height': first_indexed_height,
            'block_count': block_count,
            'expected_blocks': expected_blocks,
            'has_gaps': has_gaps,
            'status': status,
            'remaining_blocks': remaining_blocks,
            'remaining_ranges': remaining_ranges
        }
    
    def get_all_partition_progress(self, start_partition: int, end_partition: int) -> List[Dict[str, Any]]:
        """Get the progress of all partitions in the specified range"""
        progress = []
        
        for partition_id in range(start_partition, end_partition + 1):
            partition_progress = self.get_partition_progress(partition_id)
            
            # Log the progress of each partition
            start, end = self.partitioner.get_partition_range(partition_id)
            last_indexed = partition_progress['last_indexed_height']
            status = partition_progress['status']
            
            if status == 'completed':
                logger.info(f"[Manager] Partition {partition_id} ({start}-{end}): Completed (last indexed: {last_indexed})")
            else:
                remaining_blocks = partition_progress['remaining_blocks']
                remaining_ranges = partition_progress['remaining_ranges']
                ranges_str = ", ".join(remaining_ranges) if remaining_ranges else "None"
                logger.info(f"[Manager] Partition {partition_id} ({start}-{end}): Incomplete (last indexed: {last_indexed}, remaining: {remaining_blocks} blocks)")
                logger.info(f"[Manager] Partition {partition_id} remaining ranges: {ranges_str}")
            
            progress.append(partition_progress)
        
        return progress

    def get_indexing_status(self) -> Dict[str, Any]:
        """Get the current status of the block stream indexing process"""
        if not hasattr(self, 'partitioner') or not hasattr(self, 'substrate_node'):
            raise ValueError("Components for parallel indexing not initialized. Make sure to provide a network when initializing BlockStreamManager.")
            
        try:
            latest_height = self.get_latest_block_height()
            chain_height = self.substrate_node.get_current_block_height()
            gap = chain_height - latest_height
            
            start_partition = 0
            end_partition = self.partitioner(chain_height)
            all_progress = self.get_all_partition_progress(start_partition, end_partition)
            all_historical_complete = True
            latest_partition_id = self.partitioner(chain_height)
            
            for progress in all_progress:
                # Skip the latest partition
                if progress['partition_id'] == latest_partition_id:
                    continue
                    
                # If any historical partition is not complete, set flag to False
                if progress['status'] != 'completed':
                    all_historical_complete = False
                    break
            
            # All historical partitions are complete
            only_continuous_mode = all_historical_complete
            
            return {
                "network": self.network,
                "latest_indexed_block": latest_height,
                "current_chain_height": chain_height,
                "gap": gap,
                "all_historical_partitions_complete": all_historical_complete,
                "only_continuous_mode": only_continuous_mode,
                "partitions": all_progress
            }
        except Exception as e:
            logger.error(f"Error getting indexing status: {e}", error=e, trb=traceback.format_exc())
            return {
                "error": str(e)
            }