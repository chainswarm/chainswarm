import json
from typing import Dict, Any, List
import clickhouse_connect
from loguru import logger
from packages.indexers.base import terminate_event
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager


class BlockStreamService:
    """
    Service for retrieving block stream data from the blockchain.
    """
    
    def __init__(self, connection_params: Dict[str, Any], network: str = None):
        """
        Initialize the BlockStreamService with ClickHouse connection parameters.
        
        Args:
            connection_params: Dictionary containing ClickHouse connection parameters
            network: Optional network identifier for parallel indexing
        """
        self.connection_params = connection_params
        self.network = network
        
        # Initialize ClickHouse client for direct database queries
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
    
    def get_blocks_by_range(self, start_height: int, end_height: int, only_with_addresses: bool = False) -> List[Dict[str, Any]]:
        """
        Get blocks within a specified height range.
        
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
    
    def get_blocks_by_address(self, address: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get blocks that contain transactions involving the specified address.
        
        Args:
            address: The blockchain address to query
            limit: Maximum number of blocks to return
            offset: Offset for pagination
            
        Returns:
            List of block dictionaries in the same format as the Kafka implementation
        """
        try:
            # First, find block heights that contain the address
            height_query = f"""
                SELECT block_height
                FROM block_stream
                WHERE hasAny(addresses, ['{address}'])
                ORDER BY block_height DESC
                LIMIT {limit} OFFSET {offset}
            """
            
            height_result = self.client.query(height_query)
            
            if not height_result.result_rows:
                return []
            
            # Get the block heights
            block_heights = [row[0] for row in height_result.result_rows]
            
            # Then get the full blocks
            blocks = []
            for height in block_heights:
                blocks.extend(self.get_blocks_by_range(height, height))
            
            return blocks
            
        except Exception as e:
            logger.error(f"Error querying blocks by address: {e}")
            raise
    
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
    
    def get_block_count(self) -> int:
        """
        Get the total number of blocks stored in the database.
        
        Returns:
            The total number of blocks
        """
        try:
            result = self.client.query("""
                SELECT COUNT() FROM block_stream
            """)
            
            if result.result_rows:
                return result.result_rows[0][0]
            return 0
        except Exception as e:
            logger.error(f"Error getting block count: {e}")
            return 0

    def get_blocks_with_addresses_by_range(self, start_height: int, end_height: int) -> List[Dict[str, Any]]:
        """
        Get blocks within a specified height range that have address interactions.
        
        Args:
            start_height: Starting block height (inclusive)
            end_height: Ending block height (inclusive)
            
        Returns:
            List of block dictionaries that have addresses
        """
        return self.get_blocks_by_range(start_height, end_height, only_with_addresses=True)

    def get_indexing_status(self) -> Dict[str, Any]:
        """
        Get the current status of the block stream indexing process.
        
        Returns:
            Dictionary containing indexing status information
        """
        if not self.network:
            raise ValueError("Network must be provided for getting indexing status")

        block_stream_manager = BlockStreamManager(self.connection_params, self.network, terminate_event)
        return block_stream_manager.get_indexing_status()

    def close(self):
        """Close the ClickHouse connection"""
        if hasattr(self, 'client'):
            self.client.close()