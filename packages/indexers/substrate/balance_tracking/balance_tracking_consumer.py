import argparse
import traceback
import time
import os
from loguru import logger
from typing import Dict, Set
from decimal import Decimal
from packages.indexers.base import setup_logger, get_clickhouse_connection_string, create_clickhouse_database, \
    terminate_event
from packages.indexers.substrate import get_substrate_node_url, networks, data, Network
from packages.indexers.substrate.balance_tracking.balance_tracking_indexer import BalanceTrackingIndexer
from packages.indexers.substrate.balance_tracking.balance_tracking_indexer_torus import TorusBalanceTrackingIndexer
from packages.indexers.substrate.balance_tracking.balance_tracking_indexer_bittensor import BittensorBalanceTrackingIndexer
from packages.indexers.substrate.balance_tracking.balance_tracking_indexer_polkadot import PolkadotBalanceTrackingIndexer
from packages.indexers.substrate.block_range_partitioner import get_partitioner
from packages.indexers.substrate.block_stream.block_stream_manager import BlockStreamManager
from packages.indexers.substrate.node.substrate_node import SubstrateNode


def get_balance_tracking_indexer(network: str, connection_params, partitioner):
    """
    Factory function to get the appropriate indexer based on network.
    
    Args:
        network: Network identifier (torus, bittensor, polkadot)
        connection_params: ClickHouse connection parameters
        partitioner: BlockRangePartitioner instance
        
    Returns:
        BalanceTrackingIndexer: Appropriate indexer instance for the network
        
    Raises:
        ValueError: If network is invalid
    """
    if network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
        return TorusBalanceTrackingIndexer(connection_params, partitioner, network)
    elif network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
        return BittensorBalanceTrackingIndexer(connection_params, partitioner, network)
    elif network == Network.POLKADOT.value:
        return PolkadotBalanceTrackingIndexer(connection_params, partitioner, network)
    else:
        raise ValueError(f"Unsupported network: {network}")


class BalanceTrackingConsumer:
    def __init__(
            self,
            block_stream_manager: BlockStreamManager,
            substrate_node: SubstrateNode,
            balance_tracking_indexer: BalanceTrackingIndexer,
            terminate_event,
            network: str,
            batch_size: int = 100
    ):
        """Initialize the Balance Tracking Consumer
        
        Args:
            block_stream_manager: BlockStreamManager instance for querying block data
            substrate_node: SubstrateNode instance for querying blockchain data
            balance_tracking_indexer: BalanceTrackingIndexer instance for storing balance changes
            terminate_event: Event to signal termination
            network: Network identifier (e.g., 'torus', 'polkadot')
            batch_size: Number of blocks to process in a single batch and addresses to query in a single blockchain request
        """
        self.block_stream_manager = block_stream_manager
        self.substrate_node = substrate_node
        self.balance_tracking_indexer = balance_tracking_indexer
        self.terminate_event = terminate_event
        self.network = network
        self.batch_size = batch_size

    def run(self):
        """Main processing loop with improved termination handling and batch processing"""
        try:
            last_processed_height = self.balance_tracking_indexer.get_latest_processed_block_height()
            
            if last_processed_height > 0:
                start_height = last_processed_height + 1
            else:
                start_height = 1

            # Use the batch_size from constructor
            logger.info(f"Starting balance tracking consumer from block {start_height}")
            logger.info(f"Batch size for block processing and blockchain queries: {self.batch_size}")
            
            while not self.terminate_event.is_set():
                try:
                    latest_block_height = self.block_stream_manager.get_latest_block_height()
                    
                    if start_height > latest_block_height:
                        logger.info(f"Waiting for new blocks. Current target: {start_height}, Latest height: {latest_block_height}")
                        time.sleep(10)
                        continue
                    
                    # Calculate end height for the batch (don't exceed latest block height)
                    end_height = min(start_height + self.batch_size - 1, latest_block_height)
                    
                    logger.info(f"Fetching blocks from {start_height} to {end_height}")
                    
                    # Get blocks with address interactions in batch
                    blocks_with_addresses = self.block_stream_manager.get_blocks_with_addresses_by_range(start_height, end_height)
                    
                    if not blocks_with_addresses:
                        logger.warning(f"No blocks with address interactions returned for range {start_height}-{end_height}")
                        # Move to the next batch even if no blocks were found
                        start_height = end_height + 1
                        continue

                    # Process each block with addresses
                    for block in blocks_with_addresses:
                        block_height = block['block_height']
                        logger.info(f"Processing block {block_height} with {len(block.get('addresses', []))} addresses")
                        self._process_block(block_height)
                    
                    # Update start height to the next block after this batch
                    start_height = end_height + 1
                    
                except Exception as e:
                    if self.terminate_event.is_set():
                        logger.info("Termination requested, stopping processing")
                        break
                        
                    logger.error(f"Error processing block {start_height}: {e}", error=e, trb=traceback.format_exc())
                    time.sleep(5)  # Brief pause before continuing
                    # We don't break the loop - continue processing
                    
            logger.info("Termination event received, shutting down")
                    
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Fatal error: {e}", error=e, trb=traceback.format_exc())
        finally:
            self._cleanup()
            logger.info("Enhanced balance tracking consumer stopped")

    def _get_affected_addresses(self, block_height: int) -> Set[str]:
        """Get all addresses affected by transactions in the given block
        
        Args:
            block_height: Block height
            
        Returns:
            Set of addresses
        """
        try:
            # Query the block_stream table to find all addresses involved in transactions
            query = f"""
                SELECT DISTINCT arrayJoin(addresses) as address
                FROM block_stream
                WHERE block_height = {block_height}
            """
            
            result = self.block_stream_manager.client.query(query)
            addresses = {row[0] for row in result.result_rows}
            
            logger.info(f"Found {len(addresses)} affected addresses in block {block_height}")
            return addresses
            
        except Exception as e:
            logger.error(f"Error getting affected addresses: {e}")
            return set()

    def _query_blockchain_balances(self, addresses: Set[str], block_hash: str) -> Dict[str, Dict[str, int]]:
        """Query balances for multiple addresses from the blockchain
        
        Args:
            addresses: Set of addresses to query
            block_hash: Block hash to query at
            
        Returns:
            Dictionary mapping addresses to their balance information
        """
        result = {}
        address_list = list(addresses)
        
        for i in range(0, len(address_list), self.batch_size):
            # Check for termination before processing batch
            if self.terminate_event.is_set():
                logger.info("Termination requested during balance query")
                break
                
            batch = address_list[i:i + self.batch_size]
            logger.info(f"Querying balances for {len(batch)} addresses (batch {i//self.batch_size + 1}/{(len(address_list) + self.batch_size - 1)//self.batch_size})")
            
            for address in batch:
                # The get_balances_at_block method now has infinite retry built-in
                # It will only return if successful or if termination is requested
                try:
                    account_data = self.substrate_node.get_balances_at_block(
                        block_hash=block_hash,
                        params=[address]
                    )
                    
                    if account_data:
                        free = int(account_data.get('data', {}).get('free', 0))
                        reserved = int(account_data.get('data', {}).get('reserved', 0))
                        staked = int(account_data.get('data', {}).get('staked', 0))
                        total = free + reserved + staked
                        
                        result[address] = {
                            'free_balance': free,
                            'reserved_balance': reserved,
                            'staked_balance': staked,
                            'total_balance': total
                        }
                except Exception as e:
                    # This should only happen if termination was requested
                    if self.terminate_event.is_set():
                        logger.info(f"Termination requested during balance query for {address}")
                        break
                    else:
                        # This shouldn't happen with infinite retry, but log it just in case
                        logger.error(f"Unexpected error querying balance for {address}: {e}")
            
            # Brief pause between batches
            if i + self.batch_size < len(address_list) and not self.terminate_event.is_set():
                time.sleep(0.1)
        
        logger.info(f"Successfully queried balances for {len(result)} addresses")
        return result

    def _process_block(self, block_height: int):
        """Process a single block with termination handling
        
        Args:
            block_height: Block height to process
        """
        try:
            # Check for termination before starting
            if self.terminate_event.is_set():
                logger.info(f"Skipping block {block_height} due to termination request")
                return
                
            # Get the block information - we already have the blocks from the batch,
            # but this ensures we have the most up-to-date data if anything changed
            blocks = self.block_stream_manager.get_blocks_by_range(block_height, block_height)
            
            if not blocks:
                logger.error(f"No block found at height {block_height}")
                return
            
            block = blocks[0]
            block_hash = block['block_hash']
            block_timestamp = block['timestamp']

            # Get affected addresses
            affected_addresses = self._get_affected_addresses(block_height)
            
            if not affected_addresses:
                logger.info(f"No affected addresses found in block {block_height}")
                return
            
            # Query balances with infinite retries
            address_balances = self._query_blockchain_balances(affected_addresses, block_hash)
            
            # Only proceed if we weren't terminated during balance queries
            if not self.terminate_event.is_set():
                # Record balance changes
                self.balance_tracking_indexer.record_balance_change(block_height, block_timestamp, address_balances)
                
                # Calculate and record deltas
                self.balance_tracking_indexer.calculate_and_record_deltas(block_height, block_timestamp, address_balances)
                
                logger.success(f"Processed block {block_height} with {len(address_balances)} addresses")

                # Index balance transfers
                self.balance_tracking_indexer.index_blocks(blocks)
            
        except Exception as e:
            logger.error(f"Error processing block {block_height}: {e}")
            raise

    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'balance_tracking_indexer'):
                self.balance_tracking_indexer.close()
                logger.info("Closed balance tracking indexer connection")
        except Exception as e:
            logger.error(f"Error closing balance tracking indexer: {e}")
        
        try:
            if hasattr(self, 'block_stream_manager'):
                self.block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enhanced Balance Tracking Consumer')
    parser.add_argument(
        '--network',
        type=str,
        required=True,
        choices=networks,
        help='Network to track balances for (polkadot, torus, or bittensor)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of blocks to process in a single batch'
    )
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-balance-tracking'
    setup_logger(service_name)

    # Initialize components
    balance_tracking_indexer = None
    block_stream_manager = None
    substrate_node = None

    try:
        clickhouse_params = get_clickhouse_connection_string(args.network)
        create_clickhouse_database(clickhouse_params)

        partitioner = get_partitioner(args.network)
        balance_tracking_indexer = get_balance_tracking_indexer(args.network, clickhouse_params, partitioner)
        block_stream_manager = BlockStreamManager(clickhouse_params, args.network, terminate_event)
        
        node_url = get_substrate_node_url(args.network)
        substrate_node = SubstrateNode(args.network, node_url, terminate_event)

        consumer = BalanceTrackingConsumer(
            block_stream_manager,
            substrate_node,
            balance_tracking_indexer,
            terminate_event,
            args.network,
            args.batch_size
        )

        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error", error=e, trb=traceback.format_exc())
    finally:
        try:
            if balance_tracking_indexer:
                balance_tracking_indexer.close()
                logger.info("Closed balance tracking indexer connection")
        except Exception as e:
            logger.error(f"Error closing balance tracking indexer: {e}")

        try:
            if block_stream_manager:
                block_stream_manager.close()
                logger.info("Closed block stream manager connection")
        except Exception as e:
            logger.error(f"Error closing block stream manager: {e}")

        logger.info("Enhanced balance tracking consumer stopped")