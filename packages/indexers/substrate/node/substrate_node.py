import asyncio
import traceback
import time
import functools
from concurrent.futures import ThreadPoolExecutor
from substrateinterface.base import SubstrateInterface
from loguru import logger
from typing import List, Dict, Any, Optional, Tuple
from scalecodec.base import ScaleBytes
from scalecodec.types import CompactU32

from packages.indexers.substrate.node.abstract_node import Node
from packages.indexers.substrate.node.substrate_interface_factory import SubstrateInterfaceFactory
from packages.indexers.substrate import Network


def with_infinite_retry(method):
    """
    Decorator for SubstrateNode methods to implement infinite retry with connection reset.
    
    This decorator will:
    1. Retry the method indefinitely until success or termination
    2. Reset the substrate interface connections periodically during retries
    3. Use constant backoff time to avoid delays
    4. Check for termination events to allow graceful shutdown
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        retry_count = 0
        backoff_time = 1  # Constant 1 second backoff
        
        while True:  # Infinite loop
            try:
                # Attempt the operation
                return method(self, *args, **kwargs)
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Retry {retry_count} for {method.__name__}: {e}")
                
                if hasattr(self, 'terminate_event') and self.terminate_event.is_set():
                    logger.info(f"Termination requested during {method.__name__} retry")
                    raise RuntimeError(f"Operation {method.__name__} terminated during retry")
                
                self._reinitialize_substrate_interfaces()

                # Constant backoff of 1 second
                time.sleep(backoff_time)
                
                # Continue the loop (infinite retry)
    return wrapper


class SubstrateNode(Node):
    def __init__(self, network: str, node_ws_url: str, terminate_event):
        super().__init__()
        self.network = network  # Store network type
        self.node_ws_url = node_ws_url
        self.terminate_event = terminate_event  # Store termination event

        # Initialize substrate interfaces to None first
        self._get_block_data_substrate = None
        self._get_events_substrate = None
        
        # Create fresh instances
        self._reinitialize_substrate_interfaces()
        
        self.executor = ThreadPoolExecutor(max_workers=4)  # Thread pool for concurrent execution

    def _test_connection(self):
        """Test connection to the Substrate node"""
        max_attempts = 2  # Number of connection attempts before giving up
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Test connection for block data substrate instance
                logger.info(f"Testing connection to Substrate node at {self.node_ws_url} (Attempt {attempt}/{max_attempts})")
                
                # Check if websocket is connected
                if not hasattr(self._get_block_data_substrate, 'websocket') or not self._get_block_data_substrate.websocket:
                    logger.warning("Block data substrate websocket not connected, attempting to reconnect")
                    # Close and recreate if needed
                    try:
                        self._get_block_data_substrate.close()
                    except:
                        pass
                    self._get_block_data_substrate = SubstrateInterfaceFactory.create_substrate_interface(
                        self.network, self.node_ws_url
                    )
                
                # Test the connection
                chain_info = self._get_block_data_substrate.get_chain_head()
                logger.info(f"Connected to chain: {self._get_block_data_substrate.chain}")
                
                # Ensure metadata is initialized for block data instance
                if self._get_block_data_substrate.metadata is None:
                    logger.info("Initializing metadata for block data substrate instance")
                    self._get_block_data_substrate.init_runtime()
                    if self._get_block_data_substrate.metadata is None:
                        raise RuntimeError("Failed to initialize metadata for block data substrate instance")
                    logger.info("Successfully initialized metadata for block data substrate instance")
                
                # Test connection for events substrate instance
                logger.info(f"Testing connection for events substrate instance (Attempt {attempt}/{max_attempts})")
                
                # Check if websocket is connected
                if not hasattr(self._get_events_substrate, 'websocket') or not self._get_events_substrate.websocket:
                    logger.warning("Events substrate websocket not connected, attempting to reconnect")
                    # Close and recreate if needed
                    try:
                        self._get_events_substrate.close()
                    except:
                        pass
                    self._get_events_substrate = SubstrateInterfaceFactory.create_substrate_interface(
                        self.network, self.node_ws_url
                    )
                
                # Test the connection
                self._get_events_substrate.get_chain_head()
                
                # Ensure metadata is initialized for events instance
                if self._get_events_substrate.metadata is None:
                    logger.info("Initializing metadata for events substrate instance")
                    self._get_events_substrate.init_runtime()
                    if self._get_events_substrate.metadata is None:
                        raise RuntimeError("Failed to initialize metadata for events substrate instance")
                    logger.info("Successfully initialized metadata for events substrate instance")
                
                logger.info("Successfully connected to Substrate node and initialized metadata for both instances")
                return  # Success, exit the method
                
            except Exception as e:
                error_message = str(e)
                if "Broken pipe" in error_message or "Connection" in error_message or "WebSocket" in error_message:
                    if attempt < max_attempts:
                        logger.warning(f"Connection error during test: {e}. Retrying ({attempt}/{max_attempts})")
                        time.sleep(2)  # Wait before retrying
                        continue
                
                # If we've reached max attempts or it's not a connection error, log and raise
                logger.error(f"Failed to connect to Substrate node: {e}", trb=traceback.format_exc())
                raise RuntimeError(f"Failed to connect to Substrate node: {e}")

    def _get_block_data(self, block_hash: str) -> Dict[str, Any]:
        """Get block data for a specific block hash"""
        try:
            raw_block_data = self._get_block_data_substrate.get_block(block_hash)
            return raw_block_data
        except Exception as e:
            logger.error(f"Failed to fetch block data for {block_hash}: {e}", trb=traceback.format_exc())
            raise RuntimeError(f"Failed to fetch block data for {block_hash}: {e}")

    def _get_events(self, block_hash: str) -> Any:
        """Get events for a specific block hash"""
        try:
            # Check if metadata is available
            if self._get_events_substrate.metadata is None:
                logger.warning(f"Metadata is None for block {block_hash}, attempting to refresh metadata")
                # Try to refresh the metadata
                self._get_events_substrate.init_runtime()
                
                # Check again after refresh
                if self._get_events_substrate.metadata is None:
                    logger.error(f"Failed to initialize metadata even after refresh for block {block_hash}")
                    raise RuntimeError(f"Metadata is still None after refresh for block {block_hash}")

            raw_events = self._get_events_substrate.get_events(block_hash)
            return raw_events
        except Exception as e:
            logger.error(f"Failed to fetch events for {block_hash}: {e}", trb=traceback.format_exc())
            raise RuntimeError(f"Failed to fetch events for {block_hash}: {e}")

    async def _fetch_concurrently(self, block_hash: str) -> Dict[str, Any]:
        """Fetch block data and events concurrently"""
        loop = asyncio.get_event_loop()

        try:
            # Create futures for block data and events
            block_data_future = loop.run_in_executor(self.executor, self._get_block_data, block_hash)
            events_future = loop.run_in_executor(self.executor, self._get_events, block_hash)
            
            # Gather results with better error handling
            try:
                block_data, events = await asyncio.gather(
                    block_data_future, events_future,
                    return_exceptions=True  # This will prevent one failure from canceling the other task
                )
                
                # Check if either result is an exception
                if isinstance(block_data, Exception):
                    logger.error(f"Error fetching block data: {block_data}")
                    raise block_data
                
                if isinstance(events, Exception):
                    logger.error(f"Error fetching events: {events}")
                    raise events

            except Exception as e:
                logger.error(f"Error during gather operation: {e}", trb=traceback.format_exc())
                raise RuntimeError(f"Failed to gather block data and events: {e}")

            # Extract timestamp
            timestamp = None
            for e in block_data["extrinsics"]:
                if isinstance(e.value['call']['call_args'], list):
                    for arg in e.value['call']['call_args']:
                        if arg.get('name') == 'now':
                            timestamp = int(arg.get('value'))
                            break

            if timestamp is None:
                logger.error(f"Timestamp not found in block extrinsics for {block_hash}")
                raise ValueError("Timestamp not found in block extrinsics")

            return {
                "block_data": block_data,
                "events": events,
                "timestamp": timestamp
            }
        except Exception as e:
            logger.error(f"Error in _fetch_concurrently for {block_hash}: {e}", trb=traceback.format_exc())
            raise RuntimeError(f"Failed to fetch block data concurrently for {block_hash}: {e}")

    @with_infinite_retry
    def get_block_by_height(self, block_height: int) -> Dict[str, Any] | None:
        """Get block data by height with infinite retry"""
        try:
            # Use the block data instance to fetch the block hash
            block_hash = self._get_block_data_substrate.get_block_hash(block_height)
            if not block_hash:
                logger.warning(f"No block hash found for height {block_height}")
                return None

            # Check metadata status before concurrent fetch
            if self._get_block_data_substrate.metadata is None:
                logger.warning(f"Block data substrate metadata is None for block {block_height}, refreshing")
                self._get_block_data_substrate.init_runtime()
            
            if self._get_events_substrate.metadata is None:
                logger.warning(f"Events substrate metadata is None for block {block_height}, refreshing")
                self._get_events_substrate.init_runtime()

            # Run the concurrent fetch in the event loop
            # Create a new event loop for this thread if needed
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # No event loop in this thread, create a new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            result = loop.run_until_complete(self._fetch_concurrently(block_hash))
            
            return {
                "block_height": block_height,
                "block_hash": block_hash,
                "timestamp": result["timestamp"],
                "extrinsics": result["block_data"]["extrinsics"],
                "events": result["events"],
            }

        except Exception as e:
            logger.error(f"Error getting block {block_height}: {e}", trb=traceback.format_exc())
            
            # Check if this is a metadata-related error and provide more context
            if "NoneType" in str(e) and "get_metadata_pallet" in str(e):
                logger.error(f"Metadata error detected for block {block_height}. Substrate connection state: "
                           f"block_data_metadata={self._get_block_data_substrate.metadata is not None}, "
                           f"events_metadata={self._get_events_substrate.metadata is not None}")
            
            raise RuntimeError(f"Error getting block {block_height}: {e}")

    @with_infinite_retry
    def get_current_block_height(self) -> int:
        """Get current block height with infinite retry"""
        try:
            return self._get_block_data_substrate.get_block_number(None)
        except Exception as e:
            logger.error(f"Failed to fetch current block height: {e}")
            raise RuntimeError(f"Failed to fetch current block height: {e}")

    def _reinitialize_substrate_interfaces(self):
        """Reinitialize both SubstrateInterface instances to recover from connection or metadata issues"""
        logger.warning("Reinitializing SubstrateInterface instances")
        try:
            # Log connection state before closing
            logger.info(f"Connection state before reinitializing - Block data substrate: "
                      f"{'Connected' if hasattr(self._get_block_data_substrate, 'websocket') and self._get_block_data_substrate.websocket else 'Disconnected'}, "
                      f"Events substrate: {'Connected' if hasattr(self._get_events_substrate, 'websocket') and self._get_events_substrate.websocket else 'Disconnected'}")
            
            # Close existing connections if possible
            try:
                if hasattr(self._get_block_data_substrate, 'websocket') and self._get_block_data_substrate.websocket:
                    logger.info("Closing block data substrate connection")
                    self._get_block_data_substrate.close()
            except Exception as e:
                logger.warning(f"Error closing block data substrate connection: {e}")
                
            try:
                if hasattr(self._get_events_substrate, 'websocket') and self._get_events_substrate.websocket:
                    logger.info("Closing events substrate connection")
                    self._get_events_substrate.close()
            except Exception as e:
                logger.warning(f"Error closing events substrate connection: {e}")
            
            # Create new instances with a small delay to ensure clean connections
            logger.info(f"Creating new SubstrateInterface instances to {self.node_ws_url}")
            time.sleep(1)
            self._get_block_data_substrate = SubstrateInterfaceFactory.create_substrate_interface(
                self.network, self.node_ws_url
            )
            time.sleep(0.5)  # Small delay between connections
            self._get_events_substrate = SubstrateInterfaceFactory.create_substrate_interface(
                self.network, self.node_ws_url
            )
            
            # Test the new connections
            logger.info("Testing new connections")
            self._test_connection()
            
            logger.info("Successfully reinitialized SubstrateInterface instances")
            return True
        except Exception as e:
            logger.error(f"Failed to reinitialize SubstrateInterface instances: {e}", trb=traceback.format_exc())
            return False
    
    @with_infinite_retry
    def get_blocks_by_height_range(self, start_height: int, end_height: int) -> List[Dict[str, Any]]:
        """Get blocks by height range with infinite retry for each block"""
        blocks = []
        for height in range(start_height, end_height + 1):
            # The get_block_by_height method already has infinite retry
            block = self.get_block_by_height(height)
            if block:
                blocks.append(block)
            else:
                logger.error(f"No block found at height {height}")
                raise ValueError(f"No block found at height {height}")
                
            # Check for termination between blocks
            if hasattr(self, 'terminate_event') and self.terminate_event.is_set():
                logger.info(f"Termination requested during block range fetch at height {height}")
                break
                
        return blocks

    @with_infinite_retry
    def get_balances_at_block(self, block_hash: str, params: List[str]) -> Optional[Dict[str, Any]]:
        """
        Query storage at a specific block with infinite retry
        
        Args:
            block_hash: The block hash to query at
            params: List of parameters for the storage function (e.g., [address])
            
        Returns:
            The storage data or None if not found
        """
        try:
            # Query account data
            account_data = self._get_block_data_substrate.query(
                module="System",
                storage_function="Account",
                params=params,
                block_hash=block_hash
            )
            
            if not account_data or not account_data.value:
                return None
            
            address = params[0] if params else None
            result = self._get_block_data_substrate.query_map(
                module="Torus0",
                storage_function="StakingTo",
                params=[address],
                block_hash=block_hash
            )

            staking_to_raw = {}
            for item in result:
                key = item[0].value
                value = item[1].value
                staking_to_raw[key] = value

            staked_balance = sum(staking_to_raw.values())

            result = account_data.value
            result['data']['staked'] = staked_balance
            return result
                
        except Exception as e:
            # This will be caught by the decorator and retried
            logger.warning(f"Error querying storage at block {block_hash}: {e}")
            raise RuntimeError(f"Error querying storage at block {block_hash}: {e}")

    @with_infinite_retry
    def get_token_decimals(self) -> int:
        """
        Fetch the token decimal precision from the Bittensor blockchain.
        This method determines decimals from the `Balances` pallet's `Account` storage function.
        """
        try:
            # Retrieve metadata and locate the Balances pallet
            metadata = self._get_block_data_substrate.get_metadata()

            for pallet in metadata.pallets:
                if pallet.name == "Balances":
                    for storage in pallet.storage:
                        if storage.name == "Account":
                            balance_type = storage.type
                            logger.info(f"Balance type: {balance_type}")

                            # Extract decimals based on known Substrate patterns
                            if "u128" in str(balance_type):  # Most Substrate chains use u128 for balances
                                return 12
                            elif "u256" in str(balance_type):  # EVM-like chains use u256
                                return 18

            raise RuntimeError("Could not determine token decimals from metadata.")

        except Exception as e:
            logger.error(f"Failed to fetch token decimals: {e}")
            raise RuntimeError(f"Failed to fetch token decimals: {e}")


if __name__ == "__main__":
    # Example usage
    terminate_event = asyncio.Event()  # Replace with actual event if needed
    node = SubstrateNode(network=Network.TORUS.value, node_ws_url="ws://localhost:9944", terminate_event=terminate_event)
    try:
        #block_height = node.get_current_block_height()
        #print(f"Current block height: {block_height}")

        block_data = node.get_block_by_height(308)
        #print(f"Block data: {block_data}")

        #balances = node.get_balances_at_block(block_data['block_hash'], ["5F3..."])
        #print(f"Balances: {balances}")

        #decimals = node.get_token_decimals()
        #print(f"Token decimals: {decimals}")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")