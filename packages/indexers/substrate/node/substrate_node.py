import asyncio
import traceback
import time
import functools
from concurrent.futures import ThreadPoolExecutor
from substrateinterface.base import SubstrateInterface
from typing import List, Dict, Any, Optional, Tuple
from scalecodec.base import ScaleBytes
from scalecodec.types import CompactU32
from loguru import logger

from packages.indexers.base import (
    setup_logger, generate_correlation_id, set_correlation_id
)
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
        correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)
        
        while True:  # Infinite loop
            try:
                # Attempt the operation
                return method(self, *args, **kwargs)
                
            except Exception as e:
                retry_count += 1
                
                # Only log connection/network errors with simplified context
                error_category = classify_error(e)
                if error_category in ['connection_error', 'substrate_error'] and retry_count % 10 == 1:
                    # Throttled logging - only every 10th retry to reduce noise
                    log_error_with_context(
                        f"Substrate operation retry {retry_count} for {method.__name__}",
                        e,
                        correlation_id=correlation_id,
                        method=method.__name__,
                        retry_count=retry_count,
                        error_category=error_category,
                        endpoint=getattr(self, 'node_ws_url', 'unknown'),
                        network=getattr(self, 'network', 'unknown')
                    )
                
                if hasattr(self, 'terminate_event') and self.terminate_event.is_set():
                    logger.info(
                        f"Operation {method.__name__} terminated during retry",
                        extra={
                            "correlation_id": correlation_id,
                            "method": method.__name__,
                            "retry_count": retry_count
                        }
                    )
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

        # Log service initialization
        logger.info(
            "Substrate node initialized",
            extra={
                "network": network,
                "endpoint": node_ws_url
            }
        )

        # Initialize substrate interfaces to None first
        self._get_block_data_substrate = None
        self._get_events_substrate = None
        
        # Create fresh instances
        self._reinitialize_substrate_interfaces()
        
        self.executor = ThreadPoolExecutor(max_workers=4)  # Thread pool for concurrent execution

    def _test_connection(self):
        """Test connection to the Substrate node"""
        max_attempts = 2  # Number of connection attempts before giving up
        correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Check if websocket is connected for block data instance
                if not hasattr(self._get_block_data_substrate, 'websocket') or not self._get_block_data_substrate.websocket:
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
                
                # Ensure metadata is initialized for block data instance
                if self._get_block_data_substrate.metadata is None:
                    self._get_block_data_substrate.init_runtime()
                    if self._get_block_data_substrate.metadata is None:
                        raise RuntimeError("Failed to initialize metadata for block data substrate instance")
                
                # Check if websocket is connected for events instance
                if not hasattr(self._get_events_substrate, 'websocket') or not self._get_events_substrate.websocket:
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
                    self._get_events_substrate.init_runtime()
                    if self._get_events_substrate.metadata is None:
                        raise RuntimeError("Failed to initialize metadata for events substrate instance")
                
                # Only log successful connection establishment once per session
                if attempt == 1:
                    logger.info(
                        "Substrate connection established",
                        extra={
                            "correlation_id": correlation_id,
                            "endpoint": self.node_ws_url,
                            "network": self.network,
                            "chain": getattr(self._get_block_data_substrate, 'chain', 'unknown')
                        }
                    )
                return  # Success, exit the method
                
            except Exception as e:
                error_message = str(e)

                if "Broken pipe" in error_message or "Connection" in error_message or "WebSocket" in error_message:
                    if attempt < max_attempts:
                        time.sleep(2)  # Wait before retrying
                        continue
                
                # Simple error logging
                log_error_with_context(
                    f"Substrate connection test failed after {attempt} attempts",
                    e,
                    correlation_id=correlation_id,
                    endpoint=self.node_ws_url,
                    network=self.network,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    websocket_state={
                        'block_data_connected': hasattr(self._get_block_data_substrate, 'websocket') and bool(self._get_block_data_substrate.websocket),
                        'events_connected': hasattr(self._get_events_substrate, 'websocket') and bool(self._get_events_substrate.websocket)
                    }
                )
                raise RuntimeError(f"Failed to connect to Substrate node: {e}")

    def _get_block_data(self, block_hash: str) -> Dict[str, Any]:
        """Get block data for a specific block hash"""
        try:
            raw_block_data = self._get_block_data_substrate.get_block(block_hash)
            return raw_block_data
        except Exception as e:
            # Simple error logging
            log_error_with_context(
                "Failed to fetch block data via RPC",
                e,
                block_hash=block_hash,
                endpoint=self.node_ws_url,
                network=self.network,
                rpc_method="get_block"
            )
            raise RuntimeError(f"Failed to fetch block data for {block_hash}: {e}")

    def _get_events(self, block_hash: str) -> Any:
        """Get events for a specific block hash"""
        try:
            # Check if metadata is available
            if self._get_events_substrate.metadata is None:
                # Try to refresh the metadata
                self._get_events_substrate.init_runtime()
                
                # Check again after refresh
                if self._get_events_substrate.metadata is None:
                    # Simple error logging for metadata issues
                    log_error_with_context(
                        "Metadata initialization failed after refresh",
                        RuntimeError("Metadata is still None after refresh"),
                        block_hash=block_hash,
                        endpoint=self.node_ws_url,
                        network=self.network,
                        error_category="substrate_error",
                        rpc_method="get_events"
                    )
                    raise RuntimeError(f"Metadata is still None after refresh for block {block_hash}")

            raw_events = self._get_events_substrate.get_events(block_hash)
            return raw_events
        except Exception as e:
            # Simple error logging
            log_error_with_context(
                "Failed to fetch events via RPC",
                e,
                block_hash=block_hash,
                endpoint=self.node_ws_url,
                network=self.network,
                rpc_method="get_events",
                metadata_available=self._get_events_substrate.metadata is not None
            )
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
                    raise block_data
                
                if isinstance(events, Exception):
                    raise events

            except Exception as e:
                # Simple error logging for concurrent fetch failures
                log_error_with_context(
                    "Concurrent block data fetch failed",
                    e,
                    block_hash=block_hash,
                    endpoint=self.node_ws_url,
                    network=self.network,
                    operation="concurrent_fetch"
                )
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
                # Simple error logging for timestamp extraction failures
                log_error_with_context(
                    "Timestamp extraction failed from block extrinsics",
                    ValueError("Timestamp not found in block extrinsics"),
                    block_hash=block_hash,
                    endpoint=self.node_ws_url,
                    network=self.network,
                    error_category="validation_error",
                    extrinsics_count=len(block_data.get("extrinsics", []))
                )
                raise ValueError("Timestamp not found in block extrinsics")

            return {
                "block_data": block_data,
                "events": events,
                "timestamp": timestamp
            }
        except Exception as e:
            # Simple error logging for general concurrent fetch failures
            log_error_with_context(
                "Concurrent fetch operation failed",
                e,
                block_hash=block_hash,
                endpoint=self.node_ws_url,
                network=self.network,
                operation="fetch_concurrently"
            )
            raise RuntimeError(f"Failed to fetch block data concurrently for {block_hash}: {e}")

    @with_infinite_retry
    def get_block_by_height(self, block_height: int) -> Dict[str, Any] | None:
        """Get block data by height with infinite retry"""
        try:
            # Use the block data instance to fetch the block hash
            block_hash = self._get_block_data_substrate.get_block_hash(block_height)
            if not block_hash:
                return None

            # Check metadata status before concurrent fetch
            if self._get_block_data_substrate.metadata is None:
                self._get_block_data_substrate.init_runtime()
            
            if self._get_events_substrate.metadata is None:
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
            # Simple error logging with block context
            error_context = {
                "block_height": block_height,
                "endpoint": self.node_ws_url,
                "network": self.network,
            }
            
            # Check if this is a metadata-related error and provide more context
            if "NoneType" in str(e) and "get_metadata_pallet" in str(e):
                error_context.update({
                    "metadata_error": True,
                    "block_data_metadata_available": self._get_block_data_substrate.metadata is not None,
                    "events_metadata_available": self._get_events_substrate.metadata is not None
                })
                
            log_error_with_context(
                f"Block fetch failed for height {block_height}",
                e,
                **error_context
            )
            
            raise RuntimeError(f"Error getting block {block_height}: {e}")

    @with_infinite_retry
    def get_current_block_height(self) -> int:
        """Get current block height with infinite retry"""
        try:
            return self._get_block_data_substrate.get_block_number(None)
        except Exception as e:
            # Simple error logging for block height fetch failures
            log_error_with_context(
                "Failed to fetch current block height",
                e,
                endpoint=self.node_ws_url,
                network=self.network,
                rpc_method="get_block_number"
            )
            raise RuntimeError(f"Failed to fetch current block height: {e}")

    def _reinitialize_substrate_interfaces(self):
        """Reinitialize both SubstrateInterface instances to recover from connection or metadata issues"""
        correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)
        
        try:
            # Close existing connections if possible
            try:
                if hasattr(self._get_block_data_substrate, 'websocket') and self._get_block_data_substrate.websocket:
                    self._get_block_data_substrate.close()
            except Exception as e:
                # Only log if it's a significant error
                if not any(err in str(e).lower() for err in ['closed', 'disconnected', 'none']):
                    log_error_with_context(
                        "Error closing block data substrate connection",
                        e,
                        correlation_id=correlation_id,
                        endpoint=self.node_ws_url,
                        network=self.network,
                    )
                
            try:
                if hasattr(self._get_events_substrate, 'websocket') and self._get_events_substrate.websocket:
                    self._get_events_substrate.close()
            except Exception as e:
                # Only log if it's a significant error
                if not any(err in str(e).lower() for err in ['closed', 'disconnected', 'none']):
                    log_error_with_context(
                        "Error closing events substrate connection",
                        e,
                        correlation_id=correlation_id,
                        endpoint=self.node_ws_url,
                        network=self.network,
                    )
            
            # Create new instances with a small delay to ensure clean connections
            time.sleep(1)
            self._get_block_data_substrate = SubstrateInterfaceFactory.create_substrate_interface(
                self.network, self.node_ws_url
            )
            time.sleep(0.5)  # Small delay between connections
            self._get_events_substrate = SubstrateInterfaceFactory.create_substrate_interface(
                self.network, self.node_ws_url
            )
            
            # Test the new connections
            self._test_connection()
            
            # Log successful reinitialization
            logger.info(
                "Substrate interfaces reinitialized",
                extra={
                    "correlation_id": correlation_id,
                    "endpoint": self.node_ws_url,
                    "network": self.network
                }
            )
            return True
        except Exception as e:
            # Simple error logging for reinitialization failures
            log_error_with_context(
                "Failed to reinitialize SubstrateInterface instances",
                e,
                correlation_id=correlation_id,
                endpoint=self.node_ws_url,
                network=self.network,
                operation="reinitialize_interfaces"
            )
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
                # Simple error logging for missing blocks
                log_error_with_context(
                    f"No block found at height {height}",
                    ValueError(f"No block found at height {height}"),
                    height=height,
                    start_height=start_height,
                    end_height=end_height,
                    endpoint=self.node_ws_url,
                    network=self.network,
                    error_category="validation_error"
                )
                raise ValueError(f"No block found at height {height}")
                
            # Check for termination between blocks
            if hasattr(self, 'terminate_event') and self.terminate_event.is_set():
                logger.info(
                    "Block range fetch terminated",
                    extra={
                        "current_height": height,
                        "start_height": start_height,
                        "end_height": end_height,
                        "blocks_fetched": len(blocks)
                    }
                )
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
            # Simple error logging for storage query failures
            log_error_with_context(
                "Storage query failed at block",
                e,
                block_hash=block_hash,
                params=params,
                endpoint=self.node_ws_url,
                network=self.network,
                rpc_method="query_storage",
                module="System",
                storage_function="Account"
            )
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

                            # Extract decimals based on known Substrate patterns
                            if "u128" in str(balance_type):  # Most Substrate chains use u128 for balances
                                return 12
                            elif "u256" in str(balance_type):  # EVM-like chains use u256
                                return 18

            # Simple error logging for metadata parsing failures
            log_error_with_context(
                "Could not determine token decimals from metadata",
                RuntimeError("Could not determine token decimals from metadata"),
                endpoint=self.node_ws_url,
                network=self.network,
                error_category="substrate_error",
                rpc_method="get_metadata",
                metadata_available=metadata is not None,
                pallets_count=len(metadata.pallets) if metadata else 0
            )
            raise RuntimeError("Could not determine token decimals from metadata.")

        except Exception as e:
            # Simple error logging for token decimals fetch failures
            log_error_with_context(
                "Failed to fetch token decimals",
                e,
                endpoint=self.node_ws_url,
                network=self.network,
                rpc_method="get_metadata"
            )
            raise RuntimeError(f"Failed to fetch token decimals: {e}")