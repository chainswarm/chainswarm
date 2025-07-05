import os
import time
from functools import wraps
from enum import Enum
from dotenv import load_dotenv
from loguru import logger


class Network(Enum):
    TORUS = "torus"
    TORUS_TESTNET = "torus_testnet"
    POLKADOT = "polkadot"
    BITTENSOR = "bittensor"
    BITTENSOR_TESTNET = "bittensor_testnet"

    @classmethod
    def get_block_time(cls, network: str) -> int:
        """Get block time in seconds for the specified network"""
        network = network.lower()
        if network == cls.TORUS.value or network == cls.TORUS_TESTNET.value:
            return 8
        elif network == cls.POLKADOT.value:
            return 6
        elif network == cls.BITTENSOR.value or network == cls.BITTENSOR_TESTNET.value:
            return 12
        raise ValueError(f"Unsupported network: {network}")

    @classmethod
    def get_network_asset(cls, network: str) -> str:
        """
        Get the native asset symbol for the specified network.
        
        This function provides centralized mapping between blockchain networks
        and their native asset symbols. The asset information is used throughout
        the indexing system to properly categorize and track different tokens.
        
        Args:
            network (str): The blockchain network identifier (case-insensitive)
                         Supported networks: torus, torus_testnet, bittensor,
                         bittensor_testnet, polkadot
        
        Returns:
            str: The native asset symbol for the network
                - TOR for torus and torus_testnet networks
                - TAO for bittensor and bittensor_testnet networks
                - DOT for polkadot network
        
        Raises:
            ValueError: If the network is not supported
            
        Examples:
            >>> Network.get_network_asset("torus")
            'TOR'
            >>> Network.get_network_asset("bittensor")
            'TAO'
            >>> Network.get_network_asset("polkadot")
            'DOT'
        """
        network = network.lower()
        if network == cls.TORUS.value or network == cls.TORUS_TESTNET.value:
            return "TOR"
        elif network == cls.BITTENSOR.value or network == cls.BITTENSOR_TESTNET.value:
            return "TAO"
        elif network == cls.POLKADOT.value:
            return "DOT"
        raise ValueError(f"Unsupported network: {network}")


networks = [Network.POLKADOT.value, Network.TORUS.value, Network.TORUS_TESTNET.value, Network.BITTENSOR.value]


def get_native_network_asset(network: str) -> str:
    """
    Get the native asset symbol for the specified network.

    This is a convenience function that delegates to Network.get_network_asset().
    It provides a simple interface for getting asset information without needing
    to reference the Network enum directly.

    Args:
        network (str): The blockchain network identifier

    Returns:
        str: The native asset symbol for the network

    Raises:
        ValueError: If the network is not supported

    Examples:
        >>> get_native_network_asset("torus")
        'TOR'
        >>> get_native_network_asset("bittensor")
        'TAO'
    """
    return Network.get_network_asset(network)


def retry_with_backoff(retries=100, backoff_in_seconds=10):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt == retries:
                        logger.error(f"Failed after {retries} attempts. Last error: {str(e)}")
                        raise

                    sleep_time = backoff_in_seconds
                    logger.warning(
                        f"Attempt {attempt} failed with error: {str(e)}. "
                        f"Retrying in {sleep_time} seconds..."
                    )
                    time.sleep(sleep_time)
            return None

        return wrapper

    return decorator


load_dotenv()


def get_substrate_node_url(network):
    if network == Network.POLKADOT.value:
        node_ws_url = os.getenv("POLKADOT_NODE_WS_URL")
    elif network == Network.TORUS.value:
        node_ws_url = os.getenv("TORUS_NODE_WS_URL")
    elif network == Network.TORUS_TESTNET.value:
        node_ws_url = os.getenv("TORUS_TESTNET_NODE_WS_URL")
    elif network == Network.BITTENSOR.value:
        node_ws_url = os.getenv("BITTENSOR_NODE_WS_URL")
    elif network == Network.BITTENSOR_TESTNET.value:
        node_ws_url = os.getenv("BITTENSOR_TESTNET_NODE_WS_URL")
    else:
        raise ValueError(f"Unsupported network: {network}")

    if not node_ws_url:
        raise ValueError(f"Node WebSocket URL not set for network: {network}. Please check your environment variables.")

    return node_ws_url





