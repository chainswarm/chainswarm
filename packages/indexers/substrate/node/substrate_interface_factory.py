from packages.indexers.substrate import Network
from substrateinterface import SubstrateInterface
from loguru import logger


class SubstrateInterfaceFactory:
    """
    Factory class for creating SubstrateInterface instances based on network type.
    Each network may have different configuration requirements for the substrate interface.
    """
    
    @staticmethod
    def create_substrate_interface(network: str, node_ws_url: str) -> SubstrateInterface:
        """
        Create a SubstrateInterface instance based on the network type.
        
        Args:
            network: The network identifier (e.g., 'bittensor', 'torus', 'polkadot')
            node_ws_url: The WebSocket URL for the node
            
        Returns:
            SubstrateInterface: Configured for the specified network
            
        Raises:
            ValueError: If the network is not supported
        """
        network = network.lower()
        
        try:
            if network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
                return SubstrateInterfaceFactory._create_bittensor_interface(node_ws_url)
            elif network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
                return SubstrateInterfaceFactory._create_torus_interface(node_ws_url)
            elif network == Network.POLKADOT.value:
                return SubstrateInterfaceFactory._create_polkadot_interface(node_ws_url)
            else:
                # Simplified error logging for unsupported networks
                logger.error("Unsupported network configuration", extra={
                    "network": network,
                    "endpoint": node_ws_url,
                    "error_category": "validation_error",
                    "supported_networks": [Network.BITTENSOR.value, Network.TORUS.value, Network.POLKADOT.value]
                })
                raise ValueError(f"Unsupported network: {network}")
        except Exception as e:
            if not isinstance(e, ValueError):
                # Simplified error logging for interface creation failures
                logger.error("Failed to create SubstrateInterface", extra={
                    "network": network,
                    "endpoint": node_ws_url,
                    "error": str(e),
                    "error_category": classify_error(e)
                })
            raise

    @staticmethod
    def _create_bittensor_interface(node_ws_url: str) -> SubstrateInterface:
        """Create a SubstrateInterface instance for Bittensor network"""
        try:
            substrate = SubstrateInterface(
                url=node_ws_url,
                use_remote_preset=True,
                cache_region=None,
                #ws_options={
                 #   'max_size': 2 ** 25,  # 32MB
                  #  'ping_interval': 60,
                   # 'ping_timeout': 300,
                #}
            )
            return substrate
        except Exception as e:
            # Simplified error logging for Bittensor interface creation failures
            logger.error("Failed to create Bittensor SubstrateInterface", extra={
                "network": "bittensor",
                "endpoint": node_ws_url,
                "error": str(e),
                "error_category": classify_error(e),
                "interface_config": {
                    "use_remote_preset": True,
                    "cache_region": None
                }
            })
            raise RuntimeError(f"Failed to create Bittensor SubstrateInterface: {e}")
    
    @staticmethod
    def _create_torus_interface(node_ws_url: str) -> SubstrateInterface:
        """Create a SubstrateInterface instance for Torus network"""
        try:
            substrate = SubstrateInterface(
                url=node_ws_url,
                use_remote_preset=True,
                cache_region=None
            )
            return substrate
        except Exception as e:
            # Simplified error logging for Torus interface creation failures
            logger.error("Failed to create Torus SubstrateInterface", extra={
                "network": "torus",
                "endpoint": node_ws_url,
                "error": str(e),
                "error_category": classify_error(e),
                "interface_config": {
                    "use_remote_preset": True,
                    "cache_region": None
                }
            })
            raise RuntimeError(f"Failed to create Torus SubstrateInterface: {e}")
    
    @staticmethod
    def _create_polkadot_interface(node_ws_url: str) -> SubstrateInterface:
        """Create a SubstrateInterface instance for Polkadot network"""
        try:
            substrate = SubstrateInterface(
                url=node_ws_url,
                use_remote_preset=True,
                cache_region=None
            )
            return substrate
        except Exception as e:
            # Simplified error logging for Polkadot interface creation failures
            logger.error("Failed to create Polkadot SubstrateInterface", extra={
                "network": "polkadot",
                "endpoint": node_ws_url,
                "error": str(e),
                "error_category": classify_error(e),
                "interface_config": {
                    "use_remote_preset": True,
                    "cache_region": None
                }
            })
            raise RuntimeError(f"Failed to create Polkadot SubstrateInterface: {e}")