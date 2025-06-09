from loguru import logger
from packages.indexers.substrate import Network
from substrateinterface import SubstrateInterface


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
        
        if network == Network.BITTENSOR.value or network == Network.BITTENSOR_TESTNET.value:
            return SubstrateInterfaceFactory._create_bittensor_interface(node_ws_url)
        elif network == Network.TORUS.value or network == Network.TORUS_TESTNET.value:
            return SubstrateInterfaceFactory._create_torus_interface(node_ws_url)
        elif network == Network.POLKADOT.value:
            return SubstrateInterfaceFactory._create_polkadot_interface(node_ws_url)
        else:
            raise ValueError(f"Unsupported network: {network}")

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

            logger.info(f"Torus SubstrateInterface created for {node_ws_url}")
            return substrate
        except Exception as e:
            raise RuntimeError(f"Failed to create SubstrateInterface")
    
    @staticmethod
    def _create_torus_interface(node_ws_url: str) -> SubstrateInterface:
        """Create a SubstrateInterface instance for Torus network"""
        try:
            substrate = SubstrateInterface(
                url=node_ws_url,
                use_remote_preset=True,
                cache_region=None
            )
            
            logger.info(f"Torus SubstrateInterface created for {node_ws_url}")
            return substrate
        except Exception as e:
            raise RuntimeError(f"Failed to create SubstrateInterface")
    
    @staticmethod
    def _create_polkadot_interface(node_ws_url: str) -> SubstrateInterface:
        """Create a SubstrateInterface instance for Polkadot network"""
        try:
            substrate = SubstrateInterface(
                url=node_ws_url,
                use_remote_preset=True,
                cache_region=None
            )
            
            logger.info(f"Polkadot SubstrateInterface created for {node_ws_url}")
            return substrate
        except Exception as e:
            raise RuntimeError(f"Failed to create SubstrateInterface")