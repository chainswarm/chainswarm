from packages.indexers.base.enhanced_logging import (
    ErrorContextManager,
    setup_enhanced_logger,
    classify_error
)
from packages.indexers.substrate import Network
from substrateinterface import SubstrateInterface


class SubstrateInterfaceFactory:
    """
    Factory class for creating SubstrateInterface instances based on network type.
    Each network may have different configuration requirements for the substrate interface.
    """
    
    # Class-level error context manager
    _error_ctx = ErrorContextManager("substrate-interface-factory")
    
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
                # Enhanced error logging for unsupported networks
                error = ValueError(f"Unsupported network: {network}")
                SubstrateInterfaceFactory._error_ctx.log_error(
                    "Unsupported network configuration",
                    error,
                    network=network,
                    endpoint=node_ws_url,
                    error_category="validation_error",
                    supported_networks=[Network.BITTENSOR.value, Network.TORUS.value, Network.POLKADOT.value]
                )
                raise error
        except Exception as e:
            if not isinstance(e, ValueError):
                # Enhanced error logging for interface creation failures
                SubstrateInterfaceFactory._error_ctx.log_error(
                    "Failed to create SubstrateInterface",
                    e,
                    network=network,
                    endpoint=node_ws_url,
                    error_category=classify_error(e)
                )
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
            # Enhanced error logging for Bittensor interface creation failures
            SubstrateInterfaceFactory._error_ctx.log_error(
                "Failed to create Bittensor SubstrateInterface",
                e,
                network="bittensor",
                endpoint=node_ws_url,
                error_category=classify_error(e),
                interface_config={
                    "use_remote_preset": True,
                    "cache_region": None
                }
            )
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
            # Enhanced error logging for Torus interface creation failures
            SubstrateInterfaceFactory._error_ctx.log_error(
                "Failed to create Torus SubstrateInterface",
                e,
                network="torus",
                endpoint=node_ws_url,
                error_category=classify_error(e),
                interface_config={
                    "use_remote_preset": True,
                    "cache_region": None
                }
            )
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
            # Enhanced error logging for Polkadot interface creation failures
            SubstrateInterfaceFactory._error_ctx.log_error(
                "Failed to create Polkadot SubstrateInterface",
                e,
                network="polkadot",
                endpoint=node_ws_url,
                error_category=classify_error(e),
                interface_config={
                    "use_remote_preset": True,
                    "cache_region": None
                }
            )
            raise RuntimeError(f"Failed to create Polkadot SubstrateInterface: {e}")