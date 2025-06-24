"""
Unified Genesis Balance Manager with network-specific strategies.
Handles genesis balance import for both money_flow and balance_series modules.
"""

import os
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Union
from decimal import Decimal
from loguru import logger

from packages.indexers.substrate import Network, data, get_network_asset


class GenesisBalanceStrategy(ABC):
    """Abstract base class for network-specific genesis balance strategies"""
    
    @abstractmethod
    def has_genesis_balances(self) -> bool:
        """Check if this network has genesis balances"""
        pass
    
    @abstractmethod
    def get_genesis_file_path(self) -> Optional[str]:
        """Get the path to the genesis balances file"""
        pass
    
    @abstractmethod
    def load_genesis_balances(self) -> List[Tuple[str, Union[int, Decimal], str]]:
        """Load genesis balances for this network with asset information"""
        pass
    
    @abstractmethod
    def validate_genesis_data(self, data: List) -> None:
        """Validate the structure of genesis balance data"""
        pass
    
    @abstractmethod
    def get_network_asset(self) -> str:
        """Get the asset symbol for this network"""
        pass


class TorusGenesisBalanceStrategy(GenesisBalanceStrategy):
    """Genesis balance strategy for Torus networks"""
    
    def __init__(self, network: str):
        self.network = network
        self.filename = "torus-genesis-balances.json"
    
    def has_genesis_balances(self) -> bool:
        return True
    
    def get_genesis_file_path(self) -> Optional[str]:
        file_path = os.path.join(os.path.dirname(os.path.abspath(data.__file__)), self.filename)
        
        # Fail fast if configured file doesn't exist
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Genesis balances file not found for network '{self.network}': {file_path}")
        
        return file_path
    
    def load_genesis_balances(self) -> List[Tuple[str, Union[int, Decimal], str]]:
        file_path = self.get_genesis_file_path()
        if file_path is None:
            return []
        
        try:
            with open(file_path, 'r') as f:
                balances_data = json.load(f)
            
            self.validate_genesis_data(balances_data)
            
            asset = self.get_network_asset()
            genesis_balances = []
            for address, amount in balances_data:
                genesis_balances.append((address, int(amount), asset))
            
            logger.info(f"Loaded {len(genesis_balances)} genesis balances for network '{self.network}' with asset '{asset}'")
            return genesis_balances
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse genesis balances JSON for network '{self.network}': {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load genesis balances for network '{self.network}': {e}")
    
    def validate_genesis_data(self, data: List) -> None:
        if not isinstance(data, list):
            raise ValueError(f"Genesis balances file must contain a list, got {type(data)}")
        
        for i, item in enumerate(data):
            if not isinstance(item, list) or len(item) != 2:
                raise ValueError(f"Genesis balance entry {i} must be [address, amount], got {item}")
            
            address, amount = item
            if not isinstance(address, str):
                raise ValueError(f"Genesis balance address at index {i} must be string, got {type(address)}")
            if not isinstance(amount, (int, float)):
                raise ValueError(f"Genesis balance amount at index {i} must be number, got {type(amount)}")
    
    def get_network_asset(self) -> str:
        """Get the asset symbol for this network"""
        return get_network_asset(self.network)


class NoGenesisBalanceStrategy(GenesisBalanceStrategy):
    """Strategy for networks that don't have genesis balances"""
    
    def __init__(self, network: str):
        self.network = network
    
    def has_genesis_balances(self) -> bool:
        return False
    
    def get_genesis_file_path(self) -> Optional[str]:
        return None
    
    def load_genesis_balances(self) -> List[Tuple[str, Union[int, Decimal], str]]:
        logger.info(f"Network '{self.network}' does not have genesis balances - returning empty list")
        return []
    
    def validate_genesis_data(self, data: List) -> None:
        # No validation needed for networks without genesis balances
        pass
    
    def get_network_asset(self) -> str:
        """Get the asset symbol for this network"""
        return get_network_asset(self.network)


class GenesisBalanceManager:
    """Unified manager for genesis balance operations across all networks and modules"""
    
    # Strategy mapping for different networks
    NETWORK_STRATEGIES = {
        Network.TORUS.value: TorusGenesisBalanceStrategy,
        Network.TORUS_TESTNET.value: TorusGenesisBalanceStrategy,
        Network.BITTENSOR.value: NoGenesisBalanceStrategy,
        Network.BITTENSOR_TESTNET.value: NoGenesisBalanceStrategy,
        Network.POLKADOT.value: NoGenesisBalanceStrategy,
    }
    
    def __init__(self, network: str, module_type: str):
        """
        Initialize the Genesis Balance Manager
        
        Args:
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
            module_type: Module type ('money_flow' or 'balance_series')
        """
        self.network = network
        self.module_type = module_type
        
        # Validate network
        if network not in self.NETWORK_STRATEGIES:
            raise ValueError(f"Unsupported network '{network}'. Supported networks: {list(self.NETWORK_STRATEGIES.keys())}")
        
        # Validate module type
        if module_type not in ['money_flow', 'balance_series']:
            raise ValueError(f"Unsupported module type '{module_type}'. Supported types: ['money_flow', 'balance_series']")
        
        # Create strategy instance
        strategy_class = self.NETWORK_STRATEGIES[network]
        self.strategy = strategy_class(network)
        
        logger.debug(f"Initialized GenesisBalanceManager for network '{network}' and module '{module_type}'")
    
    def has_genesis_balances(self) -> bool:
        """Check if the network has genesis balances"""
        return self.strategy.has_genesis_balances()
    
    def get_genesis_file_path(self) -> Optional[str]:
        """Get the path to the genesis balances file"""
        return self.strategy.get_genesis_file_path()
    
    def load_genesis_balances_for_money_flow(self) -> List[Tuple[str, int, str]]:
        """Load genesis balances formatted for money flow module"""
        if self.module_type != 'money_flow':
            raise RuntimeError(f"Cannot load money flow balances for module type '{self.module_type}'")
        
        balances = self.strategy.load_genesis_balances()
        # Convert to int for money flow module, preserve asset
        return [(address, int(amount), asset) for address, amount, asset in balances]
    
    def load_genesis_balances_for_balance_series(self) -> List[Tuple[str, Decimal, str]]:
        """Load genesis balances formatted for balance series module"""
        if self.module_type != 'balance_series':
            raise RuntimeError(f"Cannot load balance series balances for module type '{self.module_type}'")
        
        balances = self.strategy.load_genesis_balances()
        # Convert to Decimal for balance series module, preserve asset
        return [(address, Decimal(str(amount)), asset) for address, amount, asset in balances]
    
    def get_supported_networks(self) -> List[str]:
        """Get list of all supported networks"""
        return list(self.NETWORK_STRATEGIES.keys())
    
    def get_networks_with_genesis_balances(self) -> List[str]:
        """Get list of networks that have genesis balances"""
        networks_with_genesis = []
        for network in self.NETWORK_STRATEGIES:
            strategy = self.NETWORK_STRATEGIES[network](network)
            if strategy.has_genesis_balances():
                networks_with_genesis.append(network)
        return networks_with_genesis
    
    def validate_network_for_populate_script(self) -> None:
        """Validate that the network can be used with the populate genesis balances script"""
        if not self.has_genesis_balances():
            raise ValueError(f"Network '{self.network}' is not configured to have genesis balances. "
                           f"Networks with genesis balances: {self.get_networks_with_genesis_balances()}")


# Convenience functions for backward compatibility and easy usage
def create_money_flow_manager(network: str) -> GenesisBalanceManager:
    """Create a genesis balance manager for money flow module"""
    return GenesisBalanceManager(network, 'money_flow')


def create_balance_series_manager(network: str) -> GenesisBalanceManager:
    """Create a genesis balance manager for balance series module"""
    return GenesisBalanceManager(network, 'balance_series')


def get_networks_with_genesis_balances() -> List[str]:
    """Get list of all networks that have genesis balances"""
    manager = GenesisBalanceManager(Network.TORUS.value, 'money_flow')  # Use any valid network for this query
    return manager.get_networks_with_genesis_balances()


def get_all_supported_networks() -> List[str]:
    """Get list of all supported networks"""
    manager = GenesisBalanceManager(Network.TORUS.value, 'money_flow')  # Use any valid network for this query
    return manager.get_supported_networks()