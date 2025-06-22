from typing import Dict, Any
from loguru import logger

from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer_base import BalanceVolumeSeriesIndexerBase
from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer_torus import TorusBalanceVolumeSeriesIndexer
from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer_bittensor import BittensorBalanceVolumeSeriesIndexer
from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer_polkadot import PolkadotBalanceVolumeSeriesIndexer


class BalanceVolumeSeriesIndexer(BalanceVolumeSeriesIndexerBase):
    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """Initialize the Balance Volume Series Indexer with a database connection

        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
            period_hours: Number of hours in each period (default: 4)
        """
        # Initialize the base class
        super().__init__(connection_params, network, period_hours)

    def _init_tables(self):
        """Initialize tables for balance volume series"""
        # Call the parent method to initialize base tables
        super()._init_tables()

    def categorize_transaction_size(self, volume):
        """
        Categorize a transaction based on its volume.
        This implementation delegates to the appropriate network-specific categorization.
        
        Args:
            volume: Transaction volume in tokens
            
        Returns:
            Category string: 'micro', 'small', 'medium', 'large', 'whale'
        """
        # Use the base implementation which will be overridden by network-specific classes if needed
        return super().categorize_transaction_size(volume)


def get_balance_volume_series_indexer(network: str, connection_params: Dict[str, Any], period_hours: int = 4):
    """
    Factory function to get the appropriate balance volume series indexer based on network.
    
    Args:
        network: Network identifier (torus, bittensor, polkadot)
        connection_params: ClickHouse connection parameters
        period_hours: Number of hours in each period (default: 4)
        
    Returns:
        BalanceVolumeSeriesIndexer: Appropriate indexer instance for the network
        
    Raises:
        ValueError: If network is invalid
    """
    network = network.lower()
    
    if 'torus' in network:
        return TorusBalanceVolumeSeriesIndexer(connection_params, network, period_hours)
    elif 'bittensor' in network:
        return BittensorBalanceVolumeSeriesIndexer(connection_params, network, period_hours)
    elif 'polkadot' in network:
        return PolkadotBalanceVolumeSeriesIndexer(connection_params, network, period_hours)
    else:
        logger.warning(f"No specific implementation for network {network}, using base implementation")
        return BalanceVolumeSeriesIndexer(connection_params, network, period_hours)