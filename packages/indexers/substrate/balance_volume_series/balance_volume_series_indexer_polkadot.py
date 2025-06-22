from typing import Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer import BalanceVolumeSeriesIndexer


class PolkadotBalanceVolumeSeriesIndexer(BalanceVolumeSeriesIndexer):
    """
    Polkadot-specific implementation of the BalanceVolumeSeriesIndexer.
    Handles Polkadot-specific volume calculations and transaction categorization.
    """
    
    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """
        Initialize the PolkadotBalanceVolumeSeriesIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            network: Network identifier (e.g., 'polkadot', 'kusama')
            period_hours: Number of hours in each period (default: 4)
        """
        super().__init__(connection_params, network, period_hours)
        logger.info(f"Initialized Polkadot-specific Balance Volume Series Indexer for network: {network}")
    
    def categorize_transaction_size(self, volume: Decimal) -> str:
        """
        Categorize a transaction based on its volume, using Polkadot-specific thresholds.
        DOT has a medium value token, so thresholds are adjusted accordingly.
        
        Args:
            volume: Transaction volume in tokens (DOT)
            
        Returns:
            Category string: 'micro', 'small', 'medium', 'large', 'whale'
        """
        # Polkadot-specific thresholds
        if volume < Decimal('0.2'):
            return 'micro'
        elif volume < Decimal('20'):
            return 'small'
        elif volume < Decimal('200'):
            return 'medium'
        elif volume < Decimal('2000'):
            return 'large'
        else:
            return 'whale'
    
    def _init_tables(self):
        """Initialize tables with any Polkadot-specific configurations"""
        # Use the base implementation for now, but can be extended for Polkadot-specific needs
        super()._init_tables()
        # Any Polkadot-specific table initialization would go here