from typing import Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer import BalanceVolumeSeriesIndexer


class TorusBalanceVolumeSeriesIndexer(BalanceVolumeSeriesIndexer):
    """
    Torus-specific implementation of the BalanceVolumeSeriesIndexer.
    Handles Torus-specific volume calculations and transaction categorization.
    """
    
    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """
        Initialize the TorusBalanceVolumeSeriesIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            network: Network identifier (e.g., 'torus', 'torus_testnet')
            period_hours: Number of hours in each period (default: 4)
        """
        super().__init__(connection_params, network, period_hours)
        logger.info(f"Initialized Torus-specific Balance Volume Series Indexer for network: {network}")
    
    def categorize_transaction_size(self, volume: Decimal) -> str:
        """
        Categorize a transaction based on its volume, using Torus-specific thresholds.
        Torus (TOR) has a higher value token so thresholds are adjusted accordingly.
        
        Args:
            volume: Transaction volume in tokens
            
        Returns:
            Category string: 'micro', 'small', 'medium', 'large', 'whale'
        """
        # Torus-specific thresholds (different from base implementation)
        if volume < Decimal('0.05'):
            return 'micro'
        elif volume < Decimal('5'):
            return 'small'
        elif volume < Decimal('50'):
            return 'medium'
        elif volume < Decimal('500'):
            return 'large'
        else:
            return 'whale'
    
    def _init_tables(self):
        """Initialize tables with any Torus-specific configurations"""
        # Use the base implementation for now, but can be extended for Torus-specific needs
        super()._init_tables()
        # Any Torus-specific table initialization would go here