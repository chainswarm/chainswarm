from typing import Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_volume_series.balance_volume_series_indexer import BalanceVolumeSeriesIndexer


class BittensorBalanceVolumeSeriesIndexer(BalanceVolumeSeriesIndexer):
    """
    Bittensor-specific implementation of the BalanceVolumeSeriesIndexer.
    Handles Bittensor-specific volume calculations and transaction categorization.
    """
    
    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """
        Initialize the BittensorBalanceVolumeSeriesIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            network: Network identifier (e.g., 'bittensor', 'bittensor_finney')
            period_hours: Number of hours in each period (default: 4)
        """
        super().__init__(connection_params, network, period_hours)
        logger.info(f"Initialized Bittensor-specific Balance Volume Series Indexer for network: {network}")
    
    def categorize_transaction_size(self, volume: Decimal) -> str:
        """
        Categorize a transaction based on its volume, using Bittensor-specific thresholds.
        TAO has a high value token, so the thresholds are adjusted accordingly.
        
        Args:
            volume: Transaction volume in tokens (TAO)
            
        Returns:
            Category string: 'micro', 'small', 'medium', 'large', 'whale'
        """
        # Bittensor-specific thresholds based on TAO token economics
        if volume < Decimal('0.01'):
            return 'micro'
        elif volume < Decimal('0.1'):
            return 'small'
        elif volume < Decimal('1'):
            return 'medium'
        elif volume < Decimal('10'):
            return 'large'
        else:
            return 'whale'
    
    def _init_tables(self):
        """Initialize tables with any Bittensor-specific configurations"""
        # Use the base implementation for now, but can be extended for Bittensor-specific needs
        super()._init_tables()
        # Any Bittensor-specific table initialization would go here