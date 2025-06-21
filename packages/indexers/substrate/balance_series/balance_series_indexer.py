from typing import Dict, Any
from loguru import logger

from packages.indexers.substrate.balance_series.balance_series_indexer_base import BalanceSeriesIndexerBase


class BalanceSeriesIndexer(BalanceSeriesIndexerBase):
    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """Initialize the Balance Series Indexer with database connection

        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
            period_hours: Number of hours in each period (default: 4)
        """
        # Initialize the base class
        super().__init__(connection_params, network, period_hours)

    def _init_tables(self):
        """Initialize tables for balance series"""
        # Call the parent method to initialize base tables
        super()._init_tables()