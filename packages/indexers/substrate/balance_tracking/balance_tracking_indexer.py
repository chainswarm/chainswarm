from typing import Dict, Any, List
from loguru import logger

from packages.indexers.substrate.balance_tracking.balance_tracking_indexer_base import BalanceTrackingIndexerBase
from packages.indexers.substrate.block_range_partitioner import BlockRangePartitioner


class BalanceTrackingIndexer(BalanceTrackingIndexerBase):
    def __init__(self, connection_params: Dict[str, Any], partitioner: BlockRangePartitioner, network: str):
        """Initialize the Enhanced Balance Tracking Indexer with database connection

        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
        """
        # Initialize the base class
        super().__init__(connection_params, partitioner, network)

    def _init_tables(self):
        """Initialize tables for balance tracking"""
        # Call the parent method to initialize base tables
        super()._init_tables()
