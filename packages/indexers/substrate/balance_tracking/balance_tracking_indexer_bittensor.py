from typing import List, Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_tracking.balance_tracking_indexer import BalanceTrackingIndexer
from packages.indexers.base.decimal_utils import convert_to_decimal_units


class BittensorBalanceTrackingIndexer(BalanceTrackingIndexer):
    """
    Bittensor-specific implementation of the BalanceTrackingIndexer.
    Handles Bittensor-specific balance tracking functionality.
    """
    
    def __init__(self, connection_params: Dict[str, Any], partitioner, network: str):
        """
        Initialize the BittensorBalanceTrackingIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'bittensor', 'bittensor_testnet')
        """
        super().__init__(connection_params, partitioner, network)
    