from typing import Dict, Any, List
from loguru import logger

from packages.indexers.substrate.balance_transfers.balance_transfers_indexer_base import BalanceTransfersIndexerBase
from packages.indexers.substrate.block_range_partitioner import BlockRangePartitioner
from packages.indexers.substrate.assets.asset_manager import AssetManager


class BalanceTransfersIndexer(BalanceTransfersIndexerBase):
    def __init__(self, connection_params: Dict[str, Any], partitioner: BlockRangePartitioner, network: str, metrics, asset_manager: AssetManager):
        """Initialize the Balance Transfers Indexer with database connection

        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'torus', 'bittensor', 'polkadot')
            metrics: IndexerMetrics instance for recording metrics (required)
            asset_manager: AssetManager instance for managing assets
        """
        # Initialize the base class
        super().__init__(connection_params, partitioner, network, metrics, asset_manager)

    def _init_tables(self):
        """Initialize tables for balance transfers"""
        # Call the parent method to initialize base tables
        super()._init_tables()

    def _process_events(self, events: List[Dict]):
        """
        Process events and extract balance transfers.
        Overrides the base class method to add network-specific event processing.
        
        Args:
            events: List of events to process
            
        Returns:
            List of balance transfers
        """
        # Process common events using the base class implementation
        balance_transfers = super()._process_events(events)
        
        # Process network-specific events
        network_specific_transfers = self._process_network_specific_events(events)
        
        # Combine the results
        if network_specific_transfers:
            balance_transfers.extend(network_specific_transfers)
            
        return balance_transfers
    
    def _process_network_specific_events(self, events: List[Dict]):
        """
        Process network-specific events. To be overridden by subclasses.
        
        Args:
            events: List of events to process
            
        Returns:
            List of balance transfers in the format:
            (extrinsic_id, event_idx, block_height, from_account, to_account, asset, asset_contract, amount, fee_amount, version)
        """
        # Base implementation does nothing
        return []