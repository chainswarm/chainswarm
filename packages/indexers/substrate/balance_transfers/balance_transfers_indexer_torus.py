from typing import List, Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_transfers.balance_transfers_indexer import BalanceTransfersIndexer
from packages.indexers.base.decimal_utils import convert_to_decimal_units


class TorusBalanceTransfersIndexer(BalanceTransfersIndexer):
    """
    Torus-specific implementation of the BalanceTransfersIndexer.
    Handles Torus-specific transfer events like staking and governance events.
    """
    
    def __init__(self, connection_params: Dict[str, Any], partitioner, network: str, metrics):
        """
        Initialize the TorusBalanceTransfersIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'torus', 'torus_testnet')
            metrics: IndexerMetrics instance for recording metrics (required)
        """
        super().__init__(connection_params, partitioner, network, metrics)
    
    def _process_network_specific_events(self, events: List[Dict]):
        """
        Process Torus-specific transfer events.
        
        Args:
            events: List of events to process
            
        Returns:
            List of balance transfers in the format:
            (extrinsic_id, event_idx, block_height, from_account, to_account, asset, amount, fee_amount, version)
        """
        # Event tracking
        balance_transfers = []
        
        # Group events by extrinsic for proper handling
        grouped_by_extrinsic, extrinsic_order = self._group_events(events)
        
        for extrinsic_idx in extrinsic_order:
            events_by_type = grouped_by_extrinsic[extrinsic_idx]
            
            if 'System.ExtrinsicFailed' in events_by_type:
                continue
            
            # Process Torus-specific staking events
            for event in events_by_type.get('Staking.Reward', []):
                try:
                    self._validate_event_structure(event, ['stash', 'amount'])
                    stash_account = event['attributes']['stash']
                    reward_amount = convert_to_decimal_units(event['attributes']['amount'], self.network)
                    
                    # Record as a transfer from the "system" to the stash account
                    balance_transfers.append((
                        event['extrinsic_id'],
                        event['event_idx'],
                        event['block_height'],
                        'system',  # From the system
                        stash_account,  # To the stash account
                        self.asset,  # Use network asset (TOR)
                        reward_amount,
                        Decimal(0),  # No fee for rewards
                        str(event.get('block_height'))
                    ))
                except Exception as e:
                    logger.warning(f"Error processing Staking.Reward event: {e}")
            
            # Process Torus-specific governance events
            for event in events_by_type.get('Treasury.Awarded', []):
                try:
                    self._validate_event_structure(event, ['proposal_index', 'award', 'account'])
                    recipient = event['attributes']['account']
                    award_amount = convert_to_decimal_units(event['attributes']['award'], self.network)
                    
                    # Record as a transfer from the "treasury" to the recipient
                    balance_transfers.append((
                        event['extrinsic_id'],
                        event['event_idx'],
                        event['block_height'],
                        'treasury',  # From the treasury
                        recipient,  # To the recipient
                        self.asset,  # Use network asset (TOR)
                        award_amount,
                        Decimal(0),  # No fee for treasury awards
                        str(event.get('block_height'))
                    ))
                except Exception as e:
                    logger.warning(f"Error processing Treasury.Awarded event: {e}")
        
        return balance_transfers