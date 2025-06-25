from typing import List, Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_transfers.balance_transfers_indexer import BalanceTransfersIndexer
from packages.indexers.base.decimal_utils import convert_to_decimal_units


class BittensorBalanceTransfersIndexer(BalanceTransfersIndexer):
    """
    Bittensor-specific implementation of the BalanceTransfersIndexer.
    Handles Bittensor-specific transfer events like neuron staking and TAO transfers.
    """
    
    def __init__(self, connection_params: Dict[str, Any], partitioner, network: str):
        """
        Initialize the BittensorBalanceTransfersIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'bittensor', 'bittensor_testnet')
        """
        super().__init__(connection_params, partitioner, network)
    
    def _process_network_specific_events(self, events: List[Dict]):
        """
        Process Bittensor-specific transfer events.
        
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
            
            # Process Bittensor-specific staking events
            for event in events_by_type.get('SubtensorModule.StakeAdded', []):
                try:
                    self._validate_event_structure(event, ['hotkey', 'coldkey', 'amount_staked'])
                    hotkey = event['attributes']['hotkey']
                    coldkey = event['attributes']['coldkey']
                    amount = convert_to_decimal_units(event['attributes']['amount_staked'], self.network)
                    
                    # Record as a transfer from coldkey to hotkey
                    balance_transfers.append((
                        event['extrinsic_id'],
                        event['event_idx'],
                        event['block_height'],
                        coldkey,  # From coldkey
                        hotkey,   # To hotkey
                        self.asset,  # Use network asset (TAO)
                        amount,
                        Decimal(0),  # No fee recorded for staking
                        self.version
                    ))
                except Exception as e:
                    logger.warning(f"Error processing SubtensorModule.StakeAdded event: {e}")
            
            # Process stake removal events
            for event in events_by_type.get('SubtensorModule.StakeRemoved', []):
                try:
                    self._validate_event_structure(event, ['hotkey', 'coldkey', 'amount_unstaked'])
                    hotkey = event['attributes']['hotkey']
                    coldkey = event['attributes']['coldkey']
                    amount = convert_to_decimal_units(event['attributes']['amount_unstaked'], self.network)
                    
                    # Record as a transfer from hotkey back to coldkey
                    balance_transfers.append((
                        event['extrinsic_id'],
                        event['event_idx'],
                        event['block_height'],
                        hotkey,   # From hotkey
                        coldkey,  # To coldkey
                        self.asset,  # Use network asset (TAO)
                        amount,
                        Decimal(0),  # No fee recorded for unstaking
                        self.version
                    ))
                except Exception as e:
                    logger.warning(f"Error processing SubtensorModule.StakeRemoved event: {e}")
            
            # Process emission events
            for event in events_by_type.get('SubtensorModule.EmissionReceived', []):
                try:
                    self._validate_event_structure(event, ['hotkey', 'amount'])
                    hotkey = event['attributes']['hotkey']
                    amount = convert_to_decimal_units(event['attributes']['amount'], self.network)
                    
                    # Record as a transfer from the "emission" to the hotkey
                    balance_transfers.append((
                        event['extrinsic_id'],
                        event['event_idx'],
                        event['block_height'],
                        'emission',  # From emission
                        hotkey,      # To hotkey
                        self.asset,  # Use network asset (TAO)
                        amount,
                        Decimal(0),  # No fee for emissions
                        self.version
                    ))
                except Exception as e:
                    logger.warning(f"Error processing SubtensorModule.EmissionReceived event: {e}")
        
        return balance_transfers