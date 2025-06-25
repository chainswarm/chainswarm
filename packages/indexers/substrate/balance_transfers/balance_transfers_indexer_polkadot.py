from typing import List, Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_transfers.balance_transfers_indexer import BalanceTransfersIndexer
from packages.indexers.base.decimal_utils import convert_to_decimal_units


class PolkadotBalanceTransfersIndexer(BalanceTransfersIndexer):
    """
    Polkadot-specific implementation of the BalanceTransfersIndexer.
    Handles Polkadot-specific transfer events like staking, crowdloans, and governance events.
    """
    
    def __init__(self, connection_params: Dict[str, Any], partitioner, network: str):
        """
        Initialize the PolkadotBalanceTransfersIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'polkadot')
        """
        super().__init__(connection_params, partitioner, network)
    
    def _process_network_specific_events(self, events: List[Dict]):
        """
        Process Polkadot-specific transfer events.
        
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
            
            # Process Polkadot staking reward events
            for event in events_by_type.get('Staking.Rewarded', []):
                try:
                    self._validate_event_structure(event, ['stash', 'amount'])
                    stash_account = event['attributes']['stash']
                    reward_amount = convert_to_decimal_units(event['attributes']['amount'], self.network)
                    
                    # Record as a transfer from the "staking" to the stash account
                    balance_transfers.append((
                        event['extrinsic_id'],
                        event['event_idx'],
                        event['block_height'],
                        'staking',  # From the staking system
                        stash_account,  # To the stash account
                        self.asset,  # Use network asset (DOT)
                        reward_amount,
                        Decimal(0),  # No fee for rewards
                        self.version
                    ))
                except Exception as e:
                    logger.warning(f"Error processing Staking.Rewarded event: {e}")
            
            # Process Polkadot treasury disbursement events
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
                        self.asset,  # Use network asset (DOT)
                        award_amount,
                        Decimal(0),  # No fee for treasury awards
                        self.version
                    ))
                except Exception as e:
                    logger.warning(f"Error processing Treasury.Awarded event: {e}")
            
            # Process Polkadot crowdloan contribution events
            for event in events_by_type.get('Crowdloan.Contributed', []):
                try:
                    self._validate_event_structure(event, ['who', 'fund_index', 'amount'])
                    contributor = event['attributes']['who']
                    fund_index = event['attributes']['fund_index']
                    amount = convert_to_decimal_units(event['attributes']['amount'], self.network)
                    
                    # Record as a transfer from the contributor to the crowdloan fund
                    fund_address = f"crowdloan-{fund_index}"
                    balance_transfers.append((
                        event['extrinsic_id'],
                        event['event_idx'],
                        event['block_height'],
                        contributor,  # From the contributor
                        fund_address,  # To the crowdloan fund
                        self.asset,  # Use network asset (DOT)
                        amount,
                        Decimal(0),  # No fee recorded for contributions
                        self.version
                    ))
                except Exception as e:
                    logger.warning(f"Error processing Crowdloan.Contributed event: {e}")
            
            # Process parachain auction bid events
            for event in events_by_type.get('Auctions.BidAccepted', []):
                try:
                    self._validate_event_structure(event, ['bidder', 'para_id', 'amount'])
                    bidder = event['attributes']['bidder']
                    para_id = event['attributes']['para_id']
                    amount = convert_to_decimal_units(event['attributes']['amount'], self.network)
                    
                    # Record as a transfer from the bidder to the auction system
                    auction_address = f"auction-{para_id}"
                    balance_transfers.append((
                        event['extrinsic_id'],
                        event['event_idx'],
                        event['block_height'],
                        bidder,  # From the bidder
                        auction_address,  # To the auction system
                        self.asset,  # Use network asset (DOT)
                        amount,
                        Decimal(0),  # No fee recorded for bids
                        self.version
                    ))
                except Exception as e:
                    logger.warning(f"Error processing Auctions.BidAccepted event: {e}")
        
        return balance_transfers