import os
import json
import traceback
from typing import List, Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_tracking.balance_tracking_indexer import BalanceTrackingIndexer
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import data


class TorusBalanceTrackingIndexer(BalanceTrackingIndexer):
    """
    Torus-specific implementation of the BalanceTrackingIndexer.
    Handles Torus-specific events like staking and governance events.
    """
    
    def __init__(self, connection_params: Dict[str, Any], partitioner, network: str):
        """
        Initialize the TorusBalanceTrackingIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            partitioner: BlockRangePartitioner instance for table partitioning
            network: Network identifier (e.g., 'torus', 'torus_testnet')
        """
        super().__init__(connection_params, partitioner, network)
        
        # Initialize genesis balances for Torus networks
        self._init_genesis_balances()
    
    def _process_network_specific_events(self, events: List[Dict]):
        """
        Process Torus-specific events.
        
        Args:
            events: List of events to process
            
        Returns:
            List of balance transfers in the format:
            (extrinsic_id, event_idx, block_height, from_account, to_account, amount, fee_amount, version)
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
                        self.version
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
                        self.version
                    ))
                except Exception as e:
                    logger.warning(f"Error processing Treasury.Awarded event: {e}")
        
        return balance_transfers
    
    def _init_genesis_balances(self):
        """Initialize genesis balances for Torus networks if they don't exist yet"""
        try:
            # Check if we already have balance records
            last_processed_height = self.get_latest_processed_block_height()
            if last_processed_height > 0:
                logger.info("Balance records already exist, skipping genesis balance initialization")
                return
                
            # Check if genesis balances file exists
            file_path = os.path.join(os.path.dirname(os.path.abspath(data.__file__)), "torus-genesis-balances.json")
            if not os.path.exists(file_path):
                logger.warning(f"No genesis balances file found for Torus network at {file_path}, skipping initialization")
                return
            
            # Load genesis balances
            logger.info(f"Loading genesis balances from {file_path}")
            with open(file_path, 'r') as f:
                balances = json.load(f)
            
            genesis_balances = [(address, Decimal(amount)) for address, amount in balances]
            logger.info(f"Loaded {len(genesis_balances)} genesis balances")
            
            # Insert genesis balances
            self.insert_genesis_balances(genesis_balances, self.network)
            logger.success(f"Genesis balances initialized for Torus network: {self.network}")
            
        except Exception as e:
            logger.error(f"Error initializing genesis balances for Torus network: {e}", error=e, trb=traceback.format_exc())