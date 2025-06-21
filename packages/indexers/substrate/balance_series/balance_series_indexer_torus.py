import os
import json
import traceback
from typing import List, Dict, Any
from loguru import logger
from decimal import Decimal

from packages.indexers.substrate.balance_series.balance_series_indexer import BalanceSeriesIndexer
from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import data


class TorusBalanceSeriesIndexer(BalanceSeriesIndexer):
    """
    Torus-specific implementation of the BalanceSeriesIndexer.
    Handles Torus-specific balance series functionality and genesis balance initialization.
    """
    
    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """
        Initialize the TorusBalanceSeriesIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            network: Network identifier (e.g., 'torus', 'torus_testnet')
            period_hours: Number of hours in each period (default: 4)
        """
        super().__init__(connection_params, network, period_hours)
        
        # Initialize genesis balances for Torus networks
        self._init_genesis_balances()
    
    
    def _init_genesis_balances(self):
        """Initialize genesis balances for Torus networks if they don't exist yet"""
        try:
            # Check if we already have balance records
            last_processed_timestamp, _ = self.get_latest_processed_period()
            if last_processed_timestamp > 0:
                logger.info("Balance series records already exist, skipping genesis balance initialization")
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