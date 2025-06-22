import os
import json
import traceback
from typing import List, Dict, Any, Optional
from loguru import logger
from decimal import Decimal

from packages.indexers.base.decimal_utils import convert_to_decimal_units
from packages.indexers.substrate import data
from packages.indexers.substrate.balance_series.balance_series_indexer_base import BalanceSeriesIndexerBase


class TorusBalanceSeriesIndexer(BalanceSeriesIndexerBase):

    def __init__(self, connection_params: Dict[str, Any], network: str, period_hours: int = 4):
        """
        Initialize the TorusBalanceSeriesIndexer.
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
            network: Network identifier (e.g., 'torus', 'torus_testnet')
            period_hours: Number of hours in each period (default: 4)
        """
        super().__init__(connection_params, network, period_hours)

    def init_genesis_balances(self, first_block_info):
        """Initialize genesis balances for Torus networks if they don't exist yet"""
        try:
            last_processed_timestamp, _ = self.get_latest_processed_period()
            if last_processed_timestamp > 0:
                logger.info("Balance series records already exist, skipping genesis balance initialization")
                return
                
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

            self.insert_genesis_balances(
                genesis_balances,
                self.network,
                first_block_info['block_height'],
                first_block_info['block_hash'],
                first_block_info['timestamp']
                )

            logger.success(f"Genesis balances initialized for Torus network: {self.network}")
            
        except Exception as e:
            logger.error(f"Error initializing genesis balances for Torus network: {e}", error=e, trb=traceback.format_exc())
            raise e
    
    def insert_genesis_balances(self, genesis_balances, network, block_height, block_hash, block_timestamp):
        """Insert genesis balances into the balance_series table for Torus network
        
        Args:
            genesis_balances: List of (address, amount) tuples
            network: Network identifier (e.g., 'torus')
            block_height: Height of the first block (default: 0)
            block_hash: Hash of the first block (default: None, will use 'genesis')
            block_timestamp: Timestamp of the first block in milliseconds (default: None)
                            If provided, use this as the period_start_timestamp
                            instead of 0 (Unix epoch)
        """

        try:
            period_start_timestamp = block_timestamp
            actual_block_hash = block_hash
            period_end_timestamp = period_start_timestamp + self.period_ms
            
            # Prepare data for balance_series insertion
            balance_data = []
            for address, amount in genesis_balances:
                # Convert genesis balances to decimal units
                free_balance = convert_to_decimal_units(amount, network)
                reserved_balance = Decimal(0)
                staked_balance = Decimal(0)
                total_balance = free_balance + reserved_balance + staked_balance
                
                # For genesis balances, there are no previous balances, so changes are the same as current balances
                balance_data.append((
                    period_start_timestamp,
                    period_end_timestamp,
                    block_height,
                    actual_block_hash,
                    address,
                    self.asset,
                    free_balance,
                    reserved_balance,
                    staked_balance,
                    total_balance,
                    free_balance,  # free_balance_change = free_balance for genesis
                    reserved_balance,  # reserved_balance_change = reserved_balance for genesis
                    staked_balance,  # staked_balance_change = staked_balance for genesis
                    total_balance,  # total_balance_change = total_balance for genesis
                    Decimal(0),  # No percentage change for genesis
                    self.version
                ))
            
            # Insert data in batches to avoid memory issues
            batch_size = 1000
            for i in range(0, len(balance_data), batch_size):
                batch = balance_data[i:i + batch_size]
                self.client.insert('balance_series', batch, column_names=[
                    'period_start_timestamp', 'period_end_timestamp', 'block_height', 'block_hash',
                    'address', 'asset', 'free_balance', 'reserved_balance', 'staked_balance', 'total_balance',
                    'free_balance_change', 'reserved_balance_change', 'staked_balance_change', 'total_balance_change',
                    'total_balance_percent_change', '_version'
                ])
                logger.info(f"Inserted batch {i//batch_size + 1}/{(len(balance_data) + batch_size - 1)//batch_size} of genesis balance records")
            
            logger.success(f"Successfully inserted {len(balance_data)} genesis balance records for Torus network")
            
        except Exception as e:
            logger.error(f"Error inserting genesis records for Torus: {e}")
            raise
