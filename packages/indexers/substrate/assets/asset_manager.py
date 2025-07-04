"""
Asset Manager for handling asset dictionary operations.

This module provides functionality to manage assets (both native and tokens)
in the assets dictionary table, including initialization, verification,
and ensuring assets exist before they are referenced in other tables.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
import clickhouse_connect
from packages.indexers.base.enhanced_logging import get_logger

logger = get_logger(__name__)


class AssetManager:
    """Manages asset dictionary operations for a specific network."""
    
    def __init__(self, network: str, connection_params: Dict[str, Any]):
        """
        Initialize the AssetManager.
        
        Args:
            network: The blockchain network (e.g., 'torus', 'bittensor', 'polkadot')
            connection_params: ClickHouse connection parameters
        """
        self.network = network
        self.client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database']
        )
        self.logger = logger
        
        # Cache for asset lookups to avoid repeated database queries
        self._asset_cache = {}
        
        # Native asset configurations
        self.NATIVE_ASSETS = {
            'torus': {
                'symbol': 'TOR',
                'name': 'Torus',
                'decimals': 18
            },
            'bittensor': {
                'symbol': 'TAO',
                'name': 'Bittensor',
                'decimals': 18  # Fixed from 9 to 18
            },
            'polkadot': {
                'symbol': 'DOT',
                'name': 'Polkadot',
                'decimals': 10
            }
        }
        
        self.logger.info(f"Initialized AssetManager for network: {network}")
        
    def initialize_native_assets(self) -> None:
        """
        Initialize native assets for the network if they don't exist.
        This ensures the native asset is always present in the assets table.
        """
        if self.network not in self.NATIVE_ASSETS:
            self.logger.warning(f"No native asset configuration for network: {self.network}")
            return
            
        native_config = self.NATIVE_ASSETS[self.network]
        
        try:
            # Check if native asset already exists
            result = self.client.execute(
                """
                SELECT asset_symbol 
                FROM assets 
                WHERE network = %(network)s 
                  AND asset_contract = 'native'
                LIMIT 1
                """,
                {'network': self.network}
            )
            
            if not result:
                # Insert native asset
                self.client.execute(
                    """
                    INSERT INTO assets (
                        network, asset_symbol, asset_contract, asset_verified,
                        asset_name, asset_type, decimals, first_seen_block,
                        first_seen_timestamp, updated_by
                    ) VALUES (
                        %(network)s, %(symbol)s, 'native', 'verified',
                        %(name)s, 'native', %(decimals)s, 0,
                        %(timestamp)s, 'system'
                    )
                    """,
                    {
                        'network': self.network,
                        'symbol': native_config['symbol'],
                        'name': native_config['name'],
                        'decimals': native_config['decimals'],
                        'timestamp': datetime.now()
                    }
                )
                self.logger.info(
                    f"Initialized native asset {native_config['symbol']} for {self.network}"
                )
            else:
                self.logger.debug(
                    f"Native asset {native_config['symbol']} already exists for {self.network}"
                )
                
        except Exception as e:
            self.logger.error(
                f"Failed to initialize native asset for {self.network}: {str(e)}",
                exc_info=True
            )
            raise
            
    def ensure_asset_exists(
        self,
        asset_symbol: str,
        asset_contract: str = '',
        asset_type: Optional[str] = None,
        decimals: int = 0,
        first_seen_block: Optional[int] = None,
        first_seen_timestamp: Optional[datetime] = None,
        asset_name: Optional[str] = None,
        notes: Optional[str] = None
    ) -> bool:
        """
        Ensure an asset exists in the assets table.
        
        Args:
            asset_symbol: The asset symbol/ticker
            asset_contract: Contract address or empty for native assets
            asset_type: 'native' or 'token' (auto-determined if not provided)
            decimals: Number of decimal places
            first_seen_block: Block height where asset was first seen
            first_seen_timestamp: Timestamp when asset was first seen
            asset_name: Full display name of the asset
            notes: Additional notes about the asset
            
        Returns:
            bool: True if asset was created, False if it already existed
        """
        try:
            # Determine asset type if not provided
            if not asset_type:
                asset_type = 'native' if not asset_contract else 'token'
            
            # For native assets, ensure contract is 'native'
            if asset_type == 'native' or not asset_contract:
                asset_contract = 'native'
            
            # Check cache first
            cache_key = f"{self.network}:{asset_contract}"
            if cache_key in self._asset_cache:
                self.logger.debug(
                    f"Asset found in cache: {asset_symbol} "
                    f"(contract: {asset_contract}, network: {self.network})"
                )
                return False
                
            # Check if asset already exists in database
            result = self.client.execute(
                """
                SELECT asset_symbol, asset_verified, last_updated
                FROM assets
                WHERE network = %(network)s
                  AND asset_contract = %(contract)s
                LIMIT 1
                """,
                {
                    'network': self.network,
                    'contract': asset_contract
                }
            )
            
            if result:
                # Add to cache
                self._asset_cache[cache_key] = {
                    'symbol': result[0][0],
                    'verified': result[0][1],
                    'last_updated': result[0][2]
                }
                self.logger.debug(
                    f"Asset already exists: {asset_symbol} "
                    f"(contract: {asset_contract}, network: {self.network})"
                )
                return False
                
            # Prepare asset data
            asset_data = {
                'network': self.network,
                'symbol': asset_symbol,
                'contract': asset_contract,
                'type': asset_type,
                'decimals': decimals,
                'block': first_seen_block or 0,
                'timestamp': first_seen_timestamp or datetime.now(),
                'name': asset_name or asset_symbol,
                'notes': notes or ''
            }
            
            # Insert new asset
            self.client.execute(
                """
                INSERT INTO assets (
                    network, asset_symbol, asset_contract, asset_verified,
                    asset_name, asset_type, decimals, first_seen_block,
                    first_seen_timestamp, updated_by, notes
                ) VALUES (
                    %(network)s, %(symbol)s, %(contract)s, 'unknown',
                    %(name)s, %(type)s, %(decimals)s, %(block)s,
                    %(timestamp)s, 'indexer', %(notes)s
                )
                """,
                asset_data
            )
            
            # Add to cache after successful creation
            self._asset_cache[cache_key] = {
                'symbol': asset_symbol,
                'verified': 'unknown',
                'last_updated': datetime.now()
            }
            
            self.logger.info(
                f"Created new asset: {asset_symbol} "
                f"(contract: {asset_contract}, type: {asset_type}, network: {self.network})"
            )
            return True
            
        except Exception as e:
            self.logger.error(
                f"Failed to ensure asset exists: {asset_symbol} "
                f"(contract: {asset_contract}): {str(e)}",
                exc_info=True
            )
            raise
            
    def get_asset_info(self, asset_contract: str) -> Optional[Dict[str, Any]]:
        """
        Get asset information by contract address.
        
        Args:
            asset_contract: Contract address or 'native'
            
        Returns:
            Dict with asset information or None if not found
        """
        try:
            # Check cache first
            cache_key = f"{self.network}:{asset_contract}"
            if cache_key in self._asset_cache:
                self.logger.debug(f"Asset info found in cache for contract {asset_contract}")
                # Return cached basic info, but still query for full details
                
            result = self.client.execute(
                """
                SELECT 
                    asset_symbol,
                    asset_contract,
                    asset_verified,
                    asset_name,
                    asset_type,
                    decimals,
                    first_seen_block,
                    first_seen_timestamp,
                    last_updated,
                    notes
                FROM assets 
                WHERE network = %(network)s 
                  AND asset_contract = %(contract)s
                LIMIT 1
                """,
                {
                    'network': self.network,
                    'contract': asset_contract
                }
            )
            
            if result:
                row = result[0]
                asset_info = {
                    'symbol': row[0],
                    'contract': row[1],
                    'verified': row[2],
                    'name': row[3],
                    'type': row[4],
                    'decimals': row[5],
                    'first_seen_block': row[6],
                    'first_seen_timestamp': row[7],
                    'last_updated': row[8],
                    'notes': row[9]
                }
                
                # Update cache with full info
                self._asset_cache[cache_key] = {
                    'symbol': asset_info['symbol'],
                    'verified': asset_info['verified'],
                    'last_updated': asset_info['last_updated']
                }
                
                return asset_info
            return None
            
        except Exception as e:
            self.logger.error(
                f"Failed to get asset info for contract {asset_contract}: {str(e)}",
                exc_info=True
            )
            return None
            
    def update_asset_verification(
        self,
        asset_contract: str,
        verification_status: str,
        updated_by: str = 'admin',
        notes: Optional[str] = None
    ) -> bool:
        """
        Update the verification status of an asset.
        
        Args:
            asset_contract: Contract address or 'native'
            verification_status: 'verified', 'unknown', or 'malicious'
            updated_by: Who is updating the status
            notes: Additional notes about the verification
            
        Returns:
            bool: True if updated successfully
        """
        if verification_status not in ['verified', 'unknown', 'malicious']:
            raise ValueError(
                f"Invalid verification status: {verification_status}. "
                "Must be 'verified', 'unknown', or 'malicious'"
            )
            
        try:
            update_data = {
                'network': self.network,
                'contract': asset_contract,
                'status': verification_status,
                'updated_by': updated_by,
                'timestamp': datetime.now()
            }
            
            # Build update query
            update_parts = [
                "asset_verified = %(status)s",
                "updated_by = %(updated_by)s",
                "last_updated = %(timestamp)s"
            ]
            
            if notes is not None:
                update_parts.append("notes = %(notes)s")
                update_data['notes'] = notes
                
            query = f"""
                ALTER TABLE assets
                UPDATE {', '.join(update_parts)}
                WHERE network = %(network)s 
                  AND asset_contract = %(contract)s
            """
            
            self.client.execute(query, update_data)
            
            # Update cache if asset is cached
            cache_key = f"{self.network}:{asset_contract}"
            if cache_key in self._asset_cache:
                self._asset_cache[cache_key]['verified'] = verification_status
                self._asset_cache[cache_key]['last_updated'] = datetime.now()
            
            self.logger.info(
                f"Updated asset verification: {asset_contract} -> {verification_status} "
                f"(network: {self.network}, by: {updated_by})"
            )
            return True
            
        except Exception as e:
            self.logger.error(
                f"Failed to update asset verification for {asset_contract}: {str(e)}",
                exc_info=True
            )
            return False
    
    def get_native_asset_symbol(self) -> str:
        """
        Get the native asset symbol for the current network.
        
        Returns:
            str: The native asset symbol (e.g., 'TOR', 'TAO', 'DOT')
        """
        if self.network in self.NATIVE_ASSETS:
            return self.NATIVE_ASSETS[self.network]['symbol']
        raise ValueError(f"No native asset configured for network: {self.network}")
    
    def clear_cache(self) -> None:
        """Clear the asset cache."""
        self._asset_cache.clear()
        self.logger.info(f"Cleared asset cache for network: {self.network}")