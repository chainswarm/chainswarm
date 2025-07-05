from typing import List, Dict, Any, Optional
import clickhouse_connect
import logging

logger = logging.getLogger(__name__)


class AssetsService:
    """Service for managing and retrieving asset information"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        """Initialize the Assets Service with database connection
        
        Args:
            connection_params: Dictionary with ClickHouse connection parameters
        """
        self.client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database'],
            settings={
                'async_insert': 0,
                'wait_for_async_insert': 1,
                'max_execution_time': 300,
                'max_query_size': 100000
            }
        )

    def get_assets(self, network: Optional[str] = None):
        """
        Get all assets without pagination
        
        Args:
            network: Optional network filter
            
        Returns:
            Dictionary containing all assets
        """
        try:
            params = {}
            network_filter = " WHERE network = {network:String}"
            params['network'] = network
            
            # Query to get all assets
            query = f"""
                    SELECT 
                        network,
                        asset_symbol,
                        asset_contract,
                        asset_verified,
                        asset_name,
                        asset_type,
                        decimals,
                        first_seen_block,
                        first_seen_timestamp,
                        last_updated,
                        updated_by,
                        notes
                    FROM assets FINAL
                    {network_filter}
                    ORDER BY network, asset_symbol
                    """
            
            result = self.client.query(query, parameters=params)
            
            # Get the result as a list of dictionaries
            assets = result.named_results()
            return assets
            
        except Exception as e:
            logger.error(f"Error getting all assets: {str(e)}")
            raise
    
    def get_verified_assets(self, network: Optional[str] = None):
        """
        Get all verified assets
        
        Args:
            network: Optional network filter
            
        Returns:
            Dictionary containing verified assets
        """
        try:
            params = {}
            query = """
                    SELECT
                        network,
                        asset_symbol,
                        asset_contract,
                        asset_verified,
                        asset_name,
                        asset_type,
                        decimals,
                        first_seen_block,
                        first_seen_timestamp,
                        last_updated,
                        updated_by,
                        notes
                    FROM assets FINAL
                    WHERE asset_verified = 'verified'
                    """
                    
            if network:
                query += " AND network = {network:String}"
                params['network'] = network
                
            query += " ORDER BY network, asset_symbol"
            
            result = self.client.query(query, parameters=params)
            assets = result.named_results()
            return assets
            
        except Exception as e:
            logger.error(f"Error getting verified assets: {str(e)}")
            raise
    
    def get_native_assets(self):
        """
        Get all native assets across all networks
        
        Returns:
            Dictionary containing native assets
        """
        try:
            query = """
                    SELECT
                        network,
                        asset_symbol,
                        asset_contract,
                        asset_verified,
                        asset_name,
                        asset_type,
                        decimals,
                        first_seen_block,
                        first_seen_timestamp,
                        last_updated,
                        updated_by,
                        notes
                    FROM assets FINAL
                    WHERE asset_type = 'native'
                    ORDER BY network, asset_symbol
                    """
            
            result = self.client.query(query)
            assets = result.named_results()
            return assets
            
        except Exception as e:
            logger.error(f"Error getting native assets: {str(e)}")
            raise
    
    def get_native_asset_symbol(self, network: str) -> str:
        """
        Get the native asset symbol for the specified network by querying the database.
        
        Args:
            network: The blockchain network identifier
            
        Returns:
            str: The native asset symbol for the network
            
        Raises:
            ValueError: If no native asset is found for the network
        """
        try:
            query = """
                    SELECT asset_symbol
                    FROM assets FINAL
                    WHERE network = {network:String}
                    AND asset_type = 'native'
                    LIMIT 1
                    """
            
            result = self.client.query(query, parameters={'network': network})
            
            if result.row_count == 0:
                raise ValueError(f"No native asset found in database for network: {network}")
                
            return result.first_row[0]
            
        except Exception as e:
            logger.error(f"Error getting native asset symbol for network {network}: {str(e)}")
            raise
    
    def validate_asset_contract(self, network: str, asset_contract: str) -> List[str]:
        """
        Validate an asset contract and return the corresponding asset symbols.
        
        Args:
            network: The blockchain network identifier
            asset_contract: The asset contract to validate. Special values:
                - 'all': Return all assets for the network
                - 'native': Return only the native asset for the network
                - specific contract address: Check if it exists and is supported
                
        Returns:
            List[str]: List of asset symbols corresponding to the validated asset contract
            
        Raises:
            ValueError: If the asset contract is not supported or not found
        """
        try:
            # Handle special values
            if asset_contract.lower() == 'all':
                # Return all asset symbols for the network
                query = """
                        SELECT asset_symbol
                        FROM assets FINAL
                        WHERE network = {network:String}
                        ORDER BY asset_symbol
                        """
                result = self.client.query(query, parameters={'network': network})
                if result.row_count == 0:
                    raise ValueError(f"No assets found for network: {network}")
                return [row[0] for row in result.result_rows]
                
            elif asset_contract.lower() == 'native':
                # Return only the native asset symbol
                return [self.get_native_asset_symbol(network)]
                
            else:
                # Check if the specific contract address exists and is supported
                query = """
                        SELECT asset_symbol, asset_verified
                        FROM assets FINAL
                        WHERE network = {network:String}
                        AND asset_contract = {contract:String}
                        LIMIT 1
                        """
                result = self.client.query(
                    query,
                    parameters={
                        'network': network,
                        'contract': asset_contract
                    }
                )
                
                if result.row_count == 0:
                    raise ValueError(f"Asset contract not found: {asset_contract} on network: {network}")
                    
                asset_symbol = result.first_row[0]
                asset_verified = result.first_row[1]
                
                if asset_verified == 'malicious':
                    raise ValueError(f"Asset contract is marked as malicious: {asset_contract}")
                    
                return [asset_symbol]
                
        except Exception as e:
            logger.error(f"Error validating asset contract {asset_contract} on network {network}: {str(e)}")
            raise
            
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'client'):
            self.client.close()
