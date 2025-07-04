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
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'client'):
            self.client.close()
