from typing import Dict, Any, Optional
import clickhouse_connect
from packages.api.services.assets_service import AssetsService
import logging

logger = logging.getLogger(__name__)


class AssetsTool:
    """Tool for querying asset information"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params
        self.assets_service = AssetsService(connection_params)
    
    def get_all_assets(self, network: Optional[str] = None) -> Dict[str, Any]:
        """
        Get all assets without pagination
        
        Args:
            network: Optional network filter
            
        Returns:
            Dictionary containing all assets
        """
        try:
            return self.assets_service.get_assets(network=network)
        except Exception as e:
            logger.error(f"Error getting all assets: {str(e)}")
            return {"error": str(e), "assets": []}
    
    def get_verified_assets(self, network: Optional[str] = None) -> Dict[str, Any]:
        """
        Get all verified assets
        
        Args:
            network: Optional network filter
            
        Returns:
            Dictionary containing verified assets
        """
        try:
            return self.assets_service.get_verified_assets(network=network)
        except Exception as e:
            logger.error(f"Error getting verified assets: {str(e)}")
            return {"error": str(e), "assets": []}
    
    def get_native_assets(self) -> Dict[str, Any]:
        """
        Get all native assets across all networks
        
        Returns:
            Dictionary containing native assets
        """
        try:
            return self.assets_service.get_native_assets()
        except Exception as e:
            logger.error(f"Error getting native assets: {str(e)}")
            return {"error": str(e), "assets": []}
    
    async def assets_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a custom SQL query against the assets table
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results
        """
        try:
            client = clickhouse_connect.get_client(
                host=self.connection_params['host'],
                port=int(self.connection_params['port']),
                username=self.connection_params['user'],
                password=self.connection_params['password'],
                database=self.connection_params['database']
            )
            
            # Execute query
            result = client.execute(query)
            
            # Get column names from the query result
            if result:
                # Try to extract column names from the query
                columns = []
                if hasattr(client, 'last_query') and hasattr(client.last_query, 'column_names'):
                    columns = client.last_query.column_names
                else:
                    # Fallback: try to parse from query or use generic names
                    import re
                    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
                    if select_match:
                        select_clause = select_match.group(1)
                        # Simple parsing - this won't handle all cases perfectly
                        columns = [col.strip().split()[-1].split('.')[-1] for col in select_clause.split(',')]
                    else:
                        # Generic column names
                        columns = [f"column_{i}" for i in range(len(result[0]))] if result else []
                
                # Convert to list of dictionaries
                data = []
                for row in result:
                    row_dict = {}
                    for i, value in enumerate(row):
                        col_name = columns[i] if i < len(columns) else f"column_{i}"
                        # Convert datetime objects to strings
                        if hasattr(value, 'isoformat'):
                            value = value.isoformat()
                        row_dict[col_name] = value
                    data.append(row_dict)
                
                return {
                    "success": True,
                    "data": data,
                    "row_count": len(data),
                    "columns": columns
                }
            else:
                return {
                    "success": True,
                    "data": [],
                    "row_count": 0,
                    "columns": []
                }
                
        except Exception as e:
            logger.error(f"Error executing assets query: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": []
            }
        finally:
            if 'client' in locals():
                client.close()
    
    def schema(self) -> Dict[str, Any]:
        """
        Get the schema information for assets table
        
        Returns:
            Schema information
        """
        return {
            "tables": {
                "assets": {
                    "description": "Dictionary table storing information about all assets across networks",
                    "columns": {
                        "network": "Network name (e.g., 'torus', 'bittensor', 'polkadot')",
                        "asset_symbol": "Asset symbol (e.g., 'TOR', 'TAO', 'DOT')",
                        "asset_contract": "Contract address or 'native' for native assets",
                        "asset_verified": "Verification status ('verified', 'unverified', 'unknown')",
                        "asset_name": "Full asset name",
                        "asset_type": "Asset type ('native' or 'token')",
                        "decimals": "Number of decimal places",
                        "first_seen_block": "Block height when asset was first seen",
                        "first_seen_timestamp": "Timestamp when asset was first seen",
                        "last_updated": "Last update timestamp",
                        "updated_by": "Who updated the record",
                        "notes": "Additional notes about the asset"
                    },
                    "indexes": [
                        "PRIMARY KEY (network, asset_contract)",
                        "INDEX idx_assets_symbol (network, asset_symbol)",
                        "INDEX idx_assets_type (network, asset_type)",
                        "INDEX idx_assets_verified (network, asset_verified)"
                    ]
                }
            },
            "example_queries": [
                {
                    "description": "Get all assets for a network",
                    "query": "SELECT * FROM assets WHERE network = 'torus' ORDER BY asset_symbol"
                },
                {
                    "description": "Get all verified native assets",
                    "query": "SELECT * FROM assets WHERE asset_type = 'native' AND asset_verified = 'verified'"
                },
                {
                    "description": "Get asset details by symbol",
                    "query": "SELECT * FROM assets WHERE network = 'torus' AND asset_symbol = 'TOR'"
                },
                {
                    "description": "Count assets by network and type",
                    "query": "SELECT network, asset_type, count(*) as count FROM assets GROUP BY network, asset_type"
                }
            ]
        }
    
    def close(self):
        """Close the assets service connection"""
        if hasattr(self, 'assets_service'):
            self.assets_service.close()