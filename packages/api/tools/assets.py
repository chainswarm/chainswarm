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