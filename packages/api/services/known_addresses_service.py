import os
from typing import List, Dict, Any, Optional
from loguru import logger
from clickhouse_connect import get_client

class KnownAddressesService:
    """Service for managing known blockchain addresses"""
    
    def __init__(self, clickhouse_params: Dict[str, Any]):
        """
        Initialize the KnownAddressesService
        
        Args:
            clickhouse_params: ClickHouse connection parameters
        """
        self.clickhouse_params = clickhouse_params
        self.client = get_client(
            host=clickhouse_params['host'],
            port=int(clickhouse_params['port']),
            username=clickhouse_params['user'],
            password=clickhouse_params['password'],
            database=clickhouse_params['database']
        )
    
    def format_paginated_response(self, items, page, page_size, total_items):
        """
        Format response for paginated endpoints
        
        Args:
            items: List of items to include in the response
            page: Current page number
            page_size: Number of items per page
            total_items: Total number of items available
            
        Returns:
            Dictionary with standardized pagination metadata
        """
        total_pages = (total_items + page_size - 1) // page_size if page_size > 0 else 0
        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "total_items": total_items
        }
    
    
    async def get_known_addresses(
        self,
        network: str,
        addresses: Optional[List[str]] = None,
        page: int = 1,
        page_size: int = 100
    ) -> Dict[str, Any]:
        """
        Get known addresses for a network with pagination
        
        Args:
            network: The blockchain network identifier
            addresses: Optional list of addresses to filter by
            page: Page number (1-based)
            page_size: Number of items per page
            
        Returns:
            Dictionary with known addresses and pagination info
        """
        
        try:
            # Calculate offset
            offset = (page - 1) * page_size
            
            # Build the query with FINAL to get the latest versions
            query = "SELECT id, network, address, label, source, source_type, last_updated FROM known_addresses FINAL WHERE network = {network:String}"
            params = {"network": network}
            
            # Add address filter if provided
            if addresses and len(addresses) > 0:
                query += " AND address IN {addresses:Array(String)}"
                params["addresses"] = addresses
            
            # Get total count
            count_query = query.replace("SELECT id, network, address, label, source, source_type, last_updated", "SELECT COUNT(*)")
            count_result = self.client.query(count_query, parameters=params)
            total_count = count_result.result_rows[0][0] if count_result.result_rows else 0
            
            # Add pagination
            query += " ORDER BY label, address LIMIT {limit:Int} OFFSET {offset:Int}"
            params["limit"] = page_size
            params["offset"] = offset
            
            # Execute the query
            result = self.client.query(query, parameters=params)
            
            # Format the results
            addresses_list = []
            for row in result.result_rows:
                addresses_list.append({
                    "id": str(row[0]),
                    "network": row[1],
                    "address": row[2],
                    "label": row[3],
                    "source": row[4],
                    "source_type": row[5],
                    "last_updated": row[6].isoformat() if row[6] else None
                })
            
            # Use the standardized format_paginated_response utility function
            return self.format_paginated_response(
                items=addresses_list,
                page=page,
                page_size=page_size,
                total_items=total_count
            )
        except Exception as e:
            logger.error(f"Error getting known addresses: {str(e)}")
            raise