from typing import Any, Dict, Optional, List
import clickhouse_connect

from packages.api.services.balance_utils import format_paginated_response


def get_balance_transfers_tables() -> List[str]:
    """Get the list of tables related to balance tracking
    
    Returns:
        List of table names
    """
    return [
        "balance_transfers",
        "balance_transfers_daily_volume_mv",
        "balance_transfers_statistics_view",
    ]


def create_balance_tracking_schema(schema: Dict[str, Any], assets: List[str] = None) -> Dict[str, Any]:
    """Create a complete balance tracking schema with asset support
    
    Args:
        schema: Raw schema dictionary
        assets: List of asset symbols for example queries
        
    Returns:
        Complete schema with metadata and asset support information
    """
    # Default asset examples
    asset_examples = assets if assets else ["TOR"]
    asset_filter = f"asset = '{asset_examples[0]}'" if len(asset_examples) == 1 else f"asset IN {tuple(asset_examples)}"
    
    return {
        "name": "Balance Transfers Schema",
        "description": "Schema for balance transfers data in ClickHouse",
        "tables": schema,
        "asset_support": {
            "description": "All balance transfer tables include asset fields",
            "asset_fields": ["asset", "asset_id"],
            "requested_assets": assets or ["Network native asset"]
        },
        "example_queries": [
        ]
    }

class BalanceTransferService:
    def __init__(self, connection_params: Dict[str, Any]):
        """Initialize the Balance Tracking Service with database connection
        
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

    def close(self):
        """Close the ClickHouse connection"""
        if hasattr(self, 'client'):
            self.client.close()

    def get_address_transactions(self, address, target_address: Optional[str], page, page_size, assets: List[str] = None):
        """
        Returns transaction history for a specific address with pagination

        Args:
            address: The blockchain address to query
            target_address: Optional target address to filter transactions
            page: Page number for pagination
            page_size: Number of items per page

        Returns:
            Dictionary with paginated transaction history
        """
        # Build asset filter
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            asset_filter = f" AND ({asset_conditions})"
        
        if target_address:
            count_query = f"""
                          SELECT COUNT(*) AS total
                          FROM balance_transfers FINAL
                          WHERE from_address = {{address:String}} AND to_address = {{target_address:String}}{asset_filter}
                          """
            data_query = f"""
                         SELECT bt.extrinsic_id,
                                bt.event_idx,
                                bt.block_height,
                                bt.from_address,
                                bt.to_address,
                                bt.amount,
                                bt.fee,
                                bt.block_timestamp,
                                bt.asset
                         FROM (SELECT * FROM balance_transfers FINAL) AS bt
                         WHERE bt.from_address = {{address:String}} AND bt.to_address = {{target_address:String}}{asset_filter}
                         ORDER BY bt.block_height DESC
                         LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                         """
            count_result = self.client.query(count_query,
                                             {'address': address, 'target_address': target_address}).result_rows
            total_count = count_result[0][0] if count_result else 0
        else:
            count_query = f"""
                          SELECT COUNT(*) AS total
                          FROM balance_transfers FINAL
                          WHERE (from_address = {{address:String}} OR to_address = {{address:String}}){asset_filter}
                          """
            data_query = f"""
                         SELECT bt.extrinsic_id,
                                bt.event_idx,
                                bt.block_height,
                                bt.from_address,
                                bt.to_address,
                                bt.amount,
                                bt.fee,
                                bt.block_timestamp,
                                bt.asset
                         FROM (SELECT * FROM balance_transfers FINAL) AS bt
                         WHERE (bt.from_address = {{address:String}} OR bt.to_address = {{address:String}}){asset_filter}
                         ORDER BY bt.block_height DESC
                         LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                         """
            count_result = self.client.query(count_query, {'address': address}).result_rows
            total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters.
        offset = (page - 1) * page_size

        # Query to fetch paginated transactions joined with blocks for the block timestamp.
        data_params = {'address': address, 'limit': page_size, 'offset': offset, 'target_address': target_address}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Define the column names in the order they appear in the SELECT clause.
        columns = [
            "extrinsic_id",
            "event_idx",
            "block_height",
            "from_address",
            "to_address",
            "amount",
            "fee",
            "block_timestamp",
            "asset"
        ]

        # Map each row (a list of values) into a dictionary using the column names.
        transactions = [dict(zip(columns, row)) for row in rows]

        # Use the standardized format_paginated_response utility function
        return format_paginated_response(
            items=transactions,
            page=page,
            page_size=page_size,
            total_items=total_count
        )

    def get_addresses_from_transaction_id(self, transaction_id: str, assets: List[str] = None):
        # Build asset filter
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            asset_filter = f" AND ({asset_conditions})"
        
        count_query = f"""
                      SELECT DISTINCT address
                      FROM (SELECT arrayJoin([from_address, to_address]) AS address
                            FROM balance_transfers FINAL
                            WHERE extrinsic_id = {{extrinsic_id:String}}{asset_filter})
                      """
        result = self.client.query(count_query, {'extrinsic_id': transaction_id}).result_rows
        if not result:
            return None
        return [row[0] for row in result]

    def get_addresses_from_block_height(self, block_height: int, assets: List[str] = None):
        # Build asset filter
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            asset_filter = f" AND ({asset_conditions})"
        
        count_query = f"""
                      SELECT DISTINCT address
                      FROM (SELECT arrayJoin([from_address, to_address]) AS address
                            FROM balance_transfers FINAL
                            WHERE block_height = {{block_height: Int}}{asset_filter})
                      """
        result = self.client.query(count_query, {'block_height': block_height}).result_rows
        if not result:
            return None
        return [row[0] for row in result]

