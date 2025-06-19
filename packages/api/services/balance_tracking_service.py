from typing import Any, Dict, Optional, List
import clickhouse_connect


def get_balance_tracking_tables() -> List[str]:
    """Get the list of tables related to balance tracking
    
    Returns:
        List of table names
    """
    return [
        "balance_changes",
        "balance_delta_changes",
        "known_addresses",

        "balance_daily_statistics_mv",
        "balance_significant_changes_view",
        "balances_current_view"
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
        "name": "Balance Tracking Schema",
        "description": "Schema for balance tracking data in ClickHouse with multi-asset support",
        "tables": schema,
        "asset_support": {
            "description": "All balance tables include asset fields for multi-asset tracking",
            "asset_fields": ["asset", "asset_id"],
            "requested_assets": assets or ["Network native asset"]
        },
        "example_queries": [
            f"SELECT * FROM balance_changes FINAL WHERE address = 'YOUR_ADDRESS' AND {asset_filter} LIMIT 10",
            f"SELECT * FROM balance_delta_changes FINAL WHERE address = 'YOUR_ADDRESS' AND {asset_filter} AND total_balance_delta > 0 LIMIT 10",
            f"SELECT * FROM balance_transfers FINAL WHERE (from_address = 'YOUR_ADDRESS' OR to_address = 'YOUR_ADDRESS') AND {asset_filter} LIMIT 10",
            "SELECT asset, COUNT(*) as transfer_count, SUM(amount) as total_amount FROM balance_transfers FINAL GROUP BY asset",
            f"SELECT asset, address, SUM(total_balance_delta) as net_change FROM balance_delta_changes FINAL WHERE {asset_filter} GROUP BY asset, address ORDER BY net_change DESC LIMIT 10"
        ]
    }


class BalanceTrackingService:
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

    def get_address_balance_history(self, address: str, page: int = 1, page_size: int = 20, assets: List[str] = None):
        """Get balance history for a specific address
        
        Args:
            address: The blockchain address to query
            page: Page number for pagination
            page_size: Number of items per page
            
        Returns:
            Dictionary with paginated balance history
        """
        # Build asset filter
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            asset_filter = f" AND ({asset_conditions})"
        
        # Get total count for pagination
        count_query = f"""
            SELECT COUNT(*) AS total
            FROM balance_changes FINAL
            WHERE address = {{address:String}}{asset_filter}
        """
        count_result = self.client.query(count_query, {'address': address}).result_rows
        total_count = count_result[0][0] if count_result else 0
        
        # Calculate pagination parameters
        offset = (page - 1) * page_size
        
        # Query to fetch paginated balance history
        data_query = f"""
            SELECT
                block_height,
                block_timestamp,
                free_balance,
                reserved_balance,
                staked_balance,
                total_balance,
                asset
            FROM balance_changes FINAL
            WHERE address = {{address:String}}{asset_filter}
            ORDER BY block_height DESC
            LIMIT {{limit:Int}} OFFSET {{offset:Int}}
        """
        
        data_params = {'address': address, 'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows
        
        # Define the column names in the order they appear in the SELECT clause
        columns = [
            "block_height",
            "block_timestamp",
            "free_balance",
            "reserved_balance",
            "staked_balance",
            "total_balance",
            "asset"
        ]
        
        # Map each row into a dictionary using the column names
        balance_history = [dict(zip(columns, row)) for row in rows]
        
        # Format the response
        return format_paginated_response(
            items=balance_history,
            page=page,
            page_size=page_size,
            total_items=total_count
        )
    
    def get_address_balance_deltas(self, address: str, page: int = 1, page_size: int = 20, assets: List[str] = None):
        """Get balance deltas for a specific address
        
        Args:
            address: The blockchain address to query
            page: Page number for pagination
            page_size: Number of items per page
            
        Returns:
            Dictionary with paginated balance deltas
        """
        # Build asset filter
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            asset_filter = f" AND ({asset_conditions})"
        
        # Get total count for pagination
        count_query = f"""
            SELECT COUNT(*) AS total
            FROM balance_delta_changes FINAL
            WHERE address = {{address:String}}{asset_filter}
        """
        count_result = self.client.query(count_query, {'address': address}).result_rows
        total_count = count_result[0][0] if count_result else 0
        
        # Calculate pagination parameters
        offset = (page - 1) * page_size
        
        # Query to fetch paginated balance deltas
        data_query = f"""
            SELECT
                block_height,
                block_timestamp,
                free_balance_delta,
                reserved_balance_delta,
                staked_balance_delta,
                total_balance_delta,
                previous_block_height,
                asset
            FROM balance_delta_changes FINAL
            WHERE address = {{address:String}}{asset_filter}
            ORDER BY block_height DESC
            LIMIT {{limit:Int}} OFFSET {{offset:Int}}
        """
        
        data_params = {'address': address, 'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows
        
        # Define the column names in the order they appear in the SELECT clause
        columns = [
            "block_height",
            "block_timestamp",
            "free_balance_delta",
            "reserved_balance_delta",
            "staked_balance_delta",
            "total_balance_delta",
            "previous_block_height",
            "asset"
        ]
        
        # Map each row into a dictionary using the column names
        balance_deltas = [dict(zip(columns, row)) for row in rows]
        
        # Format the response
        return format_paginated_response(
            items=balance_deltas,
            page=page,
            page_size=page_size,
            total_items=total_count
        )
    
    def get_address_balance_at_block(self, address: str, block_height: int, assets: List[str] = None):
        """Get balance for a specific address at a specific block height
        
        Args:
            address: The blockchain address to query
            block_height: The block height to query at
            
        Returns:
            Dictionary with balance information or None if not found
        """
        # Build asset filter
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            asset_filter = f" AND ({asset_conditions})"
        
        query = f"""
            SELECT
                block_height,
                block_timestamp,
                free_balance,
                reserved_balance,
                staked_balance,
                total_balance,
                asset
            FROM balance_changes FINAL
            WHERE address = {{address:String}} AND block_height <= {{block_height:Int}}{asset_filter}
            ORDER BY block_height DESC
            LIMIT 1
        """
        
        params = {'address': address, 'block_height': block_height}
        result = self.client.query(query, params).result_rows
        
        if not result:
            return None
        
        # Format the response
        return {
            'address': address,
            'block_height': result[0][0],
            'block_timestamp': result[0][1],
            'free_balance': result[0][2],
            'reserved_balance': result[0][3],
            'staked_balance': result[0][4],
            'total_balance': result[0][5],
            'asset': result[0][6]
        }

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

    def get_latest_balances_for_addresses(self, addresses: List[str], assets: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get latest balances for multiple addresses efficiently by querying the most recent
        balance_account_changes entries for each address.

        Args:
            addresses: List of addresses to query

        Returns:
            Dictionary mapping addresses to their latest balance data
        """
        if not addresses:
            return {}

        # Build asset filter
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            asset_filter = f" AND ({asset_conditions})"

        # Format addresses for SQL IN clause
        formatted_addresses = ", ".join([f"'{addr}'" for addr in addresses])

        # Query to get the latest balance for each address
        query = f"""
        WITH latest_changes AS (
            SELECT
                address,
                argMax(total_balance, block_height) AS latest_balance,
                max(block_height) AS latest_block_height,
                argMax(block_timestamp, block_height) AS latest_timestamp,
                argMax(asset, block_height) AS latest_asset
            FROM balance_changes FINAL
            WHERE address IN ({formatted_addresses}){asset_filter}
            GROUP BY address
        )
        SELECT
            address,
            latest_balance,
            latest_block_height,
            latest_timestamp,
            latest_asset
        FROM latest_changes
        """

        result = self.client.query(query).result_rows

        # Format the response
        balances = {}
        for row in result:
            address = row[0]
            balances[address] = {
                'address': address,
                'balance': row[1],
                'block_height': row[2],
                'timestamp': row[3],
                'asset': row[4]
            }

        return balances