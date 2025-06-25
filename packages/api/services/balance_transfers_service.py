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
        "balance_transfers_daily_volume_view",
        "balance_transfers_statistics_view",
    ]


def create_balance_transfers_schema(schema: Dict[str, Any], assets: List[str] = None) -> Dict[str, Any]:
    """Create a complete balance transfers schema with asset support
    
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

    def get_balance_volume_series(self, page: int = 1, page_size: int = 20, assets: List[str] = None,
                                start_timestamp: Optional[int] = None, end_timestamp: Optional[int] = None,
                                period_type: str = "4hour"):
        """
        Returns balance transfers volume series data providing network-wide transfer activity metrics
        
        Args:
            page: Page number for pagination
            page_size: Number of items per page
            assets: List of assets to filter by
            start_timestamp: Optional start timestamp in milliseconds
            end_timestamp: Optional end timestamp in milliseconds
            period_type: Period type for aggregation ("4hour", "daily", "weekly", "monthly")
            
        Returns:
            Dictionary with paginated balance volume series data
        """
        # Build asset filter
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            asset_filter = f" AND ({asset_conditions})"
        
        # Build timestamp filter based on period type
        timestamp_filter = ""
        if period_type == "4hour":
            # Use the materialized view for 4-hour periods
            table = "balance_transfers_volume_series_view"
            if start_timestamp:
                timestamp_filter += f" AND period_start >= toDateTime({start_timestamp}/1000)"
            if end_timestamp:
                timestamp_filter += f" AND period_end <= toDateTime({end_timestamp}/1000)"
                
            count_query = f"""
                          SELECT COUNT(*) AS total
                          FROM {table}
                          WHERE 1=1{asset_filter}{timestamp_filter}
                          """
            
            data_query = f"""
                         SELECT period_start,
                                period_end,
                                asset,
                                transaction_count,
                                unique_senders,
                                unique_receivers,
                                active_addresses,
                                total_volume,
                                total_fees,
                                avg_transfer_amount,
                                max_transfer_amount,
                                min_transfer_amount,
                                median_transfer_amount,
                                network_density,
                                period_start_block,
                                period_end_block,
                                blocks_in_period,
                                -- Histogram bins
                                tx_count_lt_01,
                                tx_count_01_to_1,
                                tx_count_1_to_10,
                                tx_count_10_to_100,
                                tx_count_100_to_1k,
                                tx_count_1k_to_10k,
                                tx_count_gte_10k,
                                -- Volume bins
                                volume_lt_01,
                                volume_01_to_1,
                                volume_1_to_10,
                                volume_10_to_100,
                                volume_100_to_1k,
                                volume_1k_to_10k,
                                volume_gte_10k
                         FROM {table}
                         WHERE 1=1{asset_filter}{timestamp_filter}
                         ORDER BY period_start DESC
                         LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                         """
            
            columns = [
                "period_start", "period_end", "asset", "transaction_count", "unique_senders",
                "unique_receivers", "active_addresses", "total_volume", "total_fees",
                "avg_transfer_amount", "max_transfer_amount", "min_transfer_amount",
                "median_transfer_amount", "network_density", "period_start_block",
                "period_end_block", "blocks_in_period", "tx_count_lt_01", "tx_count_01_to_1",
                "tx_count_1_to_10", "tx_count_10_to_100", "tx_count_100_to_1k",
                "tx_count_1k_to_10k", "tx_count_gte_10k", "volume_lt_01", "volume_01_to_1",
                "volume_1_to_10", "volume_10_to_100", "volume_100_to_1k", "volume_1k_to_10k",
                "volume_gte_10k"
            ]
            
        elif period_type == "daily":
            table = "balance_transfers_network_daily_view"
            if start_timestamp:
                timestamp_filter += f" AND period >= toDate(toDateTime({start_timestamp}/1000))"
            if end_timestamp:
                timestamp_filter += f" AND period <= toDate(toDateTime({end_timestamp}/1000))"
                
            count_query = f"""
                          SELECT COUNT(*) AS total
                          FROM {table}
                          WHERE 1=1{asset_filter}{timestamp_filter}
                          """
            
            data_query = f"""
                         SELECT period,
                                asset,
                                transaction_count,
                                total_volume,
                                max_unique_senders,
                                max_unique_receivers,
                                unique_addresses,
                                avg_network_density,
                                total_fees,
                                avg_transaction_size,
                                max_transaction_size,
                                min_transaction_size,
                                avg_fee,
                                max_fee,
                                min_fee,
                                median_transaction_size,
                                -- Histogram bins
                                tx_count_lt_01,
                                tx_count_01_to_1,
                                tx_count_1_to_10,
                                tx_count_10_to_100,
                                tx_count_100_to_1k,
                                tx_count_1k_to_10k,
                                tx_count_gte_10k
                         FROM {table}
                         WHERE 1=1{asset_filter}{timestamp_filter}
                         ORDER BY period DESC
                         LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                         """
            
            columns = [
                "period", "asset", "transaction_count", "total_volume", "max_unique_senders",
                "max_unique_receivers", "unique_addresses", "avg_network_density", "total_fees",
                "avg_transaction_size", "max_transaction_size", "min_transaction_size",
                "avg_fee", "max_fee", "min_fee", "median_transaction_size",
                "tx_count_lt_01", "tx_count_01_to_1", "tx_count_1_to_10", "tx_count_10_to_100",
                "tx_count_100_to_1k", "tx_count_1k_to_10k", "tx_count_gte_10k"
            ]
            
        elif period_type == "weekly":
            table = "balance_transfers_network_weekly_view"
            if start_timestamp:
                timestamp_filter += f" AND period >= toStartOfWeek(toDateTime({start_timestamp}/1000))"
            if end_timestamp:
                timestamp_filter += f" AND period <= toStartOfWeek(toDateTime({end_timestamp}/1000))"
                
            count_query = f"""
                          SELECT COUNT(*) AS total
                          FROM {table}
                          WHERE 1=1{asset_filter}{timestamp_filter}
                          """
            
            data_query = f"""
                         SELECT period,
                                asset,
                                transaction_count,
                                total_volume,
                                max_unique_senders,
                                max_unique_receivers,
                                unique_addresses,
                                avg_network_density,
                                total_fees,
                                avg_transaction_size,
                                max_transaction_size,
                                min_transaction_size,
                                avg_fee,
                                max_fee,
                                min_fee,
                                median_transaction_size,
                                -- Histogram bins
                                tx_count_lt_01,
                                tx_count_01_to_1,
                                tx_count_1_to_10,
                                tx_count_10_to_100,
                                tx_count_100_to_1k,
                                tx_count_1k_to_10k,
                                tx_count_gte_10k
                         FROM {table}
                         WHERE 1=1{asset_filter}{timestamp_filter}
                         ORDER BY period DESC
                         LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                         """
            
            columns = [
                "period", "asset", "transaction_count", "total_volume", "max_unique_senders",
                "max_unique_receivers", "unique_addresses", "avg_network_density", "total_fees",
                "avg_transaction_size", "max_transaction_size", "min_transaction_size",
                "avg_fee", "max_fee", "min_fee", "median_transaction_size",
                "tx_count_lt_01", "tx_count_01_to_1", "tx_count_1_to_10", "tx_count_10_to_100",
                "tx_count_100_to_1k", "tx_count_1k_to_10k", "tx_count_gte_10k"
            ]
            
        elif period_type == "monthly":
            table = "balance_transfers_network_monthly_view"
            if start_timestamp:
                timestamp_filter += f" AND period >= toStartOfMonth(toDateTime({start_timestamp}/1000))"
            if end_timestamp:
                timestamp_filter += f" AND period <= toStartOfMonth(toDateTime({end_timestamp}/1000))"
                
            count_query = f"""
                          SELECT COUNT(*) AS total
                          FROM {table}
                          WHERE 1=1{asset_filter}{timestamp_filter}
                          """
            
            data_query = f"""
                         SELECT period,
                                asset,
                                transaction_count,
                                total_volume,
                                max_unique_senders,
                                max_unique_receivers,
                                unique_addresses,
                                avg_network_density,
                                total_fees,
                                avg_transaction_size,
                                max_transaction_size,
                                min_transaction_size,
                                avg_fee,
                                max_fee,
                                min_fee,
                                median_transaction_size,
                                period_start_block,
                                period_end_block,
                                blocks_in_period,
                                -- Histogram bins
                                tx_count_lt_01,
                                tx_count_01_to_1,
                                tx_count_1_to_10,
                                tx_count_10_to_100,
                                tx_count_100_to_1k,
                                tx_count_1k_to_10k,
                                tx_count_gte_10k
                         FROM {table}
                         WHERE 1=1{asset_filter}{timestamp_filter}
                         ORDER BY period DESC
                         LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                         """
            
            columns = [
                "period", "asset", "transaction_count", "total_volume", "max_unique_senders",
                "max_unique_receivers", "unique_addresses", "avg_network_density", "total_fees",
                "avg_transaction_size", "max_transaction_size", "min_transaction_size",
                "avg_fee", "max_fee", "min_fee", "median_transaction_size",
                "period_start_block", "period_end_block", "blocks_in_period",
                "tx_count_lt_01", "tx_count_01_to_1", "tx_count_1_to_10", "tx_count_10_to_100",
                "tx_count_100_to_1k", "tx_count_1k_to_10k", "tx_count_gte_10k"
            ]
        else:
            raise ValueError("Period type must be '4hour', 'daily', 'weekly', or 'monthly'")

        # Execute count query
        count_result = self.client.query(count_query).result_rows
        total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters
        offset = (page - 1) * page_size

        # Execute data query
        data_params = {'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Map each row into a dictionary
        volume_series = [dict(zip(columns, row)) for row in rows]

        # Use the standardized format_paginated_response utility function
        return format_paginated_response(
            items=volume_series,
            page=page,
            page_size=page_size,
            total_items=total_count
        )

    def get_network_analytics(self, period: str, page: int = 1, page_size: int = 20, assets: List[str] = None,
                            start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Returns network analytics for daily, weekly, or monthly periods
        
        Args:
            period: Period type ('daily', 'weekly', 'monthly')
            page: Page number for pagination
            page_size: Number of items per page
            assets: List of assets to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Dictionary with paginated network analytics data
        """
        # Determine the table/view to use based on period
        if period == "daily":
            table = "balance_transfers_network_daily_view"
            date_column = "period"
        elif period == "weekly":
            table = "balance_transfers_network_weekly_view"
            date_column = "period"
        elif period == "monthly":
            table = "balance_transfers_network_monthly_view"
            date_column = "period"
        else:
            raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")

        # Build filters
        filters = []
        
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            filters.append(f"({asset_conditions})")
        
        if start_date:
            filters.append(f"{date_column} >= '{start_date}'")
        
        if end_date:
            filters.append(f"{date_column} <= '{end_date}'")

        where_clause = " WHERE " + " AND ".join(filters) if filters else ""

        count_query = f"""
                      SELECT COUNT(*) AS total
                      FROM {table}
                      {where_clause}
                      """

        data_query = f"""
                     SELECT *
                     FROM {table}
                     {where_clause}
                     ORDER BY {date_column} DESC
                     LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                     """

        count_result = self.client.query(count_query).result_rows
        total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters
        offset = (page - 1) * page_size

        # Execute data query
        data_params = {'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Get column names from the query result
        columns = query_result.column_names

        # Map each row into a dictionary
        analytics = [dict(zip(columns, row)) for row in rows]

        return format_paginated_response(
            items=analytics,
            page=page,
            page_size=page_size,
            total_items=total_count
        )

    def get_address_analytics(self, page: int = 1, page_size: int = 20, assets: List[str] = None,
                            address_type: Optional[str] = None, min_volume: Optional[float] = None):
        """
        Returns comprehensive address analytics with behavioral classification
        
        Args:
            page: Page number for pagination
            page_size: Number of items per page
            assets: List of assets to filter by
            address_type: Filter by address type classification
            min_volume: Minimum total volume filter
            
        Returns:
            Dictionary with paginated address analytics data
        """
        # Build filters
        filters = []
        
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            filters.append(f"({asset_conditions})")
        
        if address_type:
            filters.append(f"address_type = '{address_type}'")
            
        if min_volume is not None:
            filters.append(f"total_volume >= {min_volume}")

        where_clause = " WHERE " + " AND ".join(filters) if filters else ""

        count_query = f"""
                      SELECT COUNT(*) AS total
                      FROM balance_transfers_address_analytics_view
                      {where_clause}
                      """

        data_query = f"""
                     SELECT *
                     FROM balance_transfers_address_analytics_view
                     {where_clause}
                     ORDER BY total_volume DESC
                     LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                     """

        count_result = self.client.query(count_query).result_rows
        total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters
        offset = (page - 1) * page_size

        # Execute data query
        data_params = {'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Get column names from the query result
        columns = query_result.column_names

        # Map each row into a dictionary
        analytics = [dict(zip(columns, row)) for row in rows]

        return format_paginated_response(
            items=analytics,
            page=page,
            page_size=page_size,
            total_items=total_count
        )

    def get_volume_aggregations(self, period: str, page: int = 1, page_size: int = 20, assets: List[str] = None,
                              start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Returns volume aggregations for daily, weekly, or monthly periods
        
        Args:
            period: Period type ('daily', 'weekly', 'monthly')
            page: Page number for pagination
            page_size: Number of items per page
            assets: List of assets to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Dictionary with paginated volume aggregation data
        """
        # Determine the table/view to use based on period
        if period == "daily":
            table = "balance_transfers_volume_daily_view"
            date_column = "date"
        elif period == "weekly":
            table = "balance_transfers_volume_weekly_view"
            date_column = "week_start"
        elif period == "monthly":
            table = "balance_transfers_volume_monthly_view"
            date_column = "month_start"
        else:
            raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")

        # Build filters
        filters = []
        
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            filters.append(f"({asset_conditions})")
        
        if start_date:
            filters.append(f"{date_column} >= '{start_date}'")
        
        if end_date:
            filters.append(f"{date_column} <= '{end_date}'")

        where_clause = " WHERE " + " AND ".join(filters) if filters else ""

        count_query = f"""
                      SELECT COUNT(*) AS total
                      FROM {table}
                      {where_clause}
                      """

        data_query = f"""
                     SELECT *
                     FROM {table}
                     {where_clause}
                     ORDER BY {date_column} DESC
                     LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                     """

        count_result = self.client.query(count_query).result_rows
        total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters
        offset = (page - 1) * page_size

        # Execute data query
        data_params = {'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Get column names from the query result
        columns = query_result.column_names

        # Map each row into a dictionary
        aggregations = [dict(zip(columns, row)) for row in rows]

        return format_paginated_response(
            items=aggregations,
            page=page,
            page_size=page_size,
            total_items=total_count
        )

    def get_volume_trends(self, page: int = 1, page_size: int = 20, assets: List[str] = None,
                        start_timestamp: Optional[int] = None, end_timestamp: Optional[int] = None):
        """
        Returns volume trends with rolling averages for trend analysis
        
        Args:
            page: Page number for pagination
            page_size: Number of items per page
            assets: List of assets to filter by
            start_timestamp: Optional start timestamp in milliseconds
            end_timestamp: Optional end timestamp in milliseconds
            
        Returns:
            Dictionary with paginated volume trends data
        """
        # Build filters
        filters = []
        
        if assets and assets != ["all"]:
            asset_conditions = " OR ".join([f"asset = '{asset}'" for asset in assets])
            filters.append(f"({asset_conditions})")
        
        if start_timestamp:
            filters.append(f"period_start >= toDateTime({start_timestamp}/1000)")
        
        if end_timestamp:
            filters.append(f"period_start <= toDateTime({end_timestamp}/1000)")

        where_clause = " WHERE " + " AND ".join(filters) if filters else ""

        count_query = f"""
                      SELECT COUNT(*) AS total
                      FROM balance_transfers_volume_trends_view
                      {where_clause}
                      """

        data_query = f"""
                     SELECT *
                     FROM balance_transfers_volume_trends_view
                     {where_clause}
                     ORDER BY period_start DESC
                     LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                     """

        count_result = self.client.query(count_query).result_rows
        total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters
        offset = (page - 1) * page_size

        # Execute data query
        data_params = {'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Get column names from the query result
        columns = query_result.column_names

        # Map each row into a dictionary
        trends = [dict(zip(columns, row)) for row in rows]

        return format_paginated_response(
            items=trends,
            page=page,
            page_size=page_size,
            total_items=total_count
        )
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

