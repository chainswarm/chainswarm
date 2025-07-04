from typing import Any, Dict, Optional, List
import clickhouse_connect

from packages.api.services.balance_utils import format_paginated_response
from packages.api.services.asset_filter_utils import build_asset_filter


def get_balance_series_tables() -> List[str]:
    """Get the list of tables related to balance series
    
    Returns:
        List of table names
    """
    return [
        "balance_series",
        "balance_series_latest_view",
        "balance_series_daily_view",
        "balance_series_weekly_view",
        "balance_series_monthly_view",
    ]


def create_balance_series_schema(schema: Dict[str, Any], assets: List[str] = None) -> Dict[str, Any]:
    """Create a complete balance series schema with asset support
    
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
        "name": "Balance Series Schema",
        "description": "Schema for balance snapshot time-series data in ClickHouse",
        "tables": schema,
        "asset_support": {
            "description": "All balance series tables include asset fields",
            "asset_fields": ["asset"],
            "requested_assets": assets or ["Network native asset"]
        },
        "key_features": [
            "Time-series tracking with fixed 4-hour interval snapshots",
            "Multi-balance type support (free, reserved, staked, total)",
            "Change tracking between periods with both absolute and percentage metrics",
            "Multi-level time aggregation (4-hour, daily, weekly, monthly)",
            "Optimized views for efficient querying at different time scales"
        ]
    }


class BalanceSeriesService:
    def __init__(self, connection_params: Dict[str, Any]):
        """Initialize the Balance Series Service with database connection
        
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

    def get_address_balance_series(self, address: str, page: int, page_size: int, assets: List[str] = None,
                                 start_timestamp: Optional[int] = None, end_timestamp: Optional[int] = None,
                                 network: str = None):
        """
        Returns historical balance snapshots for a specific address with pagination

        Args:
            address: The blockchain address to query
            page: Page number for pagination
            page_size: Number of items per page
            assets: List of assets to filter by (can be symbols or contract addresses)
            start_timestamp: Optional start timestamp in milliseconds
            end_timestamp: Optional end timestamp in milliseconds

        Returns:
            Dictionary with paginated balance history
        """
        # Build asset filter using shared utility
        asset_filter = build_asset_filter(assets, table_type="balance_series")
        
        # Build timestamp filter
        timestamp_filter = ""
        if start_timestamp:
            timestamp_filter += f" AND period_start_timestamp >= {start_timestamp}"
        if end_timestamp:
            timestamp_filter += f" AND period_end_timestamp <= {end_timestamp}"

        count_query = f"""
                      SELECT COUNT(*) AS total
                      FROM balance_series FINAL
                      WHERE address = {{address:String}}{asset_filter}{timestamp_filter}
                      """
        
        # Build JOIN clause for assets table if network is provided
        join_clause = ""
        if network:
            join_clause = f" LEFT JOIN assets a ON bs.asset_contract = a.asset_contract AND a.network = '{network}'"
        
        data_query = f"""
                     SELECT bs.period_start_timestamp,
                            bs.period_end_timestamp,
                            bs.block_height,
                            bs.address,
                            bs.asset_symbol as asset,
                            bs.asset_contract,
                            COALESCE(a.asset_verified, 'unknown') as asset_verified,
                            COALESCE(a.asset_name, bs.asset_symbol) as asset_name,
                            bs.free_balance,
                            bs.reserved_balance,
                            bs.staked_balance,
                            bs.total_balance,
                            bs.free_balance_change,
                            bs.reserved_balance_change,
                            bs.staked_balance_change,
                            bs.total_balance_change,
                            bs.total_balance_percent_change
                     FROM (SELECT * FROM balance_series FINAL) AS bs
                     {join_clause}
                     WHERE bs.address = {{address:String}}{asset_filter}{timestamp_filter}
                     ORDER BY bs.period_start_timestamp DESC
                     LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                     """

        count_result = self.client.query(count_query, {'address': address}).result_rows
        total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters
        offset = (page - 1) * page_size

        # Query to fetch paginated balance series data
        data_params = {'address': address, 'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Define the column names in the order they appear in the SELECT clause
        columns = [
            "period_start_timestamp",
            "period_end_timestamp",
            "block_height",
            "address",
            "asset",
            "asset_contract",
            "asset_verified",
            "asset_name",
            "free_balance",
            "reserved_balance",
            "staked_balance",
            "total_balance",
            "free_balance_change",
            "reserved_balance_change",
            "staked_balance_change",
            "total_balance_change",
            "total_balance_percent_change"
        ]

        # Map each row (a list of values) into a dictionary using the column names
        balance_series = [dict(zip(columns, row)) for row in rows]

        # Use the standardized format_paginated_response utility function
        return format_paginated_response(
            items=balance_series,
            page=page,
            page_size=page_size,
            total_items=total_count
        )

    def get_current_balances(self, addresses: List[str], assets: List[str] = None, network: str = None):
        """
        Returns latest balance for addresses using balance_series_latest_view

        Args:
            addresses: List of blockchain addresses to query
            assets: List of assets to filter by (can be symbols or contract addresses)

        Returns:
            List of current balance records
        """
        from loguru import logger
        
        # Build asset filter using shared utility
        asset_filter = build_asset_filter(assets, table_type="balance_series")
        
        # Log the asset filter for debugging
        logger.debug(f"Asset filter generated: {asset_filter}")
        logger.debug(f"Assets requested: {assets}")
        
        # Build address filter
        address_conditions = " OR ".join([f"address = '{addr}'" for addr in addresses])
        address_filter = f"({address_conditions})"

        # Build JOIN clause for assets table if network is provided
        join_clause = ""
        if network:
            join_clause = f" LEFT JOIN assets a ON bslv.asset_contract = a.asset_contract AND a.network = '{network}'"

        query = f"""
                SELECT bslv.address,
                       bslv.asset_symbol as asset,
                       bslv.asset_contract,
                       COALESCE(a.asset_verified, 'unknown') as asset_verified,
                       COALESCE(a.asset_name, bslv.asset_symbol) as asset_name,
                       bslv.latest_period_start,
                       bslv.latest_period_end,
                       bslv.latest_block_height,
                       bslv.free_balance,
                       bslv.reserved_balance,
                       bslv.staked_balance,
                       bslv.total_balance
                FROM balance_series_latest_view bslv
                {join_clause}
                WHERE {address_filter}{asset_filter}
                ORDER BY bslv.address, bslv.asset_symbol
                """

        # Log the full query for debugging
        logger.debug(f"Balance series query: {query}")

        query_result = self.client.query(query)
        rows = query_result.result_rows

        # Define the column names
        columns = [
            "address",
            "asset",
            "asset_contract",
            "asset_verified",
            "asset_name",
            "latest_period_start",
            "latest_period_end",
            "latest_block_height",
            "free_balance",
            "reserved_balance",
            "staked_balance",
            "total_balance"
        ]

        # Map each row into a dictionary
        current_balances = [dict(zip(columns, row)) for row in rows]
        
        return {
            "items": current_balances,
            "total_items": len(current_balances)
        }

    def get_balance_changes(self, address: str, page: int, page_size: int, assets: List[str] = None,
                          min_change_threshold: Optional[float] = None, network: str = None):
        """
        Returns balance changes analysis for a specific address

        Args:
            address: The blockchain address to query
            page: Page number for pagination
            page_size: Number of items per page
            assets: List of assets to filter by (can be symbols or contract addresses)
            min_change_threshold: Minimum absolute change threshold to filter by

        Returns:
            Dictionary with paginated balance changes
        """
        # Build asset filter using shared utility
        asset_filter = build_asset_filter(assets, table_type="balance_series")
        
        # Build change threshold filter
        threshold_filter = ""
        if min_change_threshold is not None:
            threshold_filter = f" AND abs(total_balance_change) >= {min_change_threshold}"

        count_query = f"""
                      SELECT COUNT(*) AS total
                      FROM balance_series FINAL
                      WHERE address = {{address:String}} 
                        AND total_balance_change != 0{asset_filter}{threshold_filter}
                      """
        
        # Build JOIN clause for assets table if network is provided
        join_clause = ""
        if network:
            join_clause = f" LEFT JOIN assets a ON bs.asset_contract = a.asset_contract AND a.network = '{network}'"
        
        data_query = f"""
                     SELECT bs.period_start_timestamp,
                            bs.period_end_timestamp,
                            bs.block_height,
                            bs.address,
                            bs.asset_symbol as asset,
                            bs.asset_contract,
                            COALESCE(a.asset_verified, 'unknown') as asset_verified,
                            COALESCE(a.asset_name, bs.asset_symbol) as asset_name,
                            bs.total_balance,
                            bs.free_balance_change,
                            bs.reserved_balance_change,
                            bs.staked_balance_change,
                            bs.total_balance_change,
                            bs.total_balance_percent_change
                     FROM (SELECT * FROM balance_series FINAL) AS bs
                     {join_clause}
                     WHERE bs.address = {{address:String}}
                       AND bs.total_balance_change != 0{asset_filter}{threshold_filter}
                     ORDER BY abs(bs.total_balance_change) DESC, bs.period_start_timestamp DESC
                     LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                     """

        count_result = self.client.query(count_query, {'address': address}).result_rows
        total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters
        offset = (page - 1) * page_size

        # Query to fetch paginated balance changes
        data_params = {'address': address, 'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Define the column names
        columns = [
            "period_start_timestamp",
            "period_end_timestamp",
            "block_height",
            "address",
            "asset",
            "asset_contract",
            "asset_verified",
            "asset_name",
            "total_balance",
            "free_balance_change",
            "reserved_balance_change",
            "staked_balance_change",
            "total_balance_change",
            "total_balance_percent_change"
        ]

        # Map each row into a dictionary
        balance_changes = [dict(zip(columns, row)) for row in rows]

        # Use the standardized format_paginated_response utility function
        return format_paginated_response(
            items=balance_changes,
            page=page,
            page_size=page_size,
            total_items=total_count
        )

    def get_balance_aggregations(self, period: str, addresses: List[str] = None, assets: List[str] = None,
                               start_date: Optional[str] = None, end_date: Optional[str] = None,
                               network: str = None):
        """
        Returns daily/weekly/monthly balance aggregations

        Args:
            period: Aggregation period ('daily', 'weekly', 'monthly')
            addresses: Optional list of addresses to filter by
            assets: Optional list of assets to filter by (can be symbols or contract addresses)
            start_date: Optional start date (YYYY-MM-DD format)
            end_date: Optional end date (YYYY-MM-DD format)

        Returns:
            Dictionary with aggregated balance data
        """
        # Determine the table/view to use based on period
        if period == "daily":
            table = "balance_series_daily_view"
            date_column = "date"
        elif period == "weekly":
            table = "balance_series_weekly_view"
            date_column = "week_start"
        elif period == "monthly":
            table = "balance_series_monthly_view"
            date_column = "month_start"
        else:
            raise ValueError("Period must be 'daily', 'weekly', or 'monthly'")

        # Build filters
        filters = []
        
        if addresses:
            address_conditions = " OR ".join([f"address = '{addr}'" for addr in addresses])
            filters.append(f"({address_conditions})")
        
        asset_filter = build_asset_filter(assets, table_type="balance_series")
        if asset_filter:
            # Remove the leading " AND " from the filter
            filters.append(asset_filter[5:])
        
        if start_date:
            filters.append(f"{date_column} >= '{start_date}'")
        
        if end_date:
            filters.append(f"{date_column} <= '{end_date}'")

        where_clause = " WHERE " + " AND ".join(filters) if filters else ""

        # Build JOIN clause for assets table if network is provided
        join_clause = ""
        if network:
            join_clause = f" LEFT JOIN assets a ON {table}.asset_contract = a.asset_contract AND a.network = '{network}'"

        # Build the query based on period
        if period == "daily":
            query = f"""
                    SELECT {table}.date,
                           {table}.address,
                           {table}.asset_symbol as asset,
                           {table}.asset_contract,
                           COALESCE(a.asset_verified, 'unknown') as asset_verified,
                           COALESCE(a.asset_name, {table}.asset_symbol) as asset_name,
                           {table}.end_of_day_free_balance,
                           {table}.end_of_day_reserved_balance,
                           {table}.end_of_day_staked_balance,
                           {table}.end_of_day_total_balance,
                           {table}.daily_free_balance_change,
                           {table}.daily_reserved_balance_change,
                           {table}.daily_staked_balance_change,
                           {table}.daily_total_balance_change
                    FROM {table}
                    {join_clause}
                    {where_clause}
                    ORDER BY {table}.date DESC, {table}.address, {table}.asset_symbol
                    """
            columns = [
                "date", "address", "asset", "asset_contract", "asset_verified", "asset_name",
                "end_of_day_free_balance", "end_of_day_reserved_balance",
                "end_of_day_staked_balance", "end_of_day_total_balance",
                "daily_free_balance_change", "daily_reserved_balance_change",
                "daily_staked_balance_change", "daily_total_balance_change"
            ]
        elif period == "weekly":
            query = f"""
                    SELECT {table}.week_start,
                           {table}.address,
                           {table}.asset_symbol as asset,
                           {table}.asset_contract,
                           COALESCE(a.asset_verified, 'unknown') as asset_verified,
                           COALESCE(a.asset_name, {table}.asset_symbol) as asset_name,
                           {table}.end_of_week_free_balance,
                           {table}.end_of_week_reserved_balance,
                           {table}.end_of_week_staked_balance,
                           {table}.end_of_week_total_balance,
                           {table}.weekly_free_balance_change,
                           {table}.weekly_reserved_balance_change,
                           {table}.weekly_staked_balance_change,
                           {table}.weekly_total_balance_change,
                           {table}.last_block_of_week
                    FROM {table}
                    {join_clause}
                    {where_clause}
                    ORDER BY {table}.week_start DESC, {table}.address, {table}.asset_symbol
                    """
            columns = [
                "week_start", "address", "asset", "asset_contract", "asset_verified", "asset_name",
                "end_of_week_free_balance", "end_of_week_reserved_balance",
                "end_of_week_staked_balance", "end_of_week_total_balance",
                "weekly_free_balance_change", "weekly_reserved_balance_change",
                "weekly_staked_balance_change", "weekly_total_balance_change",
                "last_block_of_week"
            ]
        else:  # monthly
            query = f"""
                    SELECT {table}.month_start,
                           {table}.address,
                           {table}.asset_symbol as asset,
                           {table}.asset_contract,
                           COALESCE(a.asset_verified, 'unknown') as asset_verified,
                           COALESCE(a.asset_name, {table}.asset) as asset_name,
                           {table}.end_of_month_free_balance,
                           {table}.end_of_month_reserved_balance,
                           {table}.end_of_month_staked_balance,
                           {table}.end_of_month_total_balance,
                           {table}.monthly_free_balance_change,
                           {table}.monthly_reserved_balance_change,
                           {table}.monthly_staked_balance_change,
                           {table}.monthly_total_balance_change,
                           {table}.last_block_of_month
                    FROM {table}
                    {join_clause}
                    {where_clause}
                    ORDER BY {table}.month_start DESC, {table}.address, {table}.asset_symbol
                    """
            columns = [
                "month_start", "address", "asset", "asset_contract", "asset_verified", "asset_name",
                "end_of_month_free_balance", "end_of_month_reserved_balance",
                "end_of_month_staked_balance", "end_of_month_total_balance",
                "monthly_free_balance_change", "monthly_reserved_balance_change",
                "monthly_staked_balance_change", "monthly_total_balance_change",
                "last_block_of_month"
            ]

        query_result = self.client.query(query)
        rows = query_result.result_rows

        # Map each row into a dictionary
        aggregations = [dict(zip(columns, row)) for row in rows]
        
        return {
            "period": period,
            "items": aggregations,
            "total_items": len(aggregations)
        }

    def get_balance_volume_series(self, page: int = 1, page_size: int = 20, assets: List[str] = None,
                                start_timestamp: Optional[int] = None, end_timestamp: Optional[int] = None):
        """
        Returns balance volume series showing network-wide balance activity metrics over time
        
        Args:
            page: Page number for pagination
            page_size: Number of items per page
            assets: List of assets to filter by (can be symbols or contract addresses)
            start_timestamp: Optional start timestamp in milliseconds
            end_timestamp: Optional end timestamp in milliseconds
            
        Returns:
            Dictionary with paginated balance volume series data
        """
        # Build asset filter using shared utility
        asset_filter = build_asset_filter(assets, table_type="balance_series")
        
        # Build timestamp filter
        timestamp_filter = ""
        if start_timestamp:
            timestamp_filter += f" AND period_start_timestamp >= {start_timestamp}"
        if end_timestamp:
            timestamp_filter += f" AND period_end_timestamp <= {end_timestamp}"

        # Count query for pagination
        count_query = f"""
                      SELECT COUNT(DISTINCT period_start_timestamp, asset_symbol) AS total
                      FROM balance_series FINAL
                      WHERE 1=1{asset_filter}{timestamp_filter}
                      """
        
        # Main data query - aggregate balance activity by period and asset
        data_query = f"""
                     SELECT
                         period_start_timestamp,
                         period_end_timestamp,
                         asset_symbol as asset,
                         COUNT(DISTINCT address) as active_addresses_count,
                         SUM(ABS(total_balance_change)) as total_balance_changes,
                         SUM(ABS(free_balance_change)) as total_free_balance_changes,
                         SUM(ABS(reserved_balance_change)) as total_reserved_balance_changes,
                         SUM(ABS(staked_balance_change)) as total_staked_balance_changes
                     FROM (SELECT * FROM balance_series FINAL) AS bs
                     WHERE 1=1{asset_filter}{timestamp_filter}
                     GROUP BY period_start_timestamp, period_end_timestamp, asset_symbol
                     ORDER BY period_start_timestamp DESC, asset_symbol
                     LIMIT {{limit:Int}} OFFSET {{offset:Int}}
                     """

        count_result = self.client.query(count_query).result_rows
        total_count = count_result[0][0] if count_result else 0

        # Calculate pagination parameters
        offset = (page - 1) * page_size

        # Query to fetch paginated balance volume series data
        data_params = {'limit': page_size, 'offset': offset}
        query_result = self.client.query(data_query, data_params)
        rows = query_result.result_rows

        # Define the column names in the order they appear in the SELECT clause
        columns = [
            "period_start_timestamp",
            "period_end_timestamp",
            "asset",
            "active_addresses_count",
            "total_balance_changes",
            "total_free_balance_changes",
            "total_reserved_balance_changes",
            "total_staked_balance_changes"
        ]

        # Map each row into a dictionary
        volume_series = [dict(zip(columns, row)) for row in rows]

        # Use the standardized format_paginated_response utility function
        return format_paginated_response(
            items=volume_series,
            page=page,
            page_size=page_size,
            total_items=total_count
        )