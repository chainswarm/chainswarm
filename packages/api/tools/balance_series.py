from typing import Any, Dict, Optional, List, Tuple
import clickhouse_connect
from loguru import logger
from dataclasses import dataclass

@dataclass
class TableColumn:
    name: str
    type: str
    description: str = ""

@dataclass
class TableInfo:
    name: str
    description: str = ""
    columns: List[TableColumn] = None


class BalanceSeriesAnalyticsTool:
    def __init__(self, connection_params: Dict[str, Any]):
        """Initialize the BalanceSeriesAnalyticsTool with ClickHouse connection parameters"""

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

        self.database = connection_params['database']
        
        # Balance series related tables and views
        self.tables = {
            # Core table for balance snapshots
            "balance_series",
            
            # Views for balance series analysis
            "balance_series_latest_view",
            "balance_series_daily_view",
            
            # Materialized views for aggregated balance series data
            "balance_series_weekly_view",
            "balance_series_monthly_view",
            
            # Additional context tables
            "known_addresses",
        }

    async def schema(self) -> Dict[str, Any]:
        """
        Get balance series schema from ClickHouse for balance snapshots over time

        Returns:
            Dict containing the balance series schema with time-series balance data
        """
        try:
            table_info_list = []
            for table in self.tables:
                table_info = TableInfo(name=table)
                table_info_list.append(table_info)

            for table in table_info_list:
                query = f"DESCRIBE TABLE {self.database}.{table.name}"
                result = self.client.query(query)

                columns = []
                for row in result.result_rows:
                    column_name = row[0]
                    column_type = row[1]
                    columns.append(TableColumn(name=column_name, type=column_type))
                table.columns = columns

            schema = {}
            for table in table_info_list:
                table_schema = {
                    "description": table.description,
                    "columns": {}
                }
                if table.columns:
                    for column in table.columns:
                        table_schema["columns"][column.name] = {
                            "type": column.type,
                            "description": column.description
                        }

                schema[table.name] = table_schema

            # Add descriptions for balance series tables and views
            if "balance_series" in schema:
                schema["balance_series"]["description"] = "Main table storing balance snapshots at fixed 4-hour intervals with free, reserved, staked, and total balances for each address and asset. Includes asset verification information for enhanced security and transparency."
                
                # Add column descriptions for balance_series table
                if "period_start_timestamp" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["period_start_timestamp"]["description"] = "Start of the 4-hour period - Unix timestamp in milliseconds"
                
                if "period_end_timestamp" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["period_end_timestamp"]["description"] = "End of the 4-hour period - Unix timestamp in milliseconds"
                
                if "block_height" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["block_height"]["description"] = "Block height at the end of the period"
                
                if "address" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["address"]["description"] = "Account address being tracked"
                
                if "asset" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["asset"]["description"] = "Token or currency being tracked"
                
                if "asset_contract" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["asset_contract"]["description"] = "Contract address for tokens, 'native' for native blockchain assets"
                
                if "asset_verified" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["asset_verified"]["description"] = "Verification status: verified, unknown, or malicious"
                
                if "asset_name" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["asset_name"]["description"] = "Human-readable name of the asset"
                
                if "free_balance" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["free_balance"]["description"] = "Freely available balance that can be transferred"
                
                if "reserved_balance" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["reserved_balance"]["description"] = "Balance reserved for specific operations"
                
                if "staked_balance" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["staked_balance"]["description"] = "Balance staked for network participation"
                
                if "total_balance" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["total_balance"]["description"] = "Sum of free, reserved, and staked balances"
                
                if "free_balance_change" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["free_balance_change"]["description"] = "Absolute change in free balance since previous period"
                
                if "reserved_balance_change" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["reserved_balance_change"]["description"] = "Absolute change in reserved balance since previous period"
                
                if "staked_balance_change" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["staked_balance_change"]["description"] = "Absolute change in staked balance since previous period"
                
                if "total_balance_change" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["total_balance_change"]["description"] = "Absolute change in total balance since previous period"
                
                if "total_balance_percent_change" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["total_balance_percent_change"]["description"] = "Percentage change in total balance since previous period"
                
                if "_version" in schema["balance_series"]["columns"]:
                    schema["balance_series"]["columns"]["_version"]["description"] = "Version number for ReplacingMergeTree engine"
                
            if "balance_series_latest_view" in schema:
                schema["balance_series_latest_view"]["description"] = "Latest balance snapshot for each address and asset, showing current balance state with asset verification information"
                
                # Add column descriptions for balance_series_latest_view
                if "latest_period_start" in schema["balance_series_latest_view"]["columns"]:
                    schema["balance_series_latest_view"]["columns"]["latest_period_start"]["description"] = "Start timestamp of the most recent period"
                
                if "latest_period_end" in schema["balance_series_latest_view"]["columns"]:
                    schema["balance_series_latest_view"]["columns"]["latest_period_end"]["description"] = "End timestamp of the most recent period"
                
                if "latest_block_height" in schema["balance_series_latest_view"]["columns"]:
                    schema["balance_series_latest_view"]["columns"]["latest_block_height"]["description"] = "Block height of the most recent snapshot"
                
            if "balance_series_daily_view" in schema:
                schema["balance_series_daily_view"]["description"] = "Daily balance aggregations showing end-of-day balances and daily balance changes for each address and asset with asset verification information"
                
                # Add column descriptions for balance_series_daily_view
                if "date" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["date"]["description"] = "Calendar date for the daily aggregation"
                
                if "end_of_day_free_balance" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["end_of_day_free_balance"]["description"] = "Free balance at the end of the day"
                
                if "end_of_day_reserved_balance" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["end_of_day_reserved_balance"]["description"] = "Reserved balance at the end of the day"
                
                if "end_of_day_staked_balance" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["end_of_day_staked_balance"]["description"] = "Staked balance at the end of the day"
                
                if "end_of_day_total_balance" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["end_of_day_total_balance"]["description"] = "Total balance at the end of the day"
                
                if "daily_free_balance_change" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["daily_free_balance_change"]["description"] = "Cumulative change in free balance over the day"
                
                if "daily_reserved_balance_change" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["daily_reserved_balance_change"]["description"] = "Cumulative change in reserved balance over the day"
                
                if "daily_staked_balance_change" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["daily_staked_balance_change"]["description"] = "Cumulative change in staked balance over the day"
                
                if "daily_total_balance_change" in schema["balance_series_daily_view"]["columns"]:
                    schema["balance_series_daily_view"]["columns"]["daily_total_balance_change"]["description"] = "Cumulative change in total balance over the day"
                
            if "balance_series_weekly_view" in schema:
                schema["balance_series_weekly_view"]["description"] = "Weekly balance statistics materialized view with end-of-week balances and weekly balance changes, including asset verification status"
                
                # Add column descriptions for balance_series_weekly_view
                if "week_start" in schema["balance_series_weekly_view"]["columns"]:
                    schema["balance_series_weekly_view"]["columns"]["week_start"]["description"] = "Start date of the week (Monday)"
                
                if "end_of_week_free_balance" in schema["balance_series_weekly_view"]["columns"]:
                    schema["balance_series_weekly_view"]["columns"]["end_of_week_free_balance"]["description"] = "Free balance at the end of the week"
                
                if "end_of_week_total_balance" in schema["balance_series_weekly_view"]["columns"]:
                    schema["balance_series_weekly_view"]["columns"]["end_of_week_total_balance"]["description"] = "Total balance at the end of the week"
                
                if "weekly_free_balance_change" in schema["balance_series_weekly_view"]["columns"]:
                    schema["balance_series_weekly_view"]["columns"]["weekly_free_balance_change"]["description"] = "Cumulative change in free balance over the week"
                
                if "weekly_total_balance_change" in schema["balance_series_weekly_view"]["columns"]:
                    schema["balance_series_weekly_view"]["columns"]["weekly_total_balance_change"]["description"] = "Cumulative change in total balance over the week"
                
                if "last_block_of_week" in schema["balance_series_weekly_view"]["columns"]:
                    schema["balance_series_weekly_view"]["columns"]["last_block_of_week"]["description"] = "Last block height processed in the week"
                
            if "balance_series_monthly_view" in schema:
                schema["balance_series_monthly_view"]["description"] = "Monthly balance statistics materialized view with end-of-month balances and monthly balance changes, including asset verification status"
                
                # Add column descriptions for balance_series_monthly_view
                if "month_start" in schema["balance_series_monthly_view"]["columns"]:
                    schema["balance_series_monthly_view"]["columns"]["month_start"]["description"] = "Start date of the month"
                
                if "end_of_month_free_balance" in schema["balance_series_monthly_view"]["columns"]:
                    schema["balance_series_monthly_view"]["columns"]["end_of_month_free_balance"]["description"] = "Free balance at the end of the month"
                
                if "end_of_month_total_balance" in schema["balance_series_monthly_view"]["columns"]:
                    schema["balance_series_monthly_view"]["columns"]["end_of_month_total_balance"]["description"] = "Total balance at the end of the month"
                
                if "monthly_free_balance_change" in schema["balance_series_monthly_view"]["columns"]:
                    schema["balance_series_monthly_view"]["columns"]["monthly_free_balance_change"]["description"] = "Cumulative change in free balance over the month"
                
                if "monthly_total_balance_change" in schema["balance_series_monthly_view"]["columns"]:
                    schema["balance_series_monthly_view"]["columns"]["monthly_total_balance_change"]["description"] = "Cumulative change in total balance over the month"
                
                if "last_block_of_month" in schema["balance_series_monthly_view"]["columns"]:
                    schema["balance_series_monthly_view"]["columns"]["last_block_of_month"]["description"] = "Last block height processed in the month"
                
            if "known_addresses" in schema:
                schema["known_addresses"]["description"] = "Reference table with labeled/known addresses for contextual analysis"
                
                # Add column descriptions for known_addresses if available
                if "address" in schema["known_addresses"]["columns"]:
                    schema["known_addresses"]["columns"]["address"]["description"] = "Blockchain address"
                
                if "label" in schema["known_addresses"]["columns"]:
                    schema["known_addresses"]["columns"]["label"]["description"] = "Human-readable label for the address"
                
                if "category" in schema["known_addresses"]["columns"]:
                    schema["known_addresses"]["columns"]["category"]["description"] = "Category of the address (exchange, treasury, bridge, etc.)"
                
                if "description" in schema["known_addresses"]["columns"]:
                    schema["known_addresses"]["columns"]["description"]["description"] = "Detailed description of the address purpose"

            return {
                "name": "Balance Series Schema",
                "description": "Schema for balance snapshot time-series data in ClickHouse - tracks balance changes over time at fixed 4-hour intervals with support for multiple balance types (free, reserved, staked, total) and multi-level time aggregation (daily, weekly, monthly). Includes asset verification information to help identify verified, unknown, or potentially malicious assets.",
                "tables": schema,
                "key_features": [
                    "Time-series tracking with fixed 4-hour interval snapshots",
                    "Multi-balance type support (free, reserved, staked, total)",
                    "Change tracking between periods with both absolute and percentage metrics",
                    "Multi-level time aggregation (4-hour, daily, weekly, monthly)",
                    "Optimized views for efficient querying at different time scales"
                ],
                "common_use_cases": [
                    "Current balance lookup for any address and asset",
                    "Historical balance analysis over different time periods",
                    "Balance change trend identification",
                    "Balance composition analysis (free vs. reserved vs. staked)",
                    "Significant balance change detection",
                    "Correlation of balance changes with network events"
                ]
            }

        except Exception as e:
            logger.error(f"Error getting balance series schema: {str(e)}")
            raise e

    async def balance_series_query(self, query: str) -> Dict[str, Any]:
        """
        Execute balance series query against ClickHouse

        Args:
            query (str): The SQL query to execute

        Returns:
            Dict containing query results
        """
        try:
            result = self.client.query(query)
            logger.info(f"Query executed successfully: {query}")
            return {
                "data": result.result_rows,
                "columns": result.column_names,
                "rows_count": result.row_count
            }
        except Exception as e:
            logger.error(f"Error executing ClickHouse query: {query};{str(e)}")
            raise e
