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
            "balance_series_weekly_mv",
            "balance_series_monthly_mv",
            
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
                schema["balance_series"]["description"] = "Main table storing balance snapshots at fixed 4-hour intervals with free, reserved, staked, and total balances for each address and asset"
                
            if "balance_series_latest_view" in schema:
                schema["balance_series_latest_view"]["description"] = "Latest balance snapshot for each address and asset, showing current balance state"
                
            if "balance_series_daily_view" in schema:
                schema["balance_series_daily_view"]["description"] = "Daily balance aggregations showing end-of-day balances and daily balance changes for each address and asset"
                
            if "balance_series_weekly_mv" in schema:
                schema["balance_series_weekly_mv"]["description"] = "Weekly balance statistics materialized view with end-of-week balances and weekly balance changes"
                
            if "balance_series_monthly_mv" in schema:
                schema["balance_series_monthly_mv"]["description"] = "Monthly balance statistics materialized view with end-of-month balances and monthly balance changes"
                
            if "known_addresses" in schema:
                schema["known_addresses"]["description"] = "Reference table with labeled/known addresses for contextual analysis"

            return {
                "name": "Balance Series Schema",
                "description": "Schema for balance snapshot time-series data in ClickHouse - tracks balance changes over time at fixed 4-hour intervals",
                "tables": schema,
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
