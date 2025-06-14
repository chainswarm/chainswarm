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


class BalanceTrackingTool:
    def __init__(self, connection_params: Dict[str, Any]):
        """Initialize the BalanceTrackingTool with ClickHouse connection parameters"""

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
        self.tables = {
            "balance_changes",
            "balance_delta_changes",
            "balance_transfers",
            "known_addresses",
            "balance_daily_statistics_mv",
            "balance_transfers_daily_volume_mv",
            "available_assets_view",
            "balance_significant_changes_view",
            "balance_transfers_statistics_view",
            "balances_current_view",
        }

    async def get_balance_tracking_schema(self) -> Dict[str, Any]:
        """
        Get balance tracking schema from ClickHouse with asset support

        Args:
            assets: List of asset symbols for example queries

        Returns:
            Dict containing the balance tracking schema with asset information
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

            return {
                "name": "Balance Tracking Schema",
                "description": "Schema for balance data in ClickHouse",
                "tables": schema,
            }

        except Exception as e:
            logger.error(f"Error getting balance tracking schema: {str(e)}")
            raise e

    async def balance_tracking_query(self, query: str) -> Dict[str, Any]:
        """
        Execute balance tracking query against ClickHouse

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


