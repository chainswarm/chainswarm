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


class BalanceTransfersTool:
    def __init__(self, connection_params: Dict[str, Any]):
        """Initialize the BalanceTransfersTool with ClickHouse connection parameters"""

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
            # Core table
            "balance_transfers",
            
            # Basic views
            "balance_transfers_statistics_view",
            "balance_transfers_daily_volume_mv",
            "available_transfer_assets_view",
            
            # Behavior analysis views
            "balance_transfers_address_behavior_profiles_view",
            "balance_transfers_address_classification_view",
            "balance_transfers_suspicious_activity_view",
            
            # Relationship analysis views
            "balance_transfers_address_relationships_view",
            "balance_transfers_address_activity_patterns_view",
            
            # Network analysis views
            "balance_transfers_network_flow_view",
            "balance_transfers_periodic_activity_view",
            "balance_transfers_seasonality_view",
            
            # Economic analysis views
            "balance_transfers_velocity_view",
            "balance_transfers_liquidity_concentration_view",
            "balance_transfers_holding_time_view",
            
            # Anomaly detection views
            "balance_transfers_basic_anomaly_view",
        }

    async def schema(self) -> Dict[str, Any]:
        """
        Get balance transfers schema from ClickHouse with asset support

        Returns:
            Dict containing the balance transfers schema with asset information
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

            # Add descriptions for tables and views
            if "balance_transfers" in schema:
                schema["balance_transfers"]["description"] = "Stores individual transfer transactions between addresses"
                
            if "balance_transfers_statistics_view" in schema:
                schema["balance_transfers_statistics_view"]["description"] = "Basic statistics by address and asset"
                
            if "balance_transfers_daily_volume_mv" in schema:
                schema["balance_transfers_daily_volume_mv"]["description"] = "Materialized view for daily transfer volume"
                
            if "available_transfer_assets_view" in schema:
                schema["available_transfer_assets_view"]["description"] = "Simple view listing available assets"
                
            if "balance_transfers_address_behavior_profiles_view" in schema:
                schema["balance_transfers_address_behavior_profiles_view"]["description"] = "Comprehensive behavioral analysis for each address"
                
            if "balance_transfers_address_classification_view" in schema:
                schema["balance_transfers_address_classification_view"]["description"] = "Classifies addresses into behavioral categories"
                
            if "balance_transfers_suspicious_activity_view" in schema:
                schema["balance_transfers_suspicious_activity_view"]["description"] = "Identifies potentially suspicious activity patterns"
                
            if "balance_transfers_address_relationships_view" in schema:
                schema["balance_transfers_address_relationships_view"]["description"] = "Tracks relationships between addresses"
                
            if "balance_transfers_address_activity_patterns_view" in schema:
                schema["balance_transfers_address_activity_patterns_view"]["description"] = "Analyzes temporal and behavioral patterns"
                
            if "balance_transfers_network_flow_view" in schema:
                schema["balance_transfers_network_flow_view"]["description"] = "High-level overview of network activity"
                
            if "balance_transfers_periodic_activity_view" in schema:
                schema["balance_transfers_periodic_activity_view"]["description"] = "Activity patterns over weekly time periods"
                
            if "balance_transfers_seasonality_view" in schema:
                schema["balance_transfers_seasonality_view"]["description"] = "Temporal patterns in transaction activity"
                
            if "balance_transfers_velocity_view" in schema:
                schema["balance_transfers_velocity_view"]["description"] = "Measures token circulation speed"
                
            if "balance_transfers_liquidity_concentration_view" in schema:
                schema["balance_transfers_liquidity_concentration_view"]["description"] = "Analyzes holding concentration"
                
            if "balance_transfers_holding_time_view" in schema:
                schema["balance_transfers_holding_time_view"]["description"] = "Analyzes token holding duration"
                
            if "balance_transfers_basic_anomaly_view" in schema:
                schema["balance_transfers_basic_anomaly_view"]["description"] = "Detects unusual transaction patterns"

            return {
                "name": "Balance Transfers Schema",
                "description": "Schema for balance transfers data in ClickHouse",
                "tables": schema,
            }

        except Exception as e:
            logger.error(f"Error getting balance transfers schema: {str(e)}")
            raise e

    async def balance_transfers_query(self, query: str) -> Dict[str, Any]:
        """
        Execute balance transfers query against ClickHouse

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