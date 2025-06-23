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
                
                # Add column descriptions for balance_transfers table
                if "extrinsic_id" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["extrinsic_id"]["description"] = "Unique identifier for the blockchain extrinsic/transaction"
                
                if "event_idx" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["event_idx"]["description"] = "Index of the event within the extrinsic"
                
                if "block_height" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["block_height"]["description"] = "Block number where the transfer occurred"
                
                if "block_timestamp" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["block_timestamp"]["description"] = "Unix timestamp in milliseconds when the block was produced"
                
                if "from_address" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["from_address"]["description"] = "Sender address of the transfer"
                
                if "to_address" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["to_address"]["description"] = "Recipient address of the transfer"
                
                if "asset" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["asset"]["description"] = "Token or currency being transferred"
                
                if "amount" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["amount"]["description"] = "Amount of the asset transferred"
                
                if "fee" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["fee"]["description"] = "Transaction fee paid by the sender"
                
                if "_version" in schema["balance_transfers"]["columns"]:
                    schema["balance_transfers"]["columns"]["_version"]["description"] = "Version number for ReplacingMergeTree engine"
                
            # Add descriptions for volume series materialized view
            if "balance_transfers_volume_series_mv" in schema:
                schema["balance_transfers_volume_series_mv"]["description"] = "Base 4-hour interval materialized view for transfer volume analysis - asset agnostic with client-defined categorization"
                
                # Add column descriptions for key metrics
                if "period_start" in schema["balance_transfers_volume_series_mv"]["columns"]:
                    schema["balance_transfers_volume_series_mv"]["columns"]["period_start"]["description"] = "Start of the 4-hour period (UTC-based, aligned to midnight)"
                
                if "period_end" in schema["balance_transfers_volume_series_mv"]["columns"]:
                    schema["balance_transfers_volume_series_mv"]["columns"]["period_end"]["description"] = "End of the 4-hour period (UTC-based, aligned to midnight)"
                
                if "transaction_count" in schema["balance_transfers_volume_series_mv"]["columns"]:
                    schema["balance_transfers_volume_series_mv"]["columns"]["transaction_count"]["description"] = "Number of transactions in the period"
                
                if "unique_senders" in schema["balance_transfers_volume_series_mv"]["columns"]:
                    schema["balance_transfers_volume_series_mv"]["columns"]["unique_senders"]["description"] = "Number of unique sender addresses in the period"
                
                if "unique_receivers" in schema["balance_transfers_volume_series_mv"]["columns"]:
                    schema["balance_transfers_volume_series_mv"]["columns"]["unique_receivers"]["description"] = "Number of unique recipient addresses in the period"
                
                if "total_volume" in schema["balance_transfers_volume_series_mv"]["columns"]:
                    schema["balance_transfers_volume_series_mv"]["columns"]["total_volume"]["description"] = "Total amount transferred in the period"
                
                if "network_density" in schema["balance_transfers_volume_series_mv"]["columns"]:
                    schema["balance_transfers_volume_series_mv"]["columns"]["network_density"]["description"] = "Ratio of actual connections to possible connections (network connectivity metric)"
                
            # Add descriptions for network analytics views
            if "balance_transfers_network_daily_view" in schema:
                schema["balance_transfers_network_daily_view"]["description"] = "Daily network analytics with transaction counts, volumes, participant metrics, and fee statistics"
                
            if "balance_transfers_network_weekly_view" in schema:
                schema["balance_transfers_network_weekly_view"]["description"] = "Weekly network analytics with transaction counts, volumes, participant metrics, and fee statistics"
                
            if "balance_transfers_network_monthly_view" in schema:
                schema["balance_transfers_network_monthly_view"]["description"] = "Monthly network analytics with transaction counts, volumes, participant metrics, and fee statistics"
                
            # Add descriptions for address analytics view
            if "balance_transfers_address_analytics_view" in schema:
                schema["balance_transfers_address_analytics_view"]["description"] = "Comprehensive address analytics with transaction counts, volumes, temporal patterns, and behavioral classification"
                
                # Add column descriptions for key metrics
                if "address_type" in schema["balance_transfers_address_analytics_view"]["columns"]:
                    schema["balance_transfers_address_analytics_view"]["columns"]["address_type"]["description"] = "Behavioral classification of the address (Exchange, Whale, High_Volume_Trader, etc.)"
                
                if "total_transactions" in schema["balance_transfers_address_analytics_view"]["columns"]:
                    schema["balance_transfers_address_analytics_view"]["columns"]["total_transactions"]["description"] = "Total number of transactions involving this address"
                
                if "outgoing_count" in schema["balance_transfers_address_analytics_view"]["columns"]:
                    schema["balance_transfers_address_analytics_view"]["columns"]["outgoing_count"]["description"] = "Number of outgoing transactions from this address"
                
                if "incoming_count" in schema["balance_transfers_address_analytics_view"]["columns"]:
                    schema["balance_transfers_address_analytics_view"]["columns"]["incoming_count"]["description"] = "Number of incoming transactions to this address"
                
                if "total_sent" in schema["balance_transfers_address_analytics_view"]["columns"]:
                    schema["balance_transfers_address_analytics_view"]["columns"]["total_sent"]["description"] = "Total amount sent from this address"
                
                if "total_received" in schema["balance_transfers_address_analytics_view"]["columns"]:
                    schema["balance_transfers_address_analytics_view"]["columns"]["total_received"]["description"] = "Total amount received by this address"
                
                if "unique_recipients" in schema["balance_transfers_address_analytics_view"]["columns"]:
                    schema["balance_transfers_address_analytics_view"]["columns"]["unique_recipients"]["description"] = "Number of unique addresses this address has sent to"
                
                if "unique_senders" in schema["balance_transfers_address_analytics_view"]["columns"]:
                    schema["balance_transfers_address_analytics_view"]["columns"]["unique_senders"]["description"] = "Number of unique addresses this address has received from"
                
            # Add descriptions for volume aggregation views
            if "balance_transfers_volume_daily_view" in schema:
                schema["balance_transfers_volume_daily_view"]["description"] = "Daily volume aggregation with transaction counts, volumes, and histogram bins"
                
            if "balance_transfers_volume_weekly_view" in schema:
                schema["balance_transfers_volume_weekly_view"]["description"] = "Weekly volume aggregation with transaction counts, volumes, and histogram bins"
                
            if "balance_transfers_volume_monthly_view" in schema:
                schema["balance_transfers_volume_monthly_view"]["description"] = "Monthly volume aggregation with transaction counts, volumes, and histogram bins"
                
            # Add descriptions for analysis views
            if "balance_transfers_volume_trends_view" in schema:
                schema["balance_transfers_volume_trends_view"]["description"] = "Volume trends with rolling averages (7-period and 30-period) for trend analysis"
                
            if "balance_transfers_volume_quantiles_view" in schema:
                schema["balance_transfers_volume_quantiles_view"]["description"] = "Volume distribution analysis with quantiles (10th, 25th, 50th, 75th, 90th, 99th percentiles)"
                
            # Add descriptions for behavior analysis views
            if "balance_transfers_address_behavior_profiles_view" in schema:
                schema["balance_transfers_address_behavior_profiles_view"]["description"] = "Comprehensive behavioral analysis for each address with temporal and volume patterns"
                
            if "balance_transfers_address_classification_view" in schema:
                schema["balance_transfers_address_classification_view"]["description"] = "Classifies addresses into behavioral categories (Exchange, Whale, High_Volume_Trader, etc.)"
                
            if "balance_transfers_suspicious_activity_view" in schema:
                schema["balance_transfers_suspicious_activity_view"]["description"] = "Identifies potentially suspicious activity patterns based on transaction behavior"
                
            # Add descriptions for relationship analysis views
            if "balance_transfers_address_relationships_view" in schema:
                schema["balance_transfers_address_relationships_view"]["description"] = "Tracks relationships between addresses with strength metrics and interaction patterns"
                
            if "balance_transfers_address_activity_patterns_view" in schema:
                schema["balance_transfers_address_activity_patterns_view"]["description"] = "Analyzes temporal and behavioral patterns for addresses"
                
            # Add descriptions for network analysis views
            if "balance_transfers_network_flow_view" in schema:
                schema["balance_transfers_network_flow_view"]["description"] = "High-level overview of network activity with flow metrics and connectivity indicators"
                
            if "balance_transfers_periodic_activity_view" in schema:
                schema["balance_transfers_periodic_activity_view"]["description"] = "Activity patterns over weekly time periods with day-of-week analysis"
                
            if "balance_transfers_seasonality_view" in schema:
                schema["balance_transfers_seasonality_view"]["description"] = "Temporal patterns in transaction activity (hour-of-day, day-of-week, month-of-year)"
                
            # Add descriptions for economic analysis views
            if "balance_transfers_velocity_view" in schema:
                schema["balance_transfers_velocity_view"]["description"] = "Measures token circulation speed (transaction volume relative to supply)"
                
            if "balance_transfers_liquidity_concentration_view" in schema:
                schema["balance_transfers_liquidity_concentration_view"]["description"] = "Analyzes holding concentration with Gini coefficient and distribution metrics"
                
            if "balance_transfers_holding_time_view" in schema:
                schema["balance_transfers_holding_time_view"]["description"] = "Analyzes token holding duration and turnover rates"
                
            # Add descriptions for anomaly detection views
            if "balance_transfers_basic_anomaly_view" in schema:
                schema["balance_transfers_basic_anomaly_view"]["description"] = "Detects unusual transaction patterns based on statistical outliers"
                
            if "balance_transfers_statistics_view" in schema:
                schema["balance_transfers_statistics_view"]["description"] = "Basic statistics by address and asset with transaction counts and volumes"
                
            if "available_transfer_assets_view" in schema:
                schema["available_transfer_assets_view"]["description"] = "Simple view listing available assets in the balance transfers data"

            return {
                "name": "Balance Transfers Schema",
                "description": "Schema for balance transfers data in ClickHouse - tracks individual transfer transactions between addresses with comprehensive metrics for network activity, address behavior, and economic indicators",
                "tables": schema,
                "key_features": [
                    "Asset-agnostic design with universal histogram bins for consistent analysis",
                    "Multi-level time aggregation (4-hour, daily, weekly, monthly)",
                    "Address behavior profiling and classification",
                    "Network activity metrics and relationship analysis",
                    "Economic indicators like token velocity and liquidity concentration",
                    "Anomaly detection for suspicious activity identification"
                ],
                "transaction_size_bins": [
                    "< 0.1",
                    "0.1 to < 1",
                    "1 to < 10",
                    "10 to < 100",
                    "100 to < 1,000",
                    "1,000 to < 10,000",
                    "≥ 10,000"
                ],
                "address_classifications": [
                    "Exchange: High volume (≥100,000) with many recipients (≥100)",
                    "Whale: High volume (≥100,000) with few recipients (<10)",
                    "High_Volume_Trader: Significant volume (≥10,000) with many transactions (≥1,000)",
                    "Hub_Address: Many connections (≥50 recipients and ≥50 senders)",
                    "Retail_Active: Many transactions (≥100) but lower volume (<1,000)",
                    "Whale_Inactive: Few transactions (<10) but high volume (≥10,000)",
                    "Retail_Inactive: Few transactions (<10) and low volume (<100)",
                    "Regular_User: Default classification for other addresses"
                ],
                "common_use_cases": [
                    "Transaction history analysis for specific addresses",
                    "Network activity monitoring and trend identification",
                    "Address behavior profiling and classification",
                    "Relationship analysis between addresses",
                    "Economic indicator tracking (velocity, concentration)",
                    "Suspicious activity detection",
                    "Temporal pattern analysis (time-of-day, day-of-week)"
                ]
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