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


class BalanceTransfersAnalyticsTool:
    def __init__(self, connection_params: Dict[str, Any]):
        """Initialize the BalanceTransfersAnalyticsTool with ClickHouse connection parameters"""

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
        
        # Updated tables and views for consolidated balance_transfers schema
        self.tables = {
            # Core table
            "balance_transfers",
            
            # Materialized views (4-hour intervals for performance)
            "balance_transfers_volume_series_mv",
            
            # Consolidated analytics views (NEW ARCHITECTURE)
            "balance_transfers_address_analytics_view",
            "balance_transfers_address_risk_levels_view",
            "balance_transfers_network_daily_view",
            "balance_transfers_network_weekly_view", 
            "balance_transfers_network_monthly_view",
            "balance_transfers_network_analytics_view",
            "balance_transfers_network_enhanced_view",
            "balance_transfers_transaction_analytics_view",
            "balance_transfers_relationships_view",
            "balance_transfers_daily_patterns_view",
            
            # Legacy views (maintained for compatibility)
            "balance_transfers_statistics_view",
            "balance_transfers_volume_daily_view",
            "balance_transfers_volume_weekly_view",
            "balance_transfers_volume_monthly_view",
            "balance_transfers_volume_trends_view",
            "balance_transfers_volume_quantiles_view",
            "balance_transfers_address_behavior_profiles_view",
            "balance_transfers_address_classification_view",
            "balance_transfers_suspicious_activity_view",
            "balance_transfers_address_relationships_view",
            "balance_transfers_address_activity_patterns_view",
            "available_transfer_assets_view",
            
            # Additional context tables
            "known_addresses",
        }

    async def schema(self) -> Dict[str, Any]:
        """
        Get balance transfers analytics schema from ClickHouse with consolidated views

        Returns:
            Dict containing the consolidated balance transfers analytics schema
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

            # Add comprehensive descriptions for consolidated schema
            
            # Core table
            if "balance_transfers" in schema:
                schema["balance_transfers"]["description"] = "Main table storing individual transfer transactions between addresses with universal asset support and comprehensive indexing"
                
            # High-performance materialized view
            if "balance_transfers_volume_series_mv" in schema:
                schema["balance_transfers_volume_series_mv"]["description"] = "High-performance materialized view with 4-hour interval aggregations, universal histogram bins (0.1, 1, 10, 100, 1K, 10K+), network metrics, and statistical measures for all assets"
            
            # CONSOLIDATED ANALYTICS VIEWS (NEW ARCHITECTURE)
            
            # Address Analytics (replaces behavior profiles, classification, suspicious activity)
            if "balance_transfers_address_analytics_view" in schema:
                schema["balance_transfers_address_analytics_view"]["description"] = "Comprehensive address behavior analysis with universal histogram bins, time patterns, risk scoring, and suspicious activity detection. Includes computed risk indicators and behavioral metrics for any asset."
                
            if "balance_transfers_address_risk_levels_view" in schema:
                schema["balance_transfers_address_risk_levels_view"]["description"] = "Address risk assessment with computed risk levels (Normal/Low/Medium/High) based on behavioral patterns, unusual timing, fixed amounts, and anomaly indicators."
            
            # Network Analytics (replaces network flow and temporal analysis)
            if "balance_transfers_network_daily_view" in schema:
                schema["balance_transfers_network_daily_view"]["description"] = "Daily network activity metrics with universal histogram bins, network density calculations, and transaction patterns for all assets."
                
            if "balance_transfers_network_weekly_view" in schema:
                schema["balance_transfers_network_weekly_view"]["description"] = "Weekly network activity aggregations with transaction patterns, volume distributions, and network connectivity metrics."
                
            if "balance_transfers_network_monthly_view" in schema:
                schema["balance_transfers_network_monthly_view"]["description"] = "Monthly network activity summaries with long-term trend analysis capabilities and comprehensive volume breakdowns."
                
            if "balance_transfers_network_analytics_view" in schema:
                schema["balance_transfers_network_analytics_view"]["description"] = "Combined network analytics across all time periods (daily/weekly/monthly) with unified schema and universal histogram bins."
                
            if "balance_transfers_network_enhanced_view" in schema:
                schema["balance_transfers_network_enhanced_view"]["description"] = "Enhanced network view with computed fields like average transaction size, derived from network analytics with safe calculations."
            
            # Transaction Analytics (replaces relationships and anomaly detection)
            if "balance_transfers_transaction_analytics_view" in schema:
                schema["balance_transfers_transaction_analytics_view"]["description"] = "Comprehensive transaction relationship analysis with strength scoring (0-10 scale), anomaly detection, activity patterns, and universal histogram bins. Includes computed anomaly levels and relationship metrics."
                
            if "balance_transfers_relationships_view" in schema:
                schema["balance_transfers_relationships_view"]["description"] = "Base address relationship analysis with transfer counts, amounts, temporal patterns, and universal histogram bins for transaction size distribution."
                
            if "balance_transfers_daily_patterns_view" in schema:
                schema["balance_transfers_daily_patterns_view"]["description"] = "Daily activity patterns between address pairs with transaction counts, volumes, and universal histogram bins for daily transaction analysis."
            
            # LEGACY VIEWS (maintained for compatibility)
            if "balance_transfers_statistics_view" in schema:
                schema["balance_transfers_statistics_view"]["description"] = "Legacy basic transfer statistics by address and asset (maintained for compatibility)"
                
            if "balance_transfers_volume_daily_view" in schema:
                schema["balance_transfers_volume_daily_view"]["description"] = "Legacy daily volume aggregations from materialized view (maintained for compatibility)"
                
            if "balance_transfers_volume_weekly_view" in schema:
                schema["balance_transfers_volume_weekly_view"]["description"] = "Legacy weekly volume aggregations from materialized view (maintained for compatibility)"
                
            if "balance_transfers_volume_monthly_view" in schema:
                schema["balance_transfers_volume_monthly_view"]["description"] = "Legacy monthly volume aggregations from materialized view (maintained for compatibility)"
                
            if "balance_transfers_volume_trends_view" in schema:
                schema["balance_transfers_volume_trends_view"]["description"] = "Legacy volume trends with rolling averages and percentage changes (maintained for compatibility)"
                
            if "balance_transfers_volume_quantiles_view" in schema:
                schema["balance_transfers_volume_quantiles_view"]["description"] = "Legacy volume distribution quantiles for statistical analysis (maintained for compatibility)"
                
            if "balance_transfers_address_behavior_profiles_view" in schema:
                schema["balance_transfers_address_behavior_profiles_view"]["description"] = "Legacy address behavior profiles (replaced by address_analytics_view but maintained for compatibility)"
                
            if "balance_transfers_address_classification_view" in schema:
                schema["balance_transfers_address_classification_view"]["description"] = "Legacy address classification (replaced by address_analytics_view but maintained for compatibility)"
                
            if "balance_transfers_suspicious_activity_view" in schema:
                schema["balance_transfers_suspicious_activity_view"]["description"] = "Legacy suspicious activity detection (replaced by address_analytics_view but maintained for compatibility)"
                
            if "balance_transfers_address_relationships_view" in schema:
                schema["balance_transfers_address_relationships_view"]["description"] = "Legacy address relationships (replaced by transaction_analytics_view but maintained for compatibility)"
                
            if "balance_transfers_address_activity_patterns_view" in schema:
                schema["balance_transfers_address_activity_patterns_view"]["description"] = "Legacy activity patterns (replaced by daily_patterns_view but maintained for compatibility)"
                
            if "available_transfer_assets_view" in schema:
                schema["available_transfer_assets_view"]["description"] = "Simple view listing all available assets in the balance_transfers table"
                
            if "known_addresses" in schema:
                schema["known_addresses"]["description"] = "Reference table with labeled/known addresses for contextual analysis"

            return {
                "name": "Balance Transfers Analytics Schema",
                "description": "Consolidated schema for balance transfer transaction analysis with universal histogram bins, asset-agnostic design, comprehensive address analytics, network flow analysis, and relationship scoring. Features new consolidated views that replace 9 legacy schema parts with 6 optimized parts.",
                "tables": schema,
                "consolidation_info": {
                    "new_consolidated_views": [
                        "balance_transfers_address_analytics_view",
                        "balance_transfers_network_analytics_view", 
                        "balance_transfers_transaction_analytics_view"
                    ],
                    "universal_histogram_bins": "0.1, 1, 10, 100, 1K, 10K+ (asset-agnostic)",
                    "risk_scoring": "Computed risk scores and levels based on behavioral patterns",
                    "relationship_strength": "0-10 scale relationship scoring between addresses"
                }
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