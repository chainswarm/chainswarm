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

def create_clickhouse_client(network: str):
    """Create a ClickHouse client for the specified network
    
    Args:
        network (str): The blockchain network to connect to
        
    Returns:
        ClickHouse client instance
    """
    from packages.indexers.base import get_clickhouse_connection_string
    
    connection_params = get_clickhouse_connection_string(network)
    return clickhouse_connect.get_client(
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

def get_balance_tracking_tables() -> List[str]:
    """Get the list of tables related to balance tracking
    
    Returns:
        List of table names
    """
    return [
        "balance_changes",
        "balance_delta_changes",
        "balance_transfers",
        "balance_transfers_cycles",
        "balance_transfers_smurfing_patterns",
        "known_addresses",

        "daily_balance_statistics_mv",
        "daily_transfer_volume_mv",
        "current_balances_view",
        "significant_balance_changes_view",
        "transfer_statistics_view"
    ]

def get_table_info(client, database: str, tables: List[str]) -> List[TableInfo]:
    """Get information about the specified tables
    
    Args:
        client: ClickHouse client instance
        database: Database name
        tables: List of table names
        
    Returns:
        List of TableInfo objects
    """
    table_info_list = []
    
    for table in tables:
        table_info = TableInfo(name=table)
        table_info_list.append(table_info)
    
    return table_info_list

def get_column_info(client, database: str, table_name: str) -> List[TableColumn]:
    """Get column information for a specific table
    
    Args:
        client: ClickHouse client instance
        database: Database name
        table_name: Table name
        
    Returns:
        List of TableColumn objects
    """
    query = f"DESCRIBE TABLE {database}.{table_name}"
    result = client.query(query)
    
    columns = []
    for row in result.result_rows:
        column_name = row[0]
        column_type = row[1]
        columns.append(TableColumn(name=column_name, type=column_type))
    
    return columns

def format_tables_to_schema(table_info_list: List[TableInfo]) -> Dict[str, Any]:
    """Format table information into a schema dictionary
    
    Args:
        table_info_list: List of TableInfo objects
        
    Returns:
        Schema dictionary
    """
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
    
    return schema

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