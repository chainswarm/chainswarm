from typing import List, Optional, Dict, Any
from typing import Annotated
from pydantic import Field
from fastmcp import FastMCP, Context
from packages.api.routers import get_memgraph_driver, get_neo4j_driver
from packages.api.services.balance_tracking_service import BalanceTrackingService
from packages.api.tools.money_flow import MoneyFlowTool
from packages.api.tools.similarity_search import SimilaritySearchTool
from packages.indexers.base import get_clickhouse_connection_string
from packages.indexers.substrate import get_network_asset
from packages.api.middleware.mcp_session_rate_limiting import (
    session_rate_limit,
    mcp_session_rate_limiter
)
import os
import clickhouse_connect

mcp = FastMCP(
    name="Chan Insights MCP Server",
    instructions="This MCP server provides access to balance tracking, money flow, similarity search, and known addresses data.",
)


async def get_assets_from_clickhouse(network: str) -> List[str]:
    """Query ClickHouse to get available assets for the network from materialized view"""
    try:
        connection_params = get_clickhouse_connection_string(network)
        client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database']
        )
        
        # Query assets from materialized view, ordered by total balance
        query = """
        SELECT asset
        FROM available_assets_mv
        ORDER BY total_balance_sum DESC
        LIMIT 1000
        """
        
        result = client.query(query)
        assets = [row[0] for row in result.result_rows]
        
        client.close()
        
        # Ensure native asset is included
        native_asset = get_network_asset(network)
        if native_asset not in assets:
            assets.insert(0, native_asset)
            
        return assets
    except Exception as e:
        # If query fails, return at least the native asset
        return [get_network_asset(network)]


def get_memgraph_cypher_cheatsheet() -> Dict[str, Any]:
    """Get Memgraph-specific Cypher cheatsheet with examples"""
    return {
        "path_expansion": {
            "description": "Memgraph's path.expand() function for efficient graph traversal (MAX 3 HOPS)",
            "syntax": "CALL path.expand(start_node, edge_types, node_labels, min_hops, max_hops)",
            "important": "Maximum expansion depth is limited to 3 hops to protect database performance",
            "examples": [
                {
                    "name": "Basic expansion (max 3 hops)",
                    "query": "MATCH (a:Address {address: $addr}) CALL path.expand(a, ['TO'], [], 1, 3) YIELD result RETURN result LIMIT 1000"
                },
                {
                    "name": "Filtered expansion with asset (max 3 hops)",
                    "query": "MATCH (a:Address {address: $addr}) CALL path.expand(a, ['TO'], [], 1, 3) YIELD result WHERE ALL(r IN relationships(result) WHERE r.asset = $asset) RETURN result LIMIT 1000"
                }
            ]
        },
        "bfs_dfs_traversal": {
            "description": "Native BFS/DFS support in path patterns (MAX 3 HOPS)",
            "important": "Maximum traversal depth is limited to 3 hops",
            "examples": [
                {
                    "name": "BFS with depth limit (max 3)",
                    "query": "MATCH path = (a:Address {address: $addr})-[*BFS 1..3 (r, n | r.asset = $asset)]->(b:Address) RETURN path LIMIT 1000"
                },
                {
                    "name": "DFS for paths (max 3)",
                    "query": "MATCH path = (a:Address)-[*DFS 1..3 (r, n | r.volume > 1000000)]->(b:Address) RETURN path LIMIT 1000"
                },
                {
                    "name": "Weighted shortest path (max 3)",
                    "query": "MATCH path = (a:Address {address: $from})-[*WSHORTEST 1..3 (r, n | r.volume) volume]->(b:Address {address: $to}) RETURN path, volume LIMIT 1000"
                }
            ]
        },
        "result_limits": {
            "description": "Query result limitations",
            "max_limit": 1000,
            "examples": [
                {
                    "name": "Always use LIMIT",
                    "query": "MATCH (a:Address)-[r:TO]->(b:Address) WHERE r.asset = $asset RETURN a, r, b LIMIT 1000"
                },
                {
                    "name": "Pagination with SKIP",
                    "query": "MATCH (a:Address)-[r:TO]->(b:Address) WHERE r.asset = $asset RETURN a, r, b SKIP $offset LIMIT $limit"
                }
            ]
        },
        "temporal_queries": {
            "description": "Time-based graph queries",
            "examples": [
                {
                    "name": "Temporal path finding (max 3 hops)",
                    "query": "MATCH path = (a:Address)-[rels:TO*1..3]->(b:Address) WHERE ALL(idx IN range(0, size(rels)-2) WHERE rels[idx].timestamp <= rels[idx+1].timestamp) RETURN path LIMIT 1000"
                },
                {
                    "name": "Time-windowed analysis",
                    "query": "MATCH (a:Address)-[r:TO]->(b:Address) WHERE r.timestamp > $start_time AND r.timestamp < $end_time AND r.asset = $asset RETURN a, r, b LIMIT 1000"
                }
            ]
        },
        "performance_optimization": {
            "description": "Query optimization techniques",
            "examples": [
                {
                    "name": "Index hints",
                    "query": "USING INDEX SEEK ON :Address(address) MATCH (a:Address {address: $addr}) RETURN a"
                },
                {
                    "name": "Early filtering with limit",
                    "query": "MATCH (a:Address) WHERE a.address STARTS WITH '5C' WITH a LIMIT 1000 MATCH (a)-[r:TO {asset: $asset}]->(b) RETURN a, r, b LIMIT 1000"
                },
                {
                    "name": "Limit early in traversal",
                    "query": "MATCH (a:Address {address: $addr}) CALL path.expand(a, ['TO'], [], 1, 3) YIELD result LIMIT 100 RETURN result"
                }
            ]
        },
        "restricted_operations": {
            "description": "Operations that are NOT allowed",
            "note": "These operations are handled by the indexing engine",
            "restricted": [
                "PageRank calculation - computed during indexing",
                "Community detection - computed during indexing",
                "Other graph algorithms - not yet implemented",
                "Path expansions beyond 3 hops - use multiple queries",
                "Queries without LIMIT - always specify a limit (max 1000)"
            ]
        }
    }


def get_clickhouse_sql_cheatsheet() -> Dict[str, Any]:
    """Get ClickHouse-specific SQL cheatsheet with examples"""
    return {
        "mergetree_specifics": {
            "description": "MergeTree engine optimizations",
            "examples": [
                {
                    "name": "FINAL for deduplication",
                    "query": "SELECT * FROM balance_changes FINAL WHERE address = $addr AND asset = $asset ORDER BY timestamp DESC"
                },
                {
                    "name": "PREWHERE optimization",
                    "query": "SELECT * FROM balance_transfers PREWHERE asset = $asset WHERE from_address = $addr OR to_address = $addr"
                },
                {
                    "name": "Partition pruning",
                    "query": "SELECT * FROM balance_changes WHERE toYYYYMM(timestamp) = 202401 AND address = $addr"
                }
            ]
        },
        "aggregation_functions": {
            "description": "Advanced aggregation capabilities",
            "examples": [
                {
                    "name": "Moving averages",
                    "query": "SELECT timestamp, asset, amount, avg(amount) OVER (PARTITION BY asset ORDER BY timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7d FROM balance_transfers FINAL"
                },
                {
                    "name": "Quantiles and percentiles",
                    "query": "SELECT asset, quantile(0.5)(amount) as median, quantile(0.95)(amount) as p95, quantile(0.99)(amount) as p99 FROM balance_transfers FINAL GROUP BY asset"
                },
                {
                    "name": "Array aggregations",
                    "query": "SELECT address, groupArray(asset) as assets, groupArray(balance) as balances FROM current_balances_view GROUP BY address HAVING length(assets) > 1"
                }
            ]
        },
        "time_series_analysis": {
            "description": "Time-series specific functions",
            "examples": [
                {
                    "name": "Time buckets with gaps filling",
                    "query": "WITH (SELECT min(timestamp) FROM balance_changes) as min_time SELECT time_bucket, COALESCE(sum_amount, 0) as amount FROM ( SELECT toStartOfHour(timestamp) as time_bucket, SUM(amount) as sum_amount FROM balance_changes FINAL WHERE asset = $asset GROUP BY time_bucket ) RIGHT JOIN ( SELECT toStartOfHour(min_time + number * 3600) as time_bucket FROM numbers(24*30) ) USING time_bucket ORDER BY time_bucket"
                },
                {
                    "name": "Rate of change",
                    "query": "SELECT timestamp, balance, balance - lagInFrame(balance, 1, 0) OVER (PARTITION BY address, asset ORDER BY timestamp) as balance_change FROM balance_changes FINAL"
                },
                {
                    "name": "Cumulative sums",
                    "query": "SELECT timestamp, address, asset, amount, SUM(amount) OVER (PARTITION BY address, asset ORDER BY timestamp) as cumulative_amount FROM balance_transfers FINAL"
                }
            ]
        },
        "materialized_views": {
            "description": "Pre-computed aggregations",
            "examples": [
                {
                    "name": "Query materialized view",
                    "query": "SELECT * FROM daily_balance_statistics_mv WHERE address = $addr AND asset = $asset AND day >= today() - 30"
                },
                {
                    "name": "Join with materialized data",
                    "query": "SELECT t.*, mv.daily_volume FROM balance_transfers t JOIN daily_transfer_volume_mv mv ON toDate(t.timestamp) = mv.day AND t.asset = mv.asset"
                }
            ]
        },
        "performance_patterns": {
            "description": "Query optimization patterns",
            "examples": [
                {
                    "name": "Sampling for estimates",
                    "query": "SELECT asset, COUNT(*) * 100 as estimated_count FROM balance_transfers SAMPLE 0.01 GROUP BY asset"
                },
                {
                    "name": "Index usage",
                    "query": "SELECT * FROM balance_changes WHERE address = $addr AND timestamp >= now() - INTERVAL 1 DAY ORDER BY timestamp DESC LIMIT 100"
                },
                {
                    "name": "Efficient JOIN",
                    "query": "SELECT * FROM balance_transfers FINAL AS t INNER JOIN known_addresses AS k ON t.from_address = k.address WHERE t.asset = $asset AND k.label != ''"
                }
            ]
        },
        "array_and_json": {
            "description": "Complex data type operations",
            "examples": [
                {
                    "name": "Array operations",
                    "query": "SELECT address, arrayFilter(x -> x.2 > 1000000, arrayZip(assets, balances)) as large_balances FROM (SELECT address, groupArray(asset) as assets, groupArray(balance) as balances FROM current_balances_view GROUP BY address)"
                },
                {
                    "name": "JSON extraction",
                    "query": "SELECT JSONExtractString(metadata, 'category') as category, COUNT(*) FROM known_addresses WHERE has(metadata, 'category') GROUP BY category"
                }
            ]
        }
    }


@session_rate_limit
@mcp.tool(
    name="schema",
    description="Get comprehensive schema information including all data models, supported networks, assets, database specifics, and usage examples",
    tags={"schema", "networks", "assets", "money flow", "balance tracking", "similarity search", "memgraph", "clickhouse"},
    annotations={
        "title": "Get comprehensive schema and capability information",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def schema(ctx: Context) -> dict:
    """
    Get comprehensive schema information for the MCP server.
    Network is determined from environment variable.
    Assets are discovered from ClickHouse.
    
    Returns:
        dict: Complete schema including:
            - Network and available assets
            - Money flow graph schema
            - Balance tracking schema
            - Similarity search schema
            - Usage examples for each capability
    """
    
    # Get network from environment variable
    network = os.getenv("NETWORK", "torus").lower()
    
    # Get available assets from ClickHouse
    assets = await get_assets_from_clickhouse(network)
    
    # Build comprehensive response
    response = {
        "network": network,
        "assets": assets,
        "schemas": {},
        "available_tools": [
            "schema",
            "money_flow_query",
            "balance_query",
            "similarity_search_query"
        ]
    }
    
    # Add money flow schema
    response["schemas"]["money_flow"] = {
        "description": "Graph database schema for money flow analysis",
        "database": {
            "type": "Memgraph",
            "note": "Uses Memgraph-specific Cypher dialect which differs from Neo4j Cypher",
            "key_differences": [
                "Path expansion syntax: path.expand() instead of variable-length patterns",
                "BFS/DFS support: Native BFS/DFS algorithms in path patterns",
                "Triggers and streams: Real-time processing capabilities",
                "Memory storage: In-memory graph for faster queries"
            ]
        },
        "memgraph_cypher_cheatsheet": get_memgraph_cypher_cheatsheet(),
        "asset_support": {
            "description": "Money flow graph includes asset properties on edges only. Address nodes are identified by address only.",
            "asset_properties": {
                "nodes": [],
                "edges": ["asset", "id"]
            },
            "edge_naming": "Edge IDs are suffixed with asset symbol: from-{from_address}-to-{to_address}-{asset}"
        }
    }
    
    # Add balance tracking schema
    response["schemas"]["balance_tracking"] = {
        "description": "Time-series balance tracking schema",
        "database": {
            "type": "ClickHouse",
            "note": "OLAP database optimized for analytical queries, not OLTP",
            "key_features": [
                "Columnar storage for fast aggregations",
                "Time-series optimized with partitioning",
                "Materialized views for pre-computed metrics",
                "FINAL keyword for deduplication in MergeTree engines"
            ]
        },
        "clickhouse_sql_cheatsheet": get_clickhouse_sql_cheatsheet(),
        "asset_support": {
            "description": "All balance tables include asset fields for multi-asset tracking",
            "asset_fields": ["asset", "asset_id"]
        },
        "tables": [
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
    }
    
    # Add similarity search schema
    response["schemas"]["similarity_search"] = {
        "description": "Vector similarity search schema",
        "database": {
            "type": "Memgraph",
            "note": "Uses Memgraph's vector indexing capabilities"
        },
        "embedding_types": {
            "financial": {
                "description": "Financial behavior embeddings (6 dimensions)",
                "dimensions": 6
            },
            "temporal": {
                "description": "Temporal activity embeddings (4 dimensions)",
                "dimensions": 4
            },
            "network": {
                "description": "Network structure embeddings (4 dimensions)",
                "dimensions": 4
            },
            "joint": {
                "description": "Combined embeddings (14 dimensions)",
                "dimensions": 14
            }
        }
    }
    
    # Add usage examples (always included)
    response["usage_examples"] = {
        "money_flow": {
            "find_transaction_paths": {
                "description": "Find all paths between two addresses with specific asset (MAX 3 HOPS)",
                "query": "MATCH path = (a:Address {address: $from})-[rels:TO*BFS 1..3]->(b:Address {address: $to}) WHERE ALL(r IN rels WHERE r.asset = $asset) RETURN path LIMIT 1000"
            },
            "multi_asset_flow": {
                "description": "Analyze flows across multiple assets",
                "query": "MATCH (a:Address)-[r:TO]->(b:Address) WHERE r.asset IN $assets RETURN r.asset, COUNT(r) as transfers, SUM(r.volume) as total_volume LIMIT 1000"
            },
            "path_expansion": {
                "description": "Expand paths from an address (MAX 3 HOPS)",
                "query": "MATCH (a:Address {address: $addr}) CALL path.expand(a, ['TO'], [], 1, 3) YIELD result WHERE ALL(r IN relationships(result) WHERE r.asset = $asset) RETURN result LIMIT 1000"
            }
        },
        "balance_tracking": {
            "asset_balances_over_time": {
                "description": "Track balance changes for specific assets",
                "query": "SELECT toStartOfDay(timestamp) as day, asset, SUM(total_balance_delta) as daily_change FROM balance_delta_changes FINAL WHERE address = $addr AND asset IN $assets GROUP BY day, asset ORDER BY day LIMIT 1000"
            },
            "top_holders_by_asset": {
                "description": "Find top holders for each asset",
                "query": "SELECT asset, address, balance FROM current_balances_view WHERE asset IN $assets ORDER BY asset, balance DESC LIMIT 100"
            }
        },
        "similarity_search": {
            "find_similar_addresses": {
                "description": "Find addresses with similar behavior patterns",
                "query": {
                    "query_type": "by_address",
                    "reference_address": "5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f",
                    "embedding_type": "joint",
                    "limit": 10,
                    "similarity_metric": "cosine"
                }
            }
        },
        "important_limits": {
            "max_path_depth": "3 hops maximum for all path queries",
            "max_results": "1000 results maximum per query",
            "pagination": "Use SKIP and LIMIT for pagination when needed"
        }
    }
    
    return response


### BALANCE TRACKING TOOLS ###

@session_rate_limit
@mcp.tool(
    name="balance_schemas",
    description="Returns the blockchain balance tracking database schema with asset support. All balance tables include asset fields for multi-asset tracking.",
    tags={"balance tracking", "balance changes", "balance changes delta", "balances", "balance changes", "timeseries", "known addresses", "assets", "clickhouse"},
    annotations={
        "title": "Returns the blockchain balance database schema with asset support",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def balance_schemas(
        network: Annotated[str, Field(description="The blockchain network to query")],
        assets: Annotated[Optional[List[str]], Field(description="List of asset symbols to include in schema info. Use ['all'] for all assets, or specific symbols like ['TOR', 'TAO']. Defaults to network's native asset.")] = None,
        ctx: Context = None
) -> dict:
    """
    Retrieve the balance tracking schema for a given blockchain network with asset support.
    
    Args:
        network (str): The blockchain network to query.
        assets (List[str], optional): List of asset symbols to include. Defaults to network's native asset.
        ctx (Context): The context object containing request metadata.

    Returns:
        dict: The balance tracking schema including table names, columns, and asset information.
    """
    # Default to network's native asset if no assets specified
    if assets is None:
        assets = [get_network_asset(network)]
    
    balance_tracking_service = BalanceTrackingService(get_clickhouse_connection_string(network))
    try:
        result = await balance_tracking_service.get_balance_tracking_schema(assets)
        
        # Add network and asset information
        result["network_info"] = {
            "network": network,
            "native_asset": get_network_asset(network)
        }
        
        # Add database information
        result["database"] = {
            "type": "ClickHouse",
            "note": "OLAP database optimized for analytical queries, not OLTP",
            "key_features": [
                "Columnar storage for fast aggregations",
                "Time-series optimized with partitioning",
                "Materialized views for pre-computed metrics",
                "FINAL keyword for deduplication in MergeTree engines"
            ]
        }
        
        # Enhance asset support information
        if "asset_support" not in result:
            result["asset_support"] = {}
        
        result["asset_support"]["description"] = "All balance tables include asset fields for multi-asset tracking"
        result["asset_support"]["asset_fields"] = ["asset", "asset_id"]
        result["asset_support"]["native_asset"] = get_network_asset(network)
        result["asset_support"]["requested_assets"] = assets
        
        # Add ClickHouse-specific examples
        result["clickhouse_examples"] = {
            "deduplication": "SELECT * FROM balance_changes FINAL WHERE address = $addr AND asset = $asset",
            "prewhere": "SELECT * FROM balance_transfers PREWHERE asset = $asset WHERE from_address = $addr",
            "time_aggregation": "SELECT toStartOfDay(timestamp) as day, asset, SUM(amount) FROM balance_transfers FINAL GROUP BY day, asset",
            "window_function": "SELECT timestamp, asset, amount, avg(amount) OVER (PARTITION BY asset ORDER BY timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg FROM balance_transfers FINAL"
        }
        
        return result
    finally:
        balance_tracking_service.close()


@session_rate_limit
@mcp.tool(
    name="balance_query",
    description="Execute a balance tracking query on a blockchain network with asset support. All balance tables include asset fields for filtering by specific assets.",
    tags={"balance tracking", "balance changes", "balance changes delta", "balances", "balance changes", "known addresses", "assets"},
    annotations={
        "title": "Executes Clickhouse dialect SQL query against blockchain balance database with asset support",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_balance_query(
        network: Annotated[str, Field(description="The blockchain network to query")],
        query: Annotated[str, Field(description="The Clickhouse dialect SQL query to execute. Use asset field to filter by specific assets.")],
        assets: Annotated[Optional[List[str]], Field(description="List of asset symbols to filter results. Use ['all'] for all assets, or specific symbols like ['TOR', 'TAO']. Defaults to network's native asset.")] = None,
        ctx: Context = None
        ) -> dict:
    """
    Execute a balance tracking query on the specified blockchain network with asset support.
    
    Args:
        network (str): The blockchain network to query.
        query (str): The SQL query to execute. Use asset field to filter by specific assets.
        assets (List[str], optional): List of asset symbols to filter results. Defaults to network's native asset.
        ctx (Context): The context object containing request metadata.

    Returns:
        dict: The result of the balance tracking query with asset information.
    """

    # Default to network's native asset if no assets specified
    if assets is None:
        assets = [get_network_asset(network)]
    
    balance_tracking_service = BalanceTrackingService(get_clickhouse_connection_string(network))
    try:
        result = await balance_tracking_service.balance_tracking_query({"query": query, "assets": assets})
        
        # Add asset metadata to the result
        if isinstance(result, dict):
            result["asset_metadata"] = {
                "requested_assets": assets,
                "native_asset": get_network_asset(network),
                "note": "Results may include asset field for multi-asset tracking"
            }
        
        return result
    finally:
        balance_tracking_service.close()


### MONEY FLOW TOOLS ###

@session_rate_limit
@mcp.tool(
    name="money_flow_schema",
    description="Get the money flow graph database schema for a blockchain network with asset support. Nodes and edges include asset properties for multi-asset tracking.",
    tags={"money flow", "transactions", "flows", "money flows", "assets", "memgraph"},
    annotations={
        "title": "Get the money flow schema for a blockchain network with asset support",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def money_flow_schema(
        ctx: Context,
        network: Annotated[str, Field(description="The blockchain network to query")] = "torus"
) -> dict:
    """
    Get the money flow graph database schema for a given blockchain network with asset support.
    
    Args:
        network (str): The blockchain network to query.

    Returns:
        dict: The money flow schema including node labels, indexes, asset properties, and example queries.
    """
    
    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
    result = money_flow_tool.get_money_flow_schema()
    
    # Add network and asset information
    result["network_info"] = {
        "network": network,
        "native_asset": get_network_asset(network)
    }
    
    # Add database information
    result["database"] = {
        "type": "Memgraph",
        "note": "Uses Memgraph-specific Cypher dialect which differs from Neo4j Cypher",
        "key_differences": [
            "Path expansion syntax: path.expand() instead of variable-length patterns",
            "BFS/DFS support: Native BFS/DFS algorithms in path patterns",
            "Triggers and streams: Real-time processing capabilities",
            "Memory storage: In-memory graph for faster queries"
        ]
    }
    
    # Add Memgraph-specific examples if not already present
    if "memgraph_examples" not in result:
        result["memgraph_examples"] = {
            "path_expansion": "MATCH (a:Address {address: $addr}) CALL path.expand(a, ['TO'], [], 1, 3) YIELD result WHERE ALL(r IN relationships(result) WHERE r.asset = $asset) RETURN result LIMIT 1000",
            "bfs_traversal": "MATCH path = (a:Address {address: $addr})-[*BFS 1..3 (r, n | r.asset = $asset)]->(b:Address) RETURN path LIMIT 1000",
            "weighted_shortest": "MATCH path = (a:Address {address: $from})-[*WSHORTEST 1..3 (r, n | r.volume) volume]->(b:Address {address: $to}) RETURN path, volume LIMIT 1000",
            "important_note": "Maximum path depth is 3 hops, maximum results is 1000"
        }
    
    return result


@session_rate_limit
@mcp.tool(
    name="money_flow_query",
    description="Execute a money flow query on a blockchain network with asset support. Nodes and edges include asset properties for filtering by specific assets.",
    tags={"money flow", "transactions", "flows", "money flows", "assets"},
    annotations={
        "title": "Executes Memgraph Cypher query against money flow graph database with asset support",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_money_flow_query(
        network: Annotated[str, Field(description="The blockchain network to query")],
        query: Annotated[str, Field(description="The Cypher query to execute. Use asset properties to filter by specific assets.")],
        assets: Annotated[Optional[List[str]], Field(description="List of asset symbols to filter results. Use ['all'] for all assets, or specific symbols like ['TOR', 'TAO']. Defaults to network's native asset.")] = None,
        ctx: Context = None
) -> dict:
    """
    Execute a money flow query on the specified blockchain network with asset support.
    
    Args:
        network (str): The blockchain network to query.
        query (str): The Cypher query to execute. Use asset properties to filter by specific assets.
        assets (List[str], optional): List of asset symbols to filter results. Defaults to network's native asset.
        ctx (Context): The context object containing request metadata.

    Returns:
        dict: The result of the money flow query with asset information.
    """

    # Default to network's native asset if no assets specified
    if assets is None:
        assets = [get_network_asset(network)]
    
    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    try:
        money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
        result = await money_flow_tool.money_flow_query(query, assets)
        
        # Add asset metadata to the result
        if isinstance(result, dict):
            result["asset_metadata"] = {
                "requested_assets": assets,
                "native_asset": get_network_asset(network),
                "note": "Results may include asset properties on nodes and edges"
            }
        elif isinstance(result, list):
            # If result is a list, wrap it with metadata
            result = {
                "data": result,
                "asset_metadata": {
                    "requested_assets": assets,
                    "native_asset": get_network_asset(network),
                    "note": "Results may include asset properties on nodes and edges"
                }
            }
        
        return result
    finally:
        memgraph_driver.close()
        neo4j_driver.close()

### SIMILARITY SEARCH TOOLS ###

@session_rate_limit
@mcp.tool(
    name="similarity_search_schema",
    description="Get the similarity search schema for a blockchain network",
    tags={"similarity search", "vector search", "embeddings", "similar addresses", "memgraph"},
    annotations={
        "title": "Get the similarity search schema for a blockchain network",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def similarity_search_schema(
        ctx: Context,
        network: Annotated[str, Field(description="The blockchain network to query")] = "torus"
) -> dict:
    """
    Get the similarity search schema for a given blockchain network.
    This includes information about embedding types, query types, similarity metrics,
    pattern structures, and example queries.
    
    Args:
        network (str): The blockchain network to query.

    Returns:
        dict: The similarity search schema including embedding types, query types,
              similarity metrics, pattern structures, and example queries.
    """
    memgraph_driver = get_memgraph_driver(network)
    similarity_search_tool = SimilaritySearchTool(memgraph_driver)
    result = similarity_search_tool.get_similarity_search_schema()
    similarity_search_tool.close()
    
    # Add network information
    result["network_info"] = {
        "network": network,
        "note": "Similarity search operates on address behavior patterns across all assets"
    }
    
    # Add database information
    result["database"] = {
        "type": "Memgraph",
        "note": "Uses Memgraph's vector indexing capabilities for similarity search"
    }
    
    # Add Memgraph vector search examples
    result["memgraph_vector_examples"] = {
        "create_index": "CREATE VECTOR INDEX financial_embeddings ON :Address(financial_embedding) USING cosine_similarity",
        "search_similar": "CALL vector_search.search('financial_embeddings', $query_vector, 10) YIELD node, similarity RETURN node, similarity",
        "combined_search": "MATCH (a:Address {address: $addr}) WITH a.joint_embedding as query_vec CALL vector_search.search('joint_embeddings', query_vec, 20) YIELD node, similarity WHERE node.address != $addr RETURN node, similarity"
    }
    
    return result


@session_rate_limit
@mcp.tool(
    name="similarity_search_query",
    description="Execute a similarity search query on a blockchain network",
    tags={"similarity search", "vector search", "embeddings", "similar addresses"},
    annotations={
        "title": "Find addresses with similar patterns using vector similarity search",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_similarity_search_query(
        network: Annotated[str, Field(description="The blockchain network to query")],
        query: Annotated[dict, Field(description="The similarity search query parameters")],
        ctx: Context
) -> dict:
    """
    Execute a similarity search query on the specified blockchain network.
    
    Args:
        network (str): The blockchain network to query.
        query (dict): The similarity search query parameters, including:
            - query_type: How to specify the search query ('by_address', 'by_financial_pattern', etc.)
            - embedding_type: Type of embedding to use ('financial', 'temporal', 'network', or 'joint')
            - reference_address: (Optional) Address to use as reference when query_type is 'by_address'
            - financial_pattern: (Optional) Financial pattern when query_type is 'by_financial_pattern'
            - temporal_pattern: (Optional) Temporal pattern when query_type is 'by_temporal_pattern'
            - network_pattern: (Optional) Network pattern when query_type is 'by_network_pattern'
            - combined_pattern: (Optional) Combined pattern when query_type is 'by_combined_pattern'
            - limit: (Optional) Number of similar nodes to retrieve (default: 10)
            - similarity_metric: (Optional) Similarity metric to use (default: 'cosine')
            - min_similarity_score: (Optional) Minimum similarity threshold

    Returns:
        dict: The result of the similarity search query, including raw nodes and similarity scores.
    """
    memgraph_driver = get_memgraph_driver(network)
    similarity_search_tool = SimilaritySearchTool(memgraph_driver)
    result = similarity_search_tool.similarity_search_query(query)
    similarity_search_tool.close()
    return result


if __name__ == "__main__":
    mcp.run(transport="sse", host="0.0.0.0", port=8005, log_level="debug")
