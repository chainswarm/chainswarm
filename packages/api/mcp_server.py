import asyncio
import json
from typing import List, Optional, Dict, Any
from typing import Annotated

from loguru import logger
from pydantic import Field
from fastmcp import FastMCP, Context
from packages.api.routers import get_memgraph_driver, get_neo4j_driver
from packages.api.services.balance_tracking_service import BalanceTrackingService
from packages.api.tools.balance_tracking import BalanceTrackingTool
from packages.api.tools.money_flow import MoneyFlowTool
from packages.api.tools.similarity_search import SimilaritySearchTool
from packages.indexers.base import get_clickhouse_connection_string, setup_logger
from packages.indexers.substrate import get_network_asset
from packages.api.middleware.mcp_session_rate_limiting import (
    session_rate_limit,
)
import os
import clickhouse_connect

mcp = FastMCP(
    name="Chan Insights MCP Server",
    instructions="This MCP server provides access to balance tracking, money flow, similarity search, and known addresses data.",
)


async def get_assets_from_clickhouse(network: str) -> List[str]:
    """Query ClickHouse to get available assets for the network"""
    try:
        connection_params = get_clickhouse_connection_string(network)
        client = clickhouse_connect.get_client(
            host=connection_params['host'],
            port=int(connection_params['port']),
            username=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database']
        )
        
        # Ultra simple query to get assets from the view
        query = "SELECT asset FROM available_assets_view LIMIT 1000"
        
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
    """Get Memgraph-specific Cypher cheatsheet with examples from money_flow_service"""
    return {
        "path_analysis": {
            "description": "Path-based queries for exploring connections between addresses",
            "examples": [
                {
                    "name": "Shortest path between addresses",
                    "query": "MATCH path = (start:Address {address: $source_address})-[rels:TO*BFS]-(target:Address {address: $target_address}) WHERE size(path) > 0 RETURN path LIMIT 1000"
                },
                {
                    "name": "Shortest path with asset filter",
                    "query": "MATCH path = (start:Address {address: $source_address})-[rels:TO*BFS]-(target:Address {address: $target_address}) WHERE ALL(rel IN rels WHERE rel.asset = $asset) RETURN path LIMIT 1000"
                },
                {
                    "name": "Path exploration with depth control",
                    "query": "MATCH (a:Address) WHERE a.address IN $addresses CALL path.expand(a, ['TO'], [], 0, $depth) YIELD result as path RETURN path LIMIT 1000"
                },
                {
                    "name": "Directional path exploration (outgoing)",
                    "query": "MATCH (a:Address) WHERE a.address IN $addresses CALL path.expand(a, ['TO>'], [], 0, $depth) YIELD result as path RETURN path LIMIT 1000"
                },
                {
                    "name": "Directional path exploration (incoming)",
                    "query": "MATCH (a:Address) WHERE a.address IN $addresses CALL path.expand(a, ['<TO'], [], 0, $depth) YIELD result as path RETURN path LIMIT 1000"
                }
            ]
        },
        "community_analysis": {
            "description": "Queries for analyzing community structures and relationships",
            "examples": [
                {
                    "name": "Aggregate community metrics",
                    "query": "MATCH (a:Address) WHERE a.community_id IS NOT NULL WITH a.community_id AS community_id, COLLECT(a) AS community_addresses WITH community_id, REDUCE(volume_in = 0, addr IN community_addresses | volume_in + CASE WHEN addr.volume_in IS NOT NULL THEN addr.volume_in ELSE 0 END) AS total_volume_in, REDUCE(volume_out = 0, addr IN community_addresses | volume_out + CASE WHEN addr.volume_out IS NOT NULL THEN addr.volume_out ELSE 0 END) AS total_volume_out, SIZE(community_addresses) AS address_count RETURN community_id, total_volume_in, total_volume_out, address_count ORDER BY address_count DESC LIMIT 100"
                },
                {
                    "name": "Find inter-community transfers",
                    "query": "MATCH (a1:Address)-[r:TO]->(a2:Address) WHERE a1.community_id <> a2.community_id AND a1.community_id IS NOT NULL AND a2.community_id IS NOT NULL WITH a1.community_id AS from_community_id, a2.community_id AS to_community_id, SUM(r.volume) AS total_volume, COUNT(r) AS transfer_count RETURN from_community_id, to_community_id, total_volume, transfer_count ORDER BY total_volume DESC LIMIT 100"
                },
                {
                    "name": "Extract subcommunities",
                    "query": "MATCH (a:Address) WHERE a.community_id IS NOT NULL AND a.community_ids IS NOT NULL WITH a.community_id AS primary_community, a.community_ids AS all_communities UNWIND all_communities AS subcommunity_id WHERE subcommunity_id <> primary_community RETURN primary_community, COLLECT(DISTINCT subcommunity_id) AS subcommunities"
                }
            ]
        },
        "pattern_detection": {
            "description": "Queries for detecting specific transaction patterns",
            "examples": [
                {
                    "name": "Fan-in pattern detection (multiple sources to one target)",
                    "query": "MATCH (source:Address)-[r:TO]->(target:Address) WHERE r.volume >= $min_volume WITH target, count(DISTINCT source) AS source_count, sum(r.volume) AS total_volume WHERE source_count >= $min_sources AND total_volume >= $min_volume MATCH (source:Address)-[r:TO]->(target) WHERE r.volume >= $min_volume WITH target, source, r ORDER BY r.last_transfer_timestamp DESC WITH target, collect({source: source.address, timestamp: r.last_transfer_timestamp, volume: r.volume}) AS incoming_txs UNWIND incoming_txs AS tx WITH target, tx ORDER BY tx.timestamp DESC WITH target, collect(tx) AS recent_txs, min(tx.timestamp) AS window_start, max(tx.timestamp) AS window_end WHERE (window_end - window_start) <= $time_window AND size(recent_txs) >= $min_sources RETURN target.address, size(recent_txs) AS source_count, reduce(total = 0, tx IN recent_txs | total + tx.volume) AS total_volume ORDER BY source_count DESC, total_volume DESC LIMIT 100"
                },
                {
                    "name": "Fan-out pattern detection (one source to multiple targets)",
                    "query": "MATCH (source:Address)-[r:TO]->(target:Address) WHERE r.volume >= $min_volume WITH source, count(DISTINCT target) AS target_count, sum(r.volume) AS total_volume WHERE target_count >= $min_targets AND total_volume >= $min_volume MATCH (source)-[r:TO]->(target:Address) WHERE r.volume >= $min_volume WITH source, target, r ORDER BY r.last_transfer_timestamp DESC WITH source, collect({target: target.address, timestamp: r.last_transfer_timestamp, volume: r.volume}) AS outgoing_txs UNWIND outgoing_txs AS tx WITH source, tx ORDER BY tx.timestamp DESC WITH source, collect(tx) AS recent_txs, min(tx.timestamp) AS window_start, max(tx.timestamp) AS window_end WHERE (window_end - window_start) <= $time_window AND size(recent_txs) >= $min_targets RETURN source.address, size(recent_txs) AS target_count, reduce(total = 0, tx IN recent_txs | total + tx.volume) AS total_volume ORDER BY target_count DESC, total_volume DESC LIMIT 100"
                },
                {
                    "name": "High-volume transfer detection",
                    "query": "MATCH (source:Address)-[r:TO]->(target:Address) WHERE r.volume > $threshold RETURN source.address, target.address, r.volume, r.first_transfer_timestamp ORDER BY r.volume DESC LIMIT 100"
                }
            ]
        },
        "temporal_analysis": {
            "description": "Time-based analysis of transaction patterns",
            "examples": [
                {
                    "name": "Transaction frequency pattern analysis",
                    "query": "MATCH ()-[r:TO]->() WHERE r.transfer_count >= $min_transfers AND size(r.tx_frequency) >= $min_transfers - 1 WITH r, startNode(r) AS source, endNode(r) AS target, avg(toFloat(r.tx_frequency)) AS avg_interval, stdDev(r.tx_frequency) AS std_interval, min(r.tx_frequency) AS min_interval, max(r.tx_frequency) AS max_interval WITH r, source, target, avg_interval, std_interval, min_interval, max_interval, CASE WHEN std_interval <= (avg_interval * 0.3) THEN 'regular' WHEN min_interval <= (avg_interval * 0.2) THEN 'burst' ELSE 'inconsistent' END AS pattern RETURN source.address, target.address, pattern, r.transfer_count, avg_interval, std_interval ORDER BY r.transfer_count DESC LIMIT 100"
                },
                {
                    "name": "Regular transaction pattern detection",
                    "query": "MATCH ()-[r:TO]->() WHERE r.transfer_count >= $min_transfers AND size(r.tx_frequency) >= $min_transfers - 1 WITH r, startNode(r) AS source, endNode(r) AS target, avg(toFloat(r.tx_frequency)) AS avg_interval, stdDev(r.tx_frequency) AS std_interval WHERE std_interval <= (avg_interval * 0.3) RETURN source.address, target.address, r.transfer_count, avg_interval, std_interval ORDER BY r.transfer_count DESC LIMIT 100"
                },
                {
                    "name": "Temporal sequence detection",
                    "query": "MATCH path = (a:Address)-[r1:TO]->(b:Address)-[r2:TO]->(c:Address) WHERE r1.volume >= $min_volume AND r2.volume >= $min_volume AND r2.first_transfer_timestamp >= r1.last_transfer_timestamp AND (r2.first_transfer_timestamp - r1.last_transfer_timestamp) <= $max_time_gap RETURN [a.address, b.address, c.address] AS address_sequence, [r1.volume, r2.volume] AS volumes, [r1.first_transfer_timestamp, r2.first_transfer_timestamp] AS timestamps ORDER BY r1.volume + r2.volume DESC LIMIT 100"
                },
                {
                    "name": "Time-windowed transfer analysis",
                    "query": "MATCH (a:Address)-[r:TO]->(b:Address) WHERE r.first_transfer_timestamp >= $start_time AND r.last_transfer_timestamp <= $end_time RETURN a.address, b.address, r.volume, r.transfer_count, r.first_transfer_timestamp, r.last_transfer_timestamp ORDER BY r.volume DESC LIMIT 1000"
                }
            ]
        },
        "performance_optimization": {
            "description": "Query optimization techniques for Memgraph",
            "examples": [
                {
                    "name": "Index usage for address lookup",
                    "query": "USING INDEX SEEK ON :Address(address) MATCH (a:Address {address: $addr}) RETURN a"
                },
                {
                    "name": "Early filtering with limit",
                    "query": "MATCH (a:Address) WHERE a.address STARTS WITH '5C' WITH a LIMIT 1000 MATCH (a)-[r:TO {asset: $asset}]->(b) RETURN a, r, b LIMIT 1000"
                },
                {
                    "name": "Limit early in path expansion",
                    "query": "MATCH (a:Address {address: $addr}) CALL path.expand(a, ['TO'], [], 1, 3) YIELD result LIMIT 100 RETURN result"
                },
                {
                    "name": "Parameterized asset filtering",
                    "query": "MATCH (a:Address)-[r:TO]->(b:Address) WHERE r.asset IN $assets RETURN a, r, b LIMIT 1000"
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


async def _schema():
    """
    Get comprehensive schema information for the MCP server.
    Queries real databases for accurate schema information.

    Returns:
        dict: Complete schema including:
            - Network and available assets
            - Money flow graph schema
            - Balance tracking schema
            - Similarity search schema
            - Usage examples for each capability
    """

    network = os.getenv("NETWORK", "torus").lower()
    assets = await get_assets_from_clickhouse(network)

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

    balance_tracking_tool = BalanceTrackingTool(get_clickhouse_connection_string(network))
    balance_schema = await balance_tracking_tool.get_balance_tracking_schema()
    response["schemas"]["balance_tracking"] = balance_schema

    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)

    money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
    money_flow_schema = money_flow_tool.get_money_flow_schema()
    response["schemas"]["money_flow"] = money_flow_schema

    similarity_search_tool = SimilaritySearchTool(memgraph_driver)
    similarity_schema = similarity_search_tool.get_similarity_search_schema()

    response["schemas"]["similarity_search"] = similarity_schema
    return response


@session_rate_limit
@mcp.tool(
    name="schema",
    description="Get money flow, balance tracking, and similarity search schema information for the MCP server.",
    tags={"schema", "networks", "assets", "money flow", "balance tracking", "similarity search"},
    annotations={
        "title": "Get comprehensive schema and capability information",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def schema():
    """
    Get comprehensive schema information for the MCP server.
    Queries real databases for accurate schema information.
    
    Returns:
        dict: Complete schema including:
            - Network and available assets
            - Money flow graph schema
            - Balance tracking schema
            - Similarity search schema
            - Usage examples for each capability
    """
    return await _schema()


@session_rate_limit
@mcp.tool(
    name="balance_query",
    description="Execute a balance related query.",
    tags={"balance tracking", "balance changes", "balance changes delta", "balances", "balance changes", "known addresses", "assets"},
    annotations={
        "title": "Executes Clickhouse dialect SQL query against blockchain balance database with asset support",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_balance_query(query: Annotated[str, Field(description="The Clickhouse dialect SQL query to execute. Use asset field to filter by specific assets.")]) -> dict:
    """
    Execute a balance tracking query on the specified blockchain network with asset support.
    
    Args:
        query (str): The SQL query to execute. Use asset field to filter by specific assets.

    Returns:
        dict: The result of the balance tracking query with asset information.
    """
    network = os.getenv("NETWORK", "torus").lower()
    balance_tracking_service = BalanceTrackingTool(get_clickhouse_connection_string(network))
    result = await balance_tracking_service.balance_tracking_query(query)
    return result


@session_rate_limit
@mcp.tool(
    name="money_flow_query",
    description="Execute a money flow query.",
    tags={"money flow", "shortest path", "community detection", "pattern detection", "temporal analysis"},
    annotations={
        "title": "Executes Memgraph Cypher query against money flow graph database",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_money_flow_query(query: Annotated[str, Field(description="The Cypher query to execute. Use asset properties to filter by specific assets.")]) -> dict:
    """
    Execute a money flow query on the specified blockchain network with asset support.
    
    Args:
        query (str): The Cypher query to execute. Use asset properties to filter by specific assets.

    Returns:
        dict: The result of the money flow query with asset information.
    """

    network = os.getenv("NETWORK", "torus").lower()
    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    try:
        money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
        result = await money_flow_tool.money_flow_query(query)
        result = {
            "data": result,
        }
        
        return result
    finally:
        memgraph_driver.close()
        neo4j_driver.close()


@session_rate_limit
@mcp.tool(
    name="similarity_search_query",
    description="Execute a similarity search query on a money flow graph database.",
    tags={"similarity search", "vector search", "embeddings", "similar addresses"},
    annotations={
        "title": "Executes Memgraph Cypher query against money flow graph database",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_similarity_search_query(query: Annotated[dict, Field(description="The similarity search query parameters")]) -> dict:
    """
    Execute a similarity search query on the specified blockchain network.
    
    Args:
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
    network = os.getenv("NETWORK", "torus").lower()
    memgraph_driver = get_memgraph_driver(network)
    similarity_search_tool = SimilaritySearchTool(memgraph_driver)
    result = similarity_search_tool.similarity_search_query(query)
    similarity_search_tool.close()
    return result


if __name__ == "__main__":
    setup_logger("chain-insights-mcp-server")
    schema_response = asyncio.run(_schema())
    json_schema = json.dumps(schema_response, indent=2)
    logger.info(f"Schema loaded: {json_schema}")
    mcp.run(transport="sse", host="0.0.0.0", port=8005, log_level="debug")
