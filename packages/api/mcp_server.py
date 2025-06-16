import asyncio
import json
from typing import List, Optional, Dict, Any
from typing import Annotated

from loguru import logger
from pydantic import Field
from fastmcp import FastMCP, Context
from packages.api.routers import get_memgraph_driver, get_neo4j_driver
from packages.api.services.money_flow_service import MoneyFlowService
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


async def get_instructions():
    network = os.getenv('NETWORK', 'torus')
    assets = get_network_asset(network)

    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
    money_flow_schema = money_flow_tool.get_money_flow_schema()

    similarity_search_tool = SimilaritySearchTool(memgraph_driver)
    similarity_schema = similarity_search_tool.get_similarity_search_schema()

    balance_tracking_tool = BalanceTrackingTool(get_clickhouse_connection_string(network))
    balance_schema = await balance_tracking_tool.get_balance_tracking_schema()

    return f"""
    You are AI assistant on {network} blockchain and {assets} assets.

    1) You have access to money flow graph database, which describes aggregated flow of assets between addresses.
        a) money flow graph database schema: {money_flow_schema}
        b) use memgraph cypher syntax, neo4j cypher is not compatible
        c) when asked about certain addresses, explore it's connections and relationships with other addresses using 4 hops maximum
        d) when asked about transfers/flows between addresses:
           - FIRST: use BFS shortest path to find the shortest connection
           - THEN: if needed, use explicit relationship matching for accessing properties
           - ALWAYS filter by asset type in path conditions
           - Use up to 4 hops maximum for path exploration

        e) memgraph-specific syntax requirements:
           - Use BFS for shortest paths: MATCH path = (start)-[*BFS ..5]-(target) 
           - Use size() instead of length() function
           - PREFER inline filtering for performance: -[*BFS ..5 (r, n | condition)]->
           - FALLBACK to ALL() only when inline filtering insufficient
           - Use path.expand() with inline filtering: CALL path.expand(node, ['TO'], [], min, max, (r, n | condition))
           - CANNOT access relationship/node properties with array[index].property syntax
           - ALWAYS return * or full objects to get all available properties, let the schema guide what's available
           - Add HOPS LIMIT for complex queries: USING HOPS LIMIT 10000;
           - Use relationships(path) and nodes(path) functions to extract elements from paths
           - Use reduce() for aggregations during traversal instead of post-processing
           - Cannot use array[index].property syntax in Memgraph - use explicit relationship matching instead.
           - Use ['TO'] for both directions, ['TO>'] for outgoing only, ['<TO'] for incoming only.
           - Use reduce() during traversal instead of collecting large result sets for memory efficiency.
        
        f) use money_flow_query for execution

    2) You have access to similarity search functionality on top of the money flow graph database.
        a) similarity search specific schema: {similarity_schema}
        b) use memgraph cypher syntax, neo4j cypher is not compatible  
        c) similarity_search_query tool for execution

    3) You have access to balance database, which describes balance changes over time, balance transfers, known addresses.
        a) balance database schema: {balance_schema}
        b) use clickhouse sql syntax
        c) use balance_query tool for execution

    4) UNIVERSAL ADDRESS LABELING REQUIREMENT:
        a) AFTER obtaining addresses from ANY query (money flow, similarity search, or balance queries), ALWAYS check if they exist in the known_addresses table
        b) Use batch SQL query for performance: SELECT address, label FROM known_addresses WHERE address IN ('addr1', 'addr2', ...)
        c) Use balance_query tool to access the known_addresses table
        d) Include address labels/names in your response when available
 
    """

@session_rate_limit
@mcp.tool(
    name="instructions",
    description="Get money flow, balance tracking, and similarity search schema information for the MCP server.",
    tags={"schema", "networks", "assets", "money flow", "balance tracking", "similarity search"},
    annotations={
        "title": "Get comprehensive schema and capability information",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def instructions():
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
    return await get_instructions()



@session_rate_limit
@mcp.tool(
    name="explore_address"
    description="Explore connections and relationships of a specific address in the money flow graph.",
    tags={"money flow", "exploration", "address relationships"},
    annotations={
        "title": "Explore address connections in money flow graph",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    })
async def explore_address(address: Annotated[str, Field(description="The address to explore in the money flow graph.")]) -> dict:
    """
    Explore connections and relationships of a specific address in the money flow graph.

    Args:
        address (str): The address to explore in the money flow graph.

    Returns:
        dict: The result of the exploration query with address connections and relationships.
    """
    network = os.getenv("NETWORK", "torus").lower()
    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    try:
        service = MoneyFlowService(memgraph_driver)
        money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
        # TODO: Implement the explore_address method in MoneyFlowTool
        #result = await money_flow_tool.explore_address(address)
        return {"data": result}
    finally:
        memgraph_driver.close()
        neo4j_driver.close()

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


@session_rate_limit
@mcp.tool(
    name="balance_query",
    description="Execute a balance related query.",
    tags={"balance tracking", "balance changes", "balance changes delta", "balances", "balance changes",
          "known addresses", "assets"},
    annotations={
        "title": "Executes Clickhouse dialect SQL query against blockchain balance database with asset support",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)

async def execute_balance_query(query: Annotated[str, Field(
    description="The Clickhouse dialect SQL query to execute. Use asset field to filter by specific assets.")]) -> dict:
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



if __name__ == "__main__":
    setup_logger("chain-insights-mcp-server")
    schema_response = asyncio.run(get_instructions())
    json_schema = json.dumps(schema_response, indent=2)
    logger.info(f"Schema loaded: {json_schema}")
    mcp.run(transport="sse", host="0.0.0.0", port=8005, log_level="debug")
