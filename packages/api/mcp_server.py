from typing import List, Optional
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

mcp = FastMCP(
    name="Chan Insights MCP Server",
    instructions="This MCP server provides access to balance tracking, money flow, similarity search, and known addresses data.",
)


@session_rate_limit
@mcp.tool(
    name="networks",
    description="Retrieve a list of supported blockchain networks",
    tags={"networks", "blockchains"},
    annotations={
        "title": "Supported Blockchain Networks",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def networks(ctx: Context) -> List[dict]:
    """
    Retrieve a list of supported blockchain networks.
    This function returns a list of networks with their names and symbols.

    Returns:
        List[dict]: A list of dictionaries containing network names and symbols.
    """

    return [
        {"name": "torus", "symbol": "TORUS", "native_asset": "TOR"},
        {"name": "bittensor", "symbol": "TAO", "native_asset": "TAO"},
        {"name": "polkadot", "symbol": "DOT", "native_asset": "DOT"},
    ]


### BALANCE TRACKING TOOLS ###

@session_rate_limit
@mcp.tool(
    name="balance_schemas",
    description="Returns the blockchain balance tracking database schema with asset support. All balance tables include asset fields for multi-asset tracking.",
    tags={"balance tracking", "balance changes", "balance changes delta", "balances", "balance changes", "timeseries", "known addresses", "assets"},
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
        
        # Add additional asset metadata
        if "asset_support" not in result:
            result["asset_support"] = {}
        
        result["asset_support"]["native_asset"] = get_network_asset(network)
        result["asset_support"]["requested_assets"] = assets
        
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
    tags={"money flow", "transactions", "flows", "money flows", "assets"},
    annotations={
        "title": "Get the money flow schema for a blockchain network with asset support",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def money_flow_schema(
        ctx: Context,
        network: Annotated[str, Field(description="The blockchain network to query")] = "torus",
        assets: Annotated[Optional[List[str]], Field(description="List of asset symbols to include in schema info. Use ['all'] for all assets, or specific symbols like ['TOR', 'TAO']. Defaults to network's native asset.")] = None
) -> dict:
    """
    Get the money flow graph database schema for a given blockchain network with asset support.
    
    Args:
        network (str): The blockchain network to query.
        assets (List[str], optional): List of asset symbols to include. Defaults to network's native asset.

    Returns:
        dict: The money flow schema including node labels, indexes, asset properties, and example queries.
    """

    # Default to network's native asset if no assets specified
    if assets is None:
        assets = [get_network_asset(network)]
    
    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
    result = money_flow_tool.get_money_flow_schema(assets)
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
    tags={"similarity search", "vector search", "embeddings", "similar addresses"},
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


### SESSION MANAGEMENT TOOLS ###

@mcp.tool(
    name="session_stats",
    description="Get current session rate limiting statistics",
    tags={"session", "rate limiting", "statistics"},
    annotations={
        "title": "Get session rate limiting statistics",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def session_stats(ctx: Context) -> dict:
    """
    Get current session rate limiting statistics.
    Args:
        ctx (Context): The context object containing session metadata.

    Returns:
        dict: Session statistics including usage and limits.
    """
    from packages.api.middleware.mcp_session_rate_limiting import extract_session_info
    
    session_info = extract_session_info(ctx)
    session_id = session_info["session_id"]
    
    stats = mcp_session_rate_limiter.get_session_stats(session_id)
    if not stats:
        return {
            "session_id": session_id,
            "message": "Session not found or no requests made yet",
            "rate_limiting_enabled": mcp_session_rate_limiter.enabled
        }
    
    return stats


@mcp.tool(
    name="all_sessions_stats",
    description="Get statistics for all active sessions (admin tool)",
    tags={"session", "rate limiting", "statistics", "admin"},
    annotations={
        "title": "Get all sessions statistics",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def all_sessions_stats(ctx: Context) -> dict:
    """
    Get statistics for all active sessions.
    Args:
        ctx (Context): The context object containing session metadata.

    Returns:
        dict: Statistics for all active sessions.
    """
    return mcp_session_rate_limiter.get_all_sessions_stats()


if __name__ == "__main__":
    mcp.run(transport="sse", host="0.0.0.0", port=8005, log_level="debug")
