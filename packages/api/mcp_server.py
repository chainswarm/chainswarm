import asyncio
import json
from typing import List, Optional, Dict, Any
from typing import Annotated
from loguru import logger
from pydantic import Field
from fastmcp import FastMCP, Context
from packages.api.routers import get_memgraph_driver, get_neo4j_driver
from packages.api.tools.balance_tracking import BalanceTrackingTool
from packages.api.tools.balance_transfers import BalanceTransfersTool
from packages.api.tools.money_flow import MoneyFlowTool
from packages.api.tools.similarity_search import SimilaritySearchTool
from packages.indexers.base import get_clickhouse_connection_string, setup_logger
from packages.indexers.substrate import get_network_asset
from packages.api.middleware.mcp_session_rate_limiting import (
    session_rate_limit,
)
import os
import clickhouse_connect

network = os.getenv("NETWORK", "torus").lower()

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


async def get_user_guide():
    """User-facing documentation for MCP server capabilities"""
    assets = get_network_asset(network)

    return f"""

RETURN EXACT TEXT BELOW  WITHOUT CHANGES: 
    `
    # {network.upper()} Blockchain Analytics MCP Server
    
    **🚀 FIRST TIME SETUP**: Run the `instructions` tool first so the AI assistant learns how to use the blockchain analytics tools properly.
    
    Welcome to your blockchain analytics assistant! This MCP server provides comprehensive analysis capabilities for the {network} blockchain with {assets} asset support.
    
    ## 🚀 Getting Started
    
    This server connects to multiple data sources to give you complete blockchain insights:
    - **Aggregated Money Flow Graph**: Aggregated transaction connections between addresses
    - **Similarity Search**: Find addresses with similar behavior patterns
    - **Balances**: Historical balance transfers, balances at given point in time, balance change deltas, known addresses lookup
    
    
    ## 🌊 Money Flow Analysis
    
    **What it does**: Maps the network of transactions between addresses, showing how money flows through the blockchain.
    
    **You can ask about**:
    - Address connections and relationships
    - Transaction paths between any two addresses
    - Network topology and influential nodes
    - Volume flows and transaction patterns
    
    **Example questions**:
    - "Show me connections around address [ADDRESS]"
    - "Find the path between [ADDRESS1] and [ADDRESS2]"
    - "What addresses are most connected to [ADDRESS]?"
    - "Map the transaction network with 2 degrees of separation from [ADDRESS]"
    
    ## 💰 Account Balance Analytics
    
    **What it does**: Tracks balance changes over time, analyzes transfer transactions between addresses, and maintains a database of known/labeled addresses (exchanges, treasuries, bridges, etc.).
    
    **You can ask about**:
    - Historical balance changes for any address
    - Known addresses and their labels/purposes
    - Transaction history with detailed records
    - Address behavior patterns and classifications
    - Relationship analysis between addresses
    - Network flow and economic indicators
    - Suspicious activity and anomaly detection
    
    **Example questions**:
    - "What are the well-known addresses on this blockchain?"
    - "Show me the transaction history for [ADDRESS]"
    - "What's the balance history of [ADDRESS] over the last month?"
    - "Find all treasury and DAO addresses"
    - "Identify addresses with suspicious transaction patterns"
    - "Analyze the relationship between [ADDRESS1] and [ADDRESS2]"
    - "What's the token velocity for [ASSET] over the last quarter?"
    - "Show me addresses classified as 'whales' for [ASSET]"
    
    ## 🔍 Similarity & Pattern Detection
    
    **What it does**: Analyzes transaction patterns to find addresses that behave similarly, helping identify related accounts or suspicious activity.
    
    **You can ask about**:
    - Addresses with similar transaction patterns
    - Potential related wallets or accounts
    - Behavioral clustering and anomalies
    - Pattern-based investigations
    
    **Example questions**:
    - "Find addresses similar to [ADDRESS]"
    - "What addresses have unusual transaction patterns?"
    - "Group addresses by their behavior patterns"
    - "Are there any addresses that might be related to [ADDRESS]?"
    
    ## 💡 Advanced Analytics
    
    Combine multiple data sources for comprehensive insights:
    - "Analyze the complete profile of [ADDRESS] including connections, history, and similar addresses"
    - "Map the ecosystem around [KNOWN_ENTITY] showing all related addresses"
    - "Find the flow of [AMOUNT] tokens from [SOURCE] and trace where they went"
    - "What's the network structure of major token holders?"
    
    ## 🎯 Pro Tips
    
    1. **Start Broad**: Ask general questions first, then drill down into specifics
    2. **Use Address Labels**: Ask about "exchanges", "bridges", "treasuries" to find known entities
    3. **Combine Approaches**: Use flow analysis + balance history + similarity for complete pictures
    4. **Historical Analysis**: Include time ranges for balance and transaction queries
    5. **Network Exploration**: Start with 1-2 degree connections, expand if needed
    
    ## ⚡ Quick Reference
    
    **Most Popular Queries**:
    - "What are the well-known addresses?" (Great starting point)
    - "Show me the most active addresses" (Find network hubs)
    - "Trace [ADDRESS] connections" (Explore around specific address)
    - "Find path between [ADDR1] and [ADDR2]" (Direct relationship analysis)
    - "List all [TYPE] addresses" (Find specific entity types)
    
    Just ask your questions in natural language - the assistant will use the appropriate tools and data sources to provide comprehensive blockchain insights!
    `
"""


async def get_instructions():
    """
    Generate comprehensive LLM instructions for blockchain analytics tools.

    This function dynamically builds instructions based on the actual schemas
    of available tools, ensuring the AI assistant has accurate information
    about data structures and capabilities.
    """

    # Get network configuration
    assets = get_network_asset(network)

    # Initialize database connections
    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)

    # Initialize tools and get their schemas
    money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
    money_flow_schema = money_flow_tool.schema()

    similarity_search_tool = SimilaritySearchTool(memgraph_driver)
    similarity_schema = similarity_search_tool.schema()

    balance_tracking_tool = BalanceTrackingTool(get_clickhouse_connection_string(network))
    balance_schema = await balance_tracking_tool.schema()
    
    balance_transfers_tool = BalanceTransfersTool(get_clickhouse_connection_string(network))
    balance_transfers_schema = await balance_transfers_tool.schema()

    return f"""
# {network.upper()} Blockchain Analytics Assistant Instructions

You are an AI assistant specialized in {network} blockchain analytics with {assets} asset support.
Your task is to help users analyze blockchain data using the available tools.
 
**Connection Exploration**
- Tool: `money_flow_explore_address_connections`
- Purpose: Discover address relationships and transaction networks
- Usage: Specify addresses, depth (1-5 hops), and direction (in/out/all)

**Path Finding**
- Tool: `money_flow_shortest_path` 
- Purpose: Find transaction paths between two specific addresses
- Usage: Provide source and target addresses, optionally filter by assets

**Advanced Graph Queries**
- Tool: `money_flow_query`
- **Database**: MEMGRAPH (NOT Neo4j)
- **Schema**: {money_flow_schema}

**MEMGRAPH CYPHER SYNTAX REQUIREMENTS:**
```cypher
// ✅ CORRECT Memgraph syntax:
MATCH path = (start)-[*BFS ..3]-(target)           // BFS traversal
RETURN size(path) AS path_length                   // Use size(), not length()
MATCH (a)-[*BFS ..5 (r, n | n.balance > 1000)]->(b)  // Inline filtering

// ❌ AVOID Neo4j syntax:
MATCH path = (start)-[*..3]-(target)               // Standard traversal
RETURN length(path)                                // length() function
// ❌ AVOID: List comprehensions inside functions
WITH collect(addresses) as addr_list
RETURN [addr IN addr_list | addr][0..3]            // NOT SUPPORTED

// ✅ CORRECT: Separate collection and slicing
WITH collect(addresses) as addr_list
RETURN size(addr_list) as count, addr_list[0..3] as top_items
```

**Key Memgraph Differences:**
- BFS paths: `[*BFS ..max_depth]` or `[*BFS min..max]`
- Filtering: `[*BFS ..5 (relationship, node | condition)]`
- List comprehensions: Work in RETURN/WITH but NOT inside functions like size()
- Aggregation: Use `reduce()` for path calculations, `collect()` then separate operations
- Directions: `['TO']` (both), `['TO>']` (outgoing), `['<TO']` (incoming)
- Path functions: `relationships(path)`, `nodes(path)`
- Collection operations: Use separate queries instead of nested comprehensions

**CRITICAL: For complex grouping, use multiple queries instead of nested list comprehensions:**
```cypher
// ❌ AVOID: This Neo4j pattern fails in Memgraph
MATCH (a:Address)
WITH a.community_id as community, collect(a) as addresses
RETURN community, [addr IN addresses | addr][0..3] as top_addresses

// ✅ CORRECT: Split into separate operations
MATCH (a:Address) 
WHERE a.community_page_rank  IS NOT NULL
WITH a.community_id as community, max(a.community_page_rank ) as max_rank
MATCH (b:Address) 
WHERE b.community_page_rank  = max_rank AND b.community_id = community
RETURN community, b.address, b.community_page_rank 
ORDER BY community ASC
``` 


### 🎯 Pattern Recognition Tool

**Similarity Search**
- Tool: `similarity_search_query`
- Purpose: Find addresses with similar transaction patterns and behaviors
- **Schema**: {similarity_schema}
- Usage: Vector-based similarity matching for behavioral analysis

### 💰 Account Balance Analytics

**Balance Tracking**
- Tool: `balance_tracking_query`
- Purpose: Historical balance changes, balance change deltas, known addresses
- **Database**: ClickHouse
- **Schema**: {balance_schema}
- **Core Tables**:
  - `balance_changes`: Stores balance snapshots for addresses at specific block heights
  - `balance_delta_changes`: Stores balance changes (deltas) between consecutive blocks
- **Available Views**:
  - `balances_current_view`: Latest balance for each address and asset
  - `balance_significant_changes_view`: Significant balance changes (delta > 100)
  - `balance_daily_statistics_mv`: Daily balance statistics
  - `available_assets_view`: Simple view listing available assets

**Example Queries**:
```sql
-- Get current balance for an address
SELECT * FROM balances_current_view
WHERE address = '0x123...' AND asset = 'TOR';

-- Find significant balance changes
SELECT * FROM balance_significant_changes_view
WHERE address = '0x123...'
ORDER BY block_height DESC LIMIT 10;

-- Get daily balance statistics
SELECT * FROM balance_daily_statistics_mv
WHERE address = '0x123...' AND asset = 'TOR'
ORDER BY date DESC LIMIT 30;
```

**Balance Transfers Analysis**
- Tool: `balance_transfers_query`
- Purpose: Analyze individual transfer transactions between addresses
- **Database**: ClickHouse
- **Schema**: {balance_transfers_schema}
- **Core Table**:
  - `balance_transfers`: Stores individual transfer transactions between addresses
- **Available Views**:
  - **Basic Views**:
    - `balance_transfers_statistics_view`: Basic statistics by address and asset
    - `balance_transfers_daily_volume_mv`: Materialized view for daily transfer volume
    - `available_transfer_assets_view`: Simple view listing available assets
  
  - **Behavior Analysis**:
    - `balance_transfers_address_behavior_profiles_view`: Comprehensive behavioral analysis for each address
    - `balance_transfers_address_classification_view`: Classifies addresses into behavioral categories
    - `balance_transfers_suspicious_activity_view`: Identifies potentially suspicious activity patterns
  
  - **Relationship Analysis**:
    - `balance_transfers_address_relationships_view`: Tracks relationships between addresses
    - `balance_transfers_address_activity_patterns_view`: Analyzes temporal and behavioral patterns
  
  - **Network Analysis**:
    - `balance_transfers_network_flow_view`: High-level overview of network activity
    - `balance_transfers_periodic_activity_view`: Activity patterns over weekly time periods
    - `balance_transfers_seasonality_view`: Temporal patterns in transaction activity
  
  - **Economic Analysis**:
    - `balance_transfers_velocity_view`: Measures token circulation speed
    - `balance_transfers_liquidity_concentration_view`: Analyzes holding concentration
    - `balance_transfers_holding_time_view`: Analyzes token holding duration
  
  - **Anomaly Detection**:
    - `balance_transfers_basic_anomaly_view`: Detects unusual transaction patterns

**Example Queries**:
```sql
-- Get basic statistics for an address
SELECT * FROM balance_transfers_statistics_view
WHERE address = '0x123...' AND asset = 'TOR';

-- Find suspicious activity
SELECT * FROM balance_transfers_suspicious_activity_view
WHERE risk_level = 'High'
ORDER BY suspicion_score DESC LIMIT 10;

-- Analyze relationships between addresses
SELECT * FROM balance_transfers_address_relationships_view
WHERE from_address = '0x123...' OR to_address = '0x123...'
ORDER BY relationship_strength DESC;

-- Analyze token velocity
SELECT * FROM balance_transfers_velocity_view
WHERE asset = 'TOR'
ORDER BY month DESC LIMIT 12;
```

**ClickHouse Query Guidelines:**
- Use ClickHouse SQL dialect (not standard SQL)
- Available aggregation functions: `sum()`, `avg()`, `max()`, `min()`, `count()`
- Time functions: `toStartOfDay()`, `toStartOfMonth()`, etc.
- Array functions: `arrayJoin()`, `arrayElement()`, etc.
 
## 🎯 Success Metrics

A successful analysis should:
- Use actual schema information, not assumptions
- Provide accurate data based on real database structure
- Combine multiple data sources for comprehensive insights
- Handle errors gracefully and adjust queries accordingly

Remember: The schema information provided is authoritative - use it as your ground truth for database structure.
"""


mcp = FastMCP(name=f"{network.upper()} Chain Swarm MCP Server")


@mcp.tool(name="user_guide", description="Get user guide for the MCP server capabilities.",)
async def user_guide() -> str:
    """
    Get user guide for the MCP server capabilities.
    """
    return await get_user_guide()

@mcp.tool(name="instructions", description="Get instructions for using the MCP server tools by the AI assistant.")
async def instructions():
    """
    Get instructions for using the MCP server tools by the AI assistant.
    """
    return await get_instructions()


@session_rate_limit
@mcp.tool(
    name="money_flow_shortest_path",
    description="Find shortest paths between two addresses with optional asset filtering.",
    tags={"money flow", "shortest path", "path finding", "asset filtering"},
    annotations={
        "title": "Find shortest paths between addresses",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def money_flow_shortest_path(
    source_address: Annotated[str, Field(description="Source address to start the path from")],
    target_address: Annotated[str, Field(description="Target address to find path to")],
    assets: Annotated[Optional[str], Field(description="Optional comma-separated list of assets to filter by. ")] = None
) -> dict:
    """
    Find shortest paths between two addresses with optional asset filtering.
    
    Args:
        source_address: Source address to start the path from
        target_address: Target address to find path to
        assets: Optional list of assets to filter by
        
    Returns:
        dict: Path results containing nodes and edges
    """

    assets = assets if assets else ["all"]

    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    try:
        money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
        result = money_flow_tool.shortest_path(source_address, target_address, assets)
        return {"data": result}
    finally:
        memgraph_driver.close()
        neo4j_driver.close()


@session_rate_limit
@mcp.tool(
    name="money_flow_explore_address_connections",
    description="Explore address connections with depth and direction control.",
    tags={"money flow", "exploration", "depth traversal", "directional analysis"},
    annotations={
        "title": "Explore address connections with depth and direction control",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def money_flow_explore_address_connections(
    addresses: Annotated[List[str], Field(description="List of wallet addresses to start the exploration from")],
    depth_level: Annotated[int, Field(description="Number of hops to explore from the starting addresses", ge=1, le=5)],
    direction: Annotated[str, Field(description="Direction of relationships to follow: 'in', 'out', or 'all'")],
    assets: Annotated[Optional[str], Field(description="Optional comma-separated list of assets to filter by. ")] = None
) -> dict:
    """
    Explore address connections with depth and direction control.
    
    Args:
        addresses: List of wallet addresses to start the exploration from
        depth_level: Number of hops to explore (1-5)
        direction: Direction of relationships ('in', 'out', or 'all')
        assets: Optional list of assets to filter by
        
    Returns:
        dict: Exploration results containing nodes and edges
    """

    assets = assets.split(",") if assets else ["all"]

    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    try:
        money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
        # Convert string direction to enum
        from packages.api.tools.money_flow import Direction
        direction_enum = Direction(direction)
        result = money_flow_tool.explore_address_connections(addresses, depth_level, direction_enum, assets)
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
def execute_money_flow_query(query: Annotated[
    str, Field(description="The Cypher query to execute. Use asset properties to filter by specific assets.")]) -> dict:
    """
    Execute a money flow query on the specified blockchain network with asset support.

    Args:
        query (str): The Cypher query to execute. Use asset properties to filter by specific assets.

    Returns:
        dict: The result of the money flow query with asset information.
    """

    memgraph_driver = get_memgraph_driver(network)
    neo4j_driver = get_neo4j_driver(network)
    try:
        money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
        result = money_flow_tool.query(query)
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
async def execute_similarity_search_query(
        query: Annotated[dict, Field(description="The similarity search query parameters")]) -> dict:
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
    memgraph_driver = get_memgraph_driver(network)
    similarity_search_tool = SimilaritySearchTool(memgraph_driver)
    result = similarity_search_tool.similarity_search_query(query)
    similarity_search_tool.close()
    return result


@session_rate_limit
@mcp.tool(
    name="balance_tracking_query",
    description="Execute a balance tracking related query.",
    tags={"balance tracking", "balance changes", "balance changes delta", "balances",
          "known addresses", "assets"},
    annotations={
        "title": "Executes Clickhouse dialect SQL query against blockchain balance tracking database",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_balance_tracking_query(query: Annotated[str, Field(
    description="The Clickhouse dialect SQL query to execute against balance tracking tables/views.")]) -> dict:
    """
    Execute a balance tracking query on the specified blockchain network.

    Args:
        query (str): The SQL query to execute against balance tracking tables/views.

    Returns:
        dict: The result of the balance tracking query.
    """
    balance_tracking_service = BalanceTrackingTool(get_clickhouse_connection_string(network))
    result = await balance_tracking_service.balance_tracking_query(query)
    return result


@session_rate_limit
@mcp.tool(
    name="balance_transfers_query",
    description="Execute a balance transfers related query.",
    tags={"balance transfers", "transaction analysis", "address behavior", "relationship analysis",
          "network flow", "economic analysis", "anomaly detection"},
    annotations={
        "title": "Executes Clickhouse dialect SQL query against blockchain balance transfers database",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_balance_transfers_query(query: Annotated[str, Field(
    description="The Clickhouse dialect SQL query to execute against balance transfers tables/views.")]) -> dict:
    """
    Execute a balance transfers query on the specified blockchain network.

    Args:
        query (str): The SQL query to execute against balance transfers tables/views.

    Returns:
        dict: The result of the balance transfers query.
    """
    balance_transfers_service = BalanceTransfersTool(get_clickhouse_connection_string(network))
    result = await balance_transfers_service.balance_transfers_query(query)
    return result

 
if __name__ == "__main__":
    setup_logger("chain-insights-mcp-server")
    schema_response = asyncio.run(get_instructions())
    json_schema = json.dumps(schema_response, indent=2)
    logger.info(f"Schema loaded: {json_schema}")
    mcp.run(transport="sse", host="0.0.0.0", port=8005, log_level="debug")
