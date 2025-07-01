import asyncio
import json
import time
from typing import List, Optional, Dict, Any
from typing import Annotated
from loguru import logger
from pydantic import Field
from fastmcp import FastMCP, Context
from packages.api.routers import get_memgraph_driver, get_neo4j_driver
from packages.api.tools.balance_series import BalanceSeriesAnalyticsTool
from packages.api.tools.balance_transfers import BalanceTransfersTool
from packages.api.tools.money_flow import MoneyFlowTool
from packages.api.tools.similarity_search import SimilaritySearchTool
from packages.indexers.base import (
    get_clickhouse_connection_string, setup_metrics, get_metrics_registry,
    setup_enhanced_logger, ErrorContextManager, log_service_start, log_service_stop, classify_error
)
from packages.indexers.substrate import get_network_asset
from packages.api.middleware.mcp_session_rate_limiting import (
    session_rate_limit,
)
import os
import clickhouse_connect

network = os.getenv("NETWORK", "torus").lower()


# MCP Metrics class
class MCPMetrics:
    """Metrics collection for MCP server operations"""
    
    def __init__(self, metrics_registry, network: str):
        self.registry = metrics_registry
        self.network = network
        
        if metrics_registry:
            self._init_metrics()
        # REMOVED: Warning log - metrics handle availability
    
    def _init_metrics(self):
        """Initialize MCP-specific metrics"""
        # Tool usage metrics
        self.mcp_tool_calls_total = self.registry.create_counter(
            'mcp_tool_calls_total',
            'Total MCP tool calls',
            ['tool', 'network']
        )
        
        self.mcp_tool_duration = self.registry.create_histogram(
            'mcp_tool_duration_seconds',
            'MCP tool execution duration',
            ['tool', 'network'],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, float('inf'))
        )
        
        self.mcp_tool_errors_total = self.registry.create_counter(
            'mcp_tool_errors_total',
            'Total MCP tool errors',
            ['tool', 'network', 'error_type']
        )
        
        self.mcp_tool_success_rate = self.registry.create_gauge(
            'mcp_tool_success_rate',
            'MCP tool success rate',
            ['tool', 'network']
        )
        
        # Database operation metrics
        self.mcp_database_operations_total = self.registry.create_counter(
            'mcp_database_operations_total',
            'Total MCP database operations',
            ['network', 'database', 'operation']
        )
        
        self.mcp_database_query_duration = self.registry.create_histogram(
            'mcp_database_query_duration_seconds',
            'MCP database query duration',
            ['network', 'database'],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float('inf'))
        )
        
        # Session metrics
        self.mcp_active_sessions = self.registry.create_gauge(
            'mcp_active_sessions',
            'Number of active MCP sessions',
            ['network']
        )
        
        self.mcp_sessions_created_total = self.registry.create_counter(
            'mcp_sessions_created_total',
            'Total MCP sessions created',
            ['network']
        )
        
        self.mcp_session_rate_limit_hits_total = self.registry.create_counter(
            'mcp_session_rate_limit_hits_total',
            'Total MCP session rate limit hits',
            ['network']
        )
    
    def record_tool_call(self, tool_name: str, duration: float, success: bool = True, error_type: str = None):
        """Record a tool call"""
        if not self.registry:
            return
            
        self.mcp_tool_calls_total.labels(tool=tool_name, network=self.network).inc()
        self.mcp_tool_duration.labels(tool=tool_name, network=self.network).observe(duration)
        
        if not success and error_type:
            self.mcp_tool_errors_total.labels(
                tool=tool_name, network=self.network, error_type=error_type
            ).inc()
    
    def record_database_operation(self, database: str, operation: str, duration: float):
        """Record a database operation"""
        if not self.registry:
            return
            
        self.mcp_database_operations_total.labels(
            network=self.network, database=database, operation=operation
        ).inc()
        self.mcp_database_query_duration.labels(
            network=self.network, database=database
        ).observe(duration)
    
    def record_session_created(self):
        """Record a new session"""
        if not self.registry:
            return
        self.mcp_sessions_created_total.labels(network=self.network).inc()
    
    def record_rate_limit_hit(self):
        """Record a rate limit hit"""
        if not self.registry:
            return
        self.mcp_session_rate_limit_hits_total.labels(network=self.network).inc()
    
    def update_active_sessions(self, count: int):
        """Update active sessions count"""
        if not self.registry:
            return
        self.mcp_active_sessions.labels(network=self.network).set(count)

# Initialize enhanced logging and metrics
service_name = f"{network}-mcp-server"
setup_enhanced_logger(service_name)
error_ctx = ErrorContextManager(service_name)
metrics_registry = setup_metrics(service_name, start_server=True)
mcp_metrics = MCPMetrics(metrics_registry, network)

# Log service startup
log_service_start(
    service_name,
    network=network,
    port=8005,
    transport="sse"
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
        # ENHANCED: Error logging with context
        error_ctx.log_error(
            "Failed to query available assets from ClickHouse",
            error=e,
            operation="get_assets_from_clickhouse",
            network=network,
            fallback_asset=get_network_asset(network),
            error_category=classify_error(e)
        )
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
    - **Balance Series**: Historical balance tracking with 4-hour interval snapshots
    - **Balance Transfers**: Detailed transaction analysis and address behavior profiling
    - **Known Addresses**: Database of labeled addresses (exchanges, treasuries, bridges, etc.)
    
    
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
    
    ## 📊 Balance Series Analytics
    
    **What it does**: Tracks account balance changes over time with fixed 4-hour interval snapshots, supporting multiple balance types (free, reserved, staked, total).
    
    **You can ask about**:
    - Historical balance snapshots at different time scales
    - Balance change trends and volatility
    - Multi-level time aggregation (daily, weekly, monthly)
    - Balance composition (free, reserved, staked)
    
    **Example questions**:
    - "What's the current balance for [ADDRESS]?"
    - "Show me the balance history for [ADDRESS] over the last month"
    - "How has the staked balance for [ADDRESS] changed weekly?"
    - "What was the total balance for [ADDRESS] at the end of last month?"
    - "Which addresses had the largest balance increases last week?"
    - "Plot the balance trend for [ADDRESS] over the past quarter"
    
    ## 💸 Balance Transfers Analysis
    
    **What it does**: Tracks individual transfer transactions between addresses with comprehensive metrics for network activity, address behavior, and economic indicators.
    
    **You can ask about**:
    - Transaction history with detailed records
    - Address behavior patterns and classifications
    - Network activity metrics and trends
    - Transaction size distribution and patterns
    - Economic indicators like token velocity
    - Temporal patterns in transaction activity
    
    **Example questions**:
    - "Show me all transactions for [ADDRESS]"
    - "What's the transaction volume trend for [ASSET] over the last quarter?"
    - "Identify addresses with high transaction frequency but low volume"
    - "What's the distribution of transaction sizes for [ASSET]?"
    - "Show me addresses classified as 'whales' for [ASSET]"
    - "Analyze the transaction relationship between [ADDRESS1] and [ADDRESS2]"
    - "What's the token velocity for [ASSET] over time?"
    
    ## 🏷️ Known Addresses
    
    **What it does**: Maintains a database of labeled addresses for contextual analysis.
    
    **You can ask about**:
    - Well-known addresses and their purposes
    - Addresses by category (exchanges, treasuries, etc.)
    - Entity identification for unknown addresses
    
    **Example questions**:
    - "What are the well-known addresses on this blockchain?"
    - "List all exchange addresses"
    - "Find all treasury and DAO addresses"
    - "Is [ADDRESS] a known entity?"
    
    **What it does**: Maintains a database of labeled addresses for contextual analysis.
    
    **You can ask about**:
    - Well-known addresses and their purposes
    - Addresses by category (exchanges, treasuries, etc.)
    - Entity identification for unknown addresses
    
    **Example questions**:
    - "What are the well-known addresses on this blockchain?"
    - "List all exchange addresses"
    - "Find all treasury and DAO addresses"
    - "Is [ADDRESS] a known entity?"
    
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
    - "Analyze the complete profile of [ADDRESS] including balance history, transactions, and connections"
    - "Map the ecosystem around [KNOWN_ENTITY] showing all related addresses and transaction patterns"
    - "Track the flow of [AMOUNT] tokens from [SOURCE] and analyze where they went"
    - "Compare transaction patterns between [ADDRESS1] and [ADDRESS2] over time"
    - "Identify potential wash trading by finding circular transaction patterns"
    
    ## 🎯 Pro Tips
    
    1. **Start Broad**: Ask general questions first, then drill down into specifics
    2. **Use Address Labels**: Ask about "exchanges", "bridges", "treasuries" to find known entities
    3. **Combine Approaches**: Use flow analysis + balance history + similarity for complete pictures
    4. **Time-Based Analysis**: Specify time ranges for more focused results (daily, weekly, monthly)
    5. **Asset Filtering**: Specify assets of interest for more relevant results
    6. **Transaction Size Bins**: Use standardized size categories (<0.1, 0.1-1, 1-10, 10-100, etc.)
    7. **Address Classification**: Look for address types like "Exchange", "Whale", "High_Volume_Trader"
    
    ## ⚡ Quick Reference
    
    **Most Popular Queries**:
    - "What are the well-known addresses?" (Great starting point)
    - "Show me the most active addresses" (Find network hubs)
    - "Trace [ADDRESS] connections" (Explore around specific address)
    - "Show balance history for [ADDRESS]" (Track balance changes)
    - "Analyze transaction patterns for [ADDRESS]" (Behavioral analysis)
    - "Find addresses similar to [ADDRESS]" (Pattern matching)
    
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

    balance_series_tool = BalanceSeriesAnalyticsTool(get_clickhouse_connection_string(network))
    balance_series_schema = await balance_series_tool.schema()
    
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

### 📊 Balance Series Analytics

**Balance Series Query**
- Tool: `balance_series_query`
- Purpose: Analyze balance snapshots over time with fixed 4-hour intervals
- **Database**: ClickHouse
- **Schema**: {balance_series_schema}

**Core Table**:
- `balance_series`: Stores balance snapshots at fixed 4-hour intervals with the following key fields:
  - `period_start_timestamp`, `period_end_timestamp`: Define the 4-hour interval (Unix timestamps in milliseconds)
  - `block_height`: Block height at the end of the period
  - `address`: Account address being tracked
  - `asset`: Token or currency being tracked
  - `free_balance`, `reserved_balance`, `staked_balance`, `total_balance`: Different balance types
  - `free_balance_change`, `reserved_balance_change`, `staked_balance_change`, `total_balance_change`: Absolute change since previous period
  - `total_balance_percent_change`: Percentage change in total balance

**Available Views**:
- `balance_series_latest_view`: Latest balance snapshot for each address and asset
- `balance_series_daily_view`: Daily balance aggregations with end-of-day balances and daily changes
- `balance_series_weekly_view`: Weekly balance statistics with end-of-week balances and weekly changes
- `balance_series_monthly_view`: Monthly balance statistics with end-of-month balances and monthly changes

**Example Queries**:
```sql
-- Get current balance for an address
SELECT * FROM balance_series_latest_view
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
ORDER BY asset;

-- Get daily balance history for an address and asset
SELECT date, end_of_day_total_balance, daily_total_balance_change
FROM balance_series_daily_view
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
  AND asset = 'DOT'
ORDER BY date DESC;

-- Analyze monthly balance trends
SELECT month_start,
       end_of_month_total_balance,
       monthly_total_balance_change
FROM balance_series_monthly_view
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
  AND asset = 'DOT'
ORDER BY month_start DESC;

-- Find addresses with significant balance increases
SELECT address, asset, total_balance_change, total_balance_percent_change
FROM balance_series
WHERE period_start_timestamp >= toUnixTimestamp64Milli(toDateTime('2023-01-01 00:00:00'))
  AND total_balance_percent_change > 10
ORDER BY total_balance_percent_change DESC
LIMIT 20;

-- Compare free vs staked balance composition
SELECT
    address,
    asset,
    free_balance,
    staked_balance,
    reserved_balance,
    total_balance,
    free_balance / total_balance * 100 AS free_percentage,
    staked_balance / total_balance * 100 AS staked_percentage,
    reserved_balance / total_balance * 100 AS reserved_percentage
FROM balance_series_latest_view
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
  AND total_balance > 0;
```

### 💸 Balance Transfers Analysis

**Balance Transfers Query**
- Tool: `balance_transfers_query`
- Purpose: Analyze individual transfer transactions between addresses
- **Database**: ClickHouse
- **Schema**: {balance_transfers_schema}

**Core Table**:
- `balance_transfers`: Stores individual transfer transactions with the following key fields:
  - `extrinsic_id`, `event_idx`: Uniquely identify a transaction
  - `block_height`, `block_timestamp`: Blockchain location and time information
  - `from_address`, `to_address`: Transaction participants
  - `asset`: Token or currency being transferred
  - `amount`: Value transferred
  - `fee`: Transaction cost

**Key View Categories**:

1. **Network Analytics Views** (Daily, Weekly, Monthly):
   - `balance_transfers_network_daily_view`
   - `balance_transfers_network_weekly_view`
   - `balance_transfers_network_monthly_view`
   
   These views provide consistent metrics across different time scales, including transaction counts, volumes, unique participants, network density, fee statistics, and transaction size distribution.

2. **Address Analytics View**:
   - `balance_transfers_address_analytics_view`
   
   Provides comprehensive metrics for each address, including transaction counts, volume metrics, temporal patterns, transaction size distribution, and address classification.

3. **Address Time-Series Views** (Daily, Weekly, Monthly):
   - `balance_transfers_address_daily_view`
   - `balance_transfers_address_weekly_view`
   - `balance_transfers_address_monthly_view`
   
   These views provide address-level volume metrics over time, showing volume in/out, transaction counts, and fees paid for each address and asset combination. Built on efficient materialized views (`balance_transfers_address_daily_internal`, `balance_transfers_address_weekly_internal`, `balance_transfers_address_monthly_internal`) for fast time-based analytics.

4. **Volume Aggregation Views** (Daily, Weekly, Monthly):
   - `balance_transfers_volume_daily_view`
   - `balance_transfers_volume_weekly_view`
   - `balance_transfers_volume_monthly_view`
   
   These views aggregate transaction volumes at different time scales with detailed metrics.

5. **Analysis Views**:
   - `balance_transfers_volume_trends_view`: Calculates rolling averages for trend analysis

**Transaction Size Histogram Bins**:
Balance transfers uses standardized bins for consistent analysis across different assets:
- < 0.1
- 0.1 to < 1
- 1 to < 10
- 10 to < 100
- 100 to < 1,000
- 1,000 to < 10,000
- ≥ 10,000

**Address Classification**:
The system automatically classifies addresses into behavioral categories:
- `Exchange`: High volume (≥100,000) with many recipients (≥100)
- `Whale`: High volume (≥100,000) with few recipients (<10)
- `High_Volume_Trader`: Significant volume (≥10,000) with many transactions (≥1,000)
- `Hub_Address`: Many connections (≥50 recipients and ≥50 senders)
- `Retail_Active`: Many transactions (≥100) but lower volume (<1,000)
- `Whale_Inactive`: Few transactions (<10) but high volume (≥10,000)
- `Retail_Inactive`: Few transactions (<10) and low volume (<100)
- `Regular_User`: Default classification for other addresses

**Example Queries**:
```sql
-- Get basic transaction history for an address
SELECT
    block_timestamp,
    block_height,
    from_address,
    to_address,
    asset,
    amount,
    fee
FROM balance_transfers
WHERE from_address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
   OR to_address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
ORDER BY block_timestamp DESC
LIMIT 50;

-- Analyze address behavior profile
SELECT * FROM balance_transfers_address_analytics_view
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
  AND asset = 'DOT';

-- Find potential exchange addresses
SELECT address, total_volume, unique_recipients, unique_senders, address_type
FROM balance_transfers_address_analytics_view
WHERE address_type = 'Exchange'
ORDER BY total_volume DESC
LIMIT 10;

-- Analyze network activity trends
SELECT
    period,
    asset,
    transaction_count,
    total_volume,
    unique_addresses,
    avg_transaction_size,
    avg_network_density
FROM balance_transfers_network_daily_view
WHERE asset = 'DOT'
  AND period >= toDate('2023-01-01')
ORDER BY period DESC
LIMIT 30;

-- Analyze transaction size distribution
SELECT
    asset,
    sum(tx_count_lt_01) as tx_count_lt_01,
    sum(tx_count_01_to_1) as tx_count_01_to_1,
    sum(tx_count_1_to_10) as tx_count_1_to_10,
    sum(tx_count_10_to_100) as tx_count_10_to_100,
    sum(tx_count_100_to_1k) as tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as tx_count_gte_10k
FROM balance_transfers_network_monthly_view
WHERE period = toStartOfMonth(toDate('2023-01-01'))
GROUP BY asset;

-- Analyze volume trends with rolling averages
SELECT
    period_start,
    asset,
    total_volume,
    rolling_7_period_avg_volume,
    rolling_30_period_avg_volume
FROM balance_transfers_volume_trends_view
WHERE asset = 'DOT'
ORDER BY period_start DESC
LIMIT 30;

-- Get address-level daily volume metrics
SELECT
    address,
    asset,
    date,
    volume_in,
    volume_out,
    net_volume,
    total_transactions,
    fees_paid
FROM balance_transfers_address_daily_view
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
  AND asset = 'DOT'
ORDER BY date DESC
LIMIT 30;

-- Analyze weekly volume patterns for multiple addresses
SELECT
    address,
    asset,
    week_start,
    volume_in,
    volume_out,
    net_volume,
    total_transactions
FROM balance_transfers_address_weekly_view
WHERE address IN ('5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY', '5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty')
  AND asset = 'DOT'
  AND week_start >= toStartOfWeek(today() - 90)
ORDER BY address, week_start DESC;

-- Find addresses with highest monthly volume growth
SELECT
    address,
    asset,
    month_start,
    volume_in,
    volume_out,
    net_volume,
    total_transactions
FROM balance_transfers_address_monthly_view
WHERE asset = 'DOT'
  AND month_start = toStartOfMonth(today() - 30)
  AND net_volume > 0
ORDER BY net_volume DESC
LIMIT 20;
```

**ClickHouse Query Guidelines:**
- Use ClickHouse SQL dialect (not standard SQL)
- Available aggregation functions: `sum()`, `avg()`, `max()`, `min()`, `count()`, `quantile()`
- Time functions: `toStartOfDay()`, `toStartOfMonth()`, `toStartOfWeek()`, `toDate()`, `toDateTime()`
- Timestamp conversion: `fromUnixTimestamp64Milli()`, `toUnixTimestamp64Milli()`
- Conditional aggregation: `sumIf()`, `countIf()`, `avgIf()`
- Statistical functions: `stddevPop()`, `varPop()`, `skewPop()`, `kurtPop()`
 
## 🎯 Success Metrics

A successful analysis should:
- Use actual schema information, not assumptions
- Provide accurate data based on real database structure
- Combine multiple data sources for comprehensive insights
- Handle errors gracefully and adjust queries accordingly
- Leverage appropriate time aggregation levels (4-hour, daily, weekly, monthly)
- Use standardized transaction size bins for consistent analysis
- Consider address classifications for behavioral analysis

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

    start_time = time.time()
    tool_name = "money_flow_shortest_path"
    
    try:
        assets = assets if assets else ["all"]

        memgraph_driver = get_memgraph_driver(network)
        neo4j_driver = get_neo4j_driver(network)
        try:
            money_flow_tool = MoneyFlowTool(memgraph_driver, neo4j_driver)
            result = money_flow_tool.shortest_path(source_address, target_address, assets)
            
            # Record successful tool call
            duration = time.time() - start_time
            mcp_metrics.record_tool_call(tool_name, duration, True)
            mcp_metrics.record_database_operation("memgraph", "shortest_path", duration)
            
            return {"data": result}
        finally:
            memgraph_driver.close()
            neo4j_driver.close()
            
    except Exception as e:
        # Record failed tool call
        duration = time.time() - start_time
        mcp_metrics.record_tool_call(tool_name, duration, False, "execution_error")
        
        # ENHANCED: Error logging with context
        error_ctx.log_error(
            f"MCP tool execution failed: {tool_name}",
            error=e,
            operation="mcp_tool_execution",
            tool_name=tool_name,
            source_address=source_address,
            target_address=target_address,
            assets=assets,
            duration=duration,
            error_category=classify_error(e)
        )
        raise

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
    name="balance_series_query",
    description="Execute a balance series related query.",
    tags={"balance series", "balance changes", "balance changes delta", "balances",
          "known addresses", "assets"},
    annotations={
        "title": "Executes Clickhouse dialect SQL query against blockchain balance series database",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def execute_balance_series_query(query: Annotated[str, Field(
    description="The Clickhouse dialect SQL query to execute against balance series tables/views.")]) -> dict:
    """
    Execute a balance series query on the specified blockchain network.

    Args:
        query (str): The SQL query to execute against balance series tables/views.

    Returns:
        dict: The result of the balance series query.
    """

    start_time = time.time()
    tool_name = "balance_series_query"
    
    try:
        balance_series_service = BalanceSeriesAnalyticsTool(get_clickhouse_connection_string(network))
        result = await balance_series_service.balance_series_query(query)
        
        # Record successful tool call
        duration = time.time() - start_time
        mcp_metrics.record_tool_call(tool_name, duration, True)
        mcp_metrics.record_database_operation("clickhouse", "balance_series_query", duration)
        
        return result
        
    except Exception as e:
        # Record failed tool call
        duration = time.time() - start_time
        mcp_metrics.record_tool_call(tool_name, duration, False, "query_error")
        
        # ENHANCED: Error logging with context
        error_ctx.log_error(
            f"MCP tool query failed: {tool_name}",
            error=e,
            operation="mcp_tool_query",
            tool_name=tool_name,
            query_preview=query[:200] if len(query) > 200 else query,
            duration=duration,
            error_category=classify_error(e)
        )
        raise


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

    start_time = time.time()
    tool_name = "balance_transfers_query"
    
    try:
        balance_transfers_service = BalanceTransfersTool(get_clickhouse_connection_string(network))
        result = await balance_transfers_service.balance_transfers_query(query)
        
        # Record successful tool call
        duration = time.time() - start_time
        mcp_metrics.record_tool_call(tool_name, duration, True)
        mcp_metrics.record_database_operation("clickhouse", "balance_transfers_query", duration)
        
        return result
        
    except Exception as e:
        # Record failed tool call
        duration = time.time() - start_time
        mcp_metrics.record_tool_call(tool_name, duration, False, "query_error")
        
        # ENHANCED: Error logging with context
        error_ctx.log_error(
            f"MCP tool query failed: {tool_name}",
            error=e,
            operation="mcp_tool_query",
            tool_name=tool_name,
            query_preview=query[:200] if len(query) > 200 else query,
            duration=duration,
            error_category=classify_error(e)
        )
        raise

 
if __name__ == "__main__":
    try:
        # REMOVED: Verbose schema logging - not needed
        schema_response = asyncio.run(get_instructions())
        
        # Log service ready
        error_ctx.log_service_lifecycle(
            "service_ready",
            host="0.0.0.0",
            port=8005,
            transport="sse"
        )
        
        mcp.run(transport="sse", host="0.0.0.0", port=8005, log_level="info")
        
    except KeyboardInterrupt:
        log_service_stop(service_name, reason="keyboard_interrupt")
    except Exception as e:
        error_ctx.log_error(
            "Fatal MCP server error",
            error=e,
            operation="mcp_server_startup",
            error_category=classify_error(e)
        )
        raise
    finally:
        log_service_stop(service_name, reason="shutdown")
