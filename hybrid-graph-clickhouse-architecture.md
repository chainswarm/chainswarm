# Hybrid Architecture: Lightweight Graph + ClickHouse Analytics

## Overview
Instead of storing detailed transaction data on graph edges, we use:
- **Graph (Memgraph)**: Lightweight network structure only
- **ClickHouse**: All transaction details, volumes, frequencies, and time-series data

## Architecture Design

### Graph Model (Simplified)
```cypher
// Address nodes - only network properties
(:Address {
    address: "0x123",
    // Network metrics only
    neighbor_count: 10,
    in_transaction_count: 50,
    out_transaction_count: 45,
    community_id: 5,
    pagerank: 0.0023,
    // Remove: volume_in, volume_out, timestamps
})

// Edges - minimal data
(:Address)-[:TRANSFERS {
    // Just enough to maintain network structure
    asset: "TOR",
    active: true,
    // Remove: volumes, frequencies, arrays
}]->(:Address)
```

### ClickHouse Schema
```sql
-- Detailed transfer table (existing balance_transfers enhanced)
CREATE TABLE IF NOT EXISTS transfers (
    -- Transaction identification
    tx_hash String,
    extrinsic_id String,
    event_idx String,
    
    -- Block information
    block_height UInt32,
    block_timestamp DateTime64(3),
    
    -- Transfer details
    from_address String,
    to_address String,
    asset String,
    amount Decimal128(18),
    fee Decimal128(18),
    
    -- Indexing
    INDEX idx_from_asset (from_address, asset) TYPE bloom_filter(0.01),
    INDEX idx_to_asset (to_address, asset) TYPE bloom_filter(0.01),
    INDEX idx_pair_asset (from_address, to_address, asset) TYPE bloom_filter(0.01)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_timestamp, from_address, to_address, asset);

-- Materialized View: Address volumes by asset
CREATE MATERIALIZED VIEW IF NOT EXISTS address_volumes_mv
ENGINE = AggregatingMergeTree()
ORDER BY (address, asset)
AS
SELECT
    address,
    asset,
    countIf(direction = 'out') as out_transfer_count,
    countIf(direction = 'in') as in_transfer_count,
    sumIf(amount, direction = 'out') as volume_out,
    sumIf(amount, direction = 'in') as volume_in,
    minIf(block_timestamp, direction = 'out') as first_out_transfer,
    maxIf(block_timestamp, direction = 'out') as last_out_transfer,
    minIf(block_timestamp, direction = 'in') as first_in_transfer,
    maxIf(block_timestamp, direction = 'in') as last_in_transfer
FROM (
    SELECT from_address as address, asset, amount, block_timestamp, 'out' as direction
    FROM transfers
    UNION ALL
    SELECT to_address as address, asset, amount, block_timestamp, 'in' as direction
    FROM transfers
)
GROUP BY address, asset;

-- Materialized View: Edge aggregates with frequencies
CREATE MATERIALIZED VIEW IF NOT EXISTS edge_aggregates_mv
ENGINE = AggregatingMergeTree()
ORDER BY (from_address, to_address, asset)
AS
SELECT
    from_address,
    to_address,
    asset,
    count() as transfer_count,
    sum(amount) as total_volume,
    min(block_timestamp) as first_transfer,
    max(block_timestamp) as last_transfer,
    avg(amount) as avg_amount,
    
    -- Frequency analysis
    groupArray(block_timestamp) as timestamps,
    arrayDifference(timestamps) as time_gaps,
    avg(arrayElement(time_gaps, 1)) as avg_time_gap_ms,
    stddevPop(arrayElement(time_gaps, 1)) as time_gap_stddev,
    
    -- Pattern detection
    CASE 
        WHEN time_gap_stddev < avg_time_gap_ms * 0.3 THEN 'regular'
        WHEN max(arrayElement(time_gaps, 1)) > avg_time_gap_ms * 5 THEN 'burst'
        ELSE 'irregular'
    END as transfer_pattern
FROM transfers
GROUP BY from_address, to_address, asset;

-- Materialized View: Recent transfer windows
CREATE MATERIALIZED VIEW IF NOT EXISTS recent_transfers_mv
ENGINE = ReplacingMergeTree()
ORDER BY (from_address, to_address, asset)
AS
SELECT
    from_address,
    to_address,
    asset,
    groupArray(block_timestamp) as recent_timestamps,
    groupArray(amount) as recent_amounts,
    groupArray(block_height) as recent_blocks
FROM (
    SELECT *
    FROM transfers
    WHERE block_timestamp > now() - INTERVAL 30 DAY
    ORDER BY block_timestamp DESC
    LIMIT 20 BY from_address, to_address, asset
)
GROUP BY from_address, to_address, asset;
```

## API Implementation

### Money Flow Service Updates

```python
class MoneyFlowService:
    def __init__(self, graph_database: Driver, clickhouse_client):
        self.graph_database = graph_database
        self.clickhouse = clickhouse_client
        
    def get_money_flow_by_path_explore(self, addresses, depth_level, direction, assets):
        # Step 1: Get network structure from graph
        paths = self._get_paths_from_graph(addresses, depth_level, direction)
        
        # Step 2: Extract unique addresses and edges
        unique_addresses = set()
        unique_edges = set()
        for path in paths:
            for node in path['nodes']:
                unique_addresses.add(node['address'])
            for edge in path['edges']:
                unique_edges.add((edge['from'], edge['to']))
        
        # Step 3: Enrich with ClickHouse data
        address_volumes = self._get_address_volumes_from_clickhouse(
            list(unique_addresses), assets
        )
        edge_details = self._get_edge_details_from_clickhouse(
            list(unique_edges), assets
        )
        
        # Step 4: Combine and return
        return self._combine_graph_and_analytics(
            paths, address_volumes, edge_details
        )
    
    def _get_address_volumes_from_clickhouse(self, addresses, assets):
        """Get volumes from materialized view"""
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_filter = f"AND asset IN {tuple(assets)}"
            
        query = f"""
        SELECT 
            address,
            asset,
            volume_in,
            volume_out,
            out_transfer_count,
            in_transfer_count,
            first_out_transfer,
            last_out_transfer,
            first_in_transfer,
            last_in_transfer
        FROM address_volumes_mv
        WHERE address IN {tuple(addresses)}
        {asset_filter}
        """
        
        result = self.clickhouse.query(query)
        
        # Group by address
        address_data = {}
        for row in result:
            addr = row['address']
            if addr not in address_data:
                address_data[addr] = []
            
            address_data[addr].append({
                'asset': row['asset'],
                'volume_in': row['volume_in'],
                'volume_out': row['volume_out'],
                'transfer_count_in': row['in_transfer_count'],
                'transfer_count_out': row['out_transfer_count'],
                'first_transfer': min(
                    row['first_in_transfer'] or datetime.max,
                    row['first_out_transfer'] or datetime.max
                ),
                'last_transfer': max(
                    row['last_in_transfer'] or datetime.min,
                    row['last_out_transfer'] or datetime.min
                )
            })
            
        return address_data
    
    def _get_edge_details_from_clickhouse(self, edges, assets):
        """Get edge details from materialized view"""
        edge_conditions = []
        for from_addr, to_addr in edges:
            edge_conditions.append(
                f"(from_address = '{from_addr}' AND to_address = '{to_addr}')"
            )
        
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_filter = f"AND asset IN {tuple(assets)}"
            
        query = f"""
        SELECT 
            from_address,
            to_address,
            asset,
            transfer_count,
            total_volume,
            avg_amount,
            first_transfer,
            last_transfer,
            avg_time_gap_ms,
            transfer_pattern
        FROM edge_aggregates_mv
        WHERE ({' OR '.join(edge_conditions)})
        {asset_filter}
        """
        
        result = self.clickhouse.query(query)
        
        # Group by edge
        edge_data = {}
        for row in result:
            edge_key = (row['from_address'], row['to_address'])
            if edge_key not in edge_data:
                edge_data[edge_key] = []
                
            edge_data[edge_key].append({
                'asset': row['asset'],
                'volume': row['total_volume'],
                'transfer_count': row['transfer_count'],
                'avg_amount': row['avg_amount'],
                'first_transfer': row['first_transfer'],
                'last_transfer': row['last_transfer'],
                'avg_frequency': row['avg_time_gap_ms'],
                'pattern': row['transfer_pattern']
            })
            
        return edge_data
```

### Frequency Analysis Queries

```python
def analyze_transfer_frequencies(self, address: str, asset: str = None):
    """Analyze transfer patterns using ClickHouse"""
    
    asset_filter = f"AND asset = '{asset}'" if asset else ""
    
    # Get detailed frequency analysis
    query = f"""
    WITH transfer_pairs AS (
        SELECT 
            from_address,
            to_address,
            asset,
            block_timestamp,
            amount,
            LAG(block_timestamp) OVER (
                PARTITION BY from_address, to_address, asset 
                ORDER BY block_timestamp
            ) as prev_timestamp
        FROM transfers
        WHERE (from_address = '{address}' OR to_address = '{address}')
        {asset_filter}
    )
    SELECT 
        from_address,
        to_address,
        asset,
        count() as transfer_count,
        avg(block_timestamp - prev_timestamp) as avg_gap_ms,
        stddevPop(block_timestamp - prev_timestamp) as gap_stddev,
        min(block_timestamp - prev_timestamp) as min_gap,
        max(block_timestamp - prev_timestamp) as max_gap,
        quantile(0.5)(block_timestamp - prev_timestamp) as median_gap,
        -- Detect patterns
        CASE
            WHEN stddevPop(block_timestamp - prev_timestamp) < 
                 avg(block_timestamp - prev_timestamp) * 0.3 THEN 'regular'
            WHEN max(block_timestamp - prev_timestamp) > 
                 avg(block_timestamp - prev_timestamp) * 5 THEN 'burst'
            ELSE 'irregular'
        END as pattern
    FROM transfer_pairs
    WHERE prev_timestamp IS NOT NULL
    GROUP BY from_address, to_address, asset
    ORDER BY transfer_count DESC
    """
    
    return self.clickhouse.query(query)
```

## Benefits of This Approach

### Advantages
1. **Scalability**: ClickHouse handles billions of transfers efficiently
2. **No Array Limits**: Materialized views aggregate without storing arrays
3. **Time-Series Native**: ClickHouse optimized for time-based queries
4. **Flexible Analysis**: Can create new views without changing graph
5. **Performance**: Graph stays lightweight for traversals
6. **Real-time Updates**: Materialized views update automatically

### Trade-offs
1. **Two Systems**: Need to query both graph and ClickHouse
2. **Sync Complexity**: Must keep graph edges in sync with transfers
3. **Query Complexity**: Some queries need to join data from both systems

## Migration Path

1. **Keep existing ClickHouse tables** (balance_transfers)
2. **Add new materialized views** for aggregations
3. **Simplify graph edges** to just maintain structure
4. **Update API** to query both systems
5. **Remove volume properties** from Address nodes

## Example Combined Query

```python
# Get money flow with full details
def get_enriched_money_flow(address: str):
    # 1. Get network structure from graph
    graph_query = """
    MATCH (a:Address {address: $address})-[:TRANSFERS]-(b:Address)
    RETURN a, b, 
           a.pagerank as source_pagerank,
           b.pagerank as target_pagerank,
           a.community_id = b.community_id as same_community
    """
    
    # 2. Get volumes from ClickHouse
    volume_query = """
    SELECT * FROM address_volumes_mv
    WHERE address = %(address)s
    """
    
    # 3. Get transfer patterns from ClickHouse
    pattern_query = """
    SELECT * FROM edge_aggregates_mv
    WHERE from_address = %(address)s 
       OR to_address = %(address)s
    """
    
    # Combine results
    return combine_results(graph_data, volume_data, pattern_data)
```

This hybrid approach gives us the best of both worlds:
- Graph for network analysis
- ClickHouse for time-series and aggregations
- No unbounded array growth
- Flexible materialized views for new metrics