 k# MCP Schema Enhancement Plan

## Overview

This plan outlines enhancements to the MCP server to:
1. Add comprehensive network and asset information to schemas
2. Create a unified `schema` method that returns all information in one call
3. Include database-specific documentation for Memgraph and ClickHouse
4. Provide extensive examples and cheatsheets

## Key Changes

### 1. Unified Schema Method

Create a single `schema` MCP tool that consolidates:
- All network and asset information (making the `networks` tool obsolete)
- Money flow schema (Memgraph graph database)
- Balance tracking schema (ClickHouse OLAP database)
- Similarity search schema (Memgraph vector search)
- Database-specific syntax and optimization guides

### 2. Global Asset Support

Emphasize that asset support is implemented globally across all schemas:
- All tables in ClickHouse include asset fields
- All edges in Memgraph include asset properties
- Asset filtering is universally available

### 3. Database-Specific Documentation

#### Memgraph (Graph Database)
- Cypher dialect differences from Neo4j
- Path expansion with `path.expand()`
- Native BFS/DFS traversal support
- Triggers and streams for real-time processing
- Built-in graph algorithms
- Performance optimization techniques

#### ClickHouse (OLAP Database)
- MergeTree engine specifics and FINAL keyword
- PREWHERE optimization for filtering
- Advanced aggregation functions
- Time-series analysis capabilities
- Materialized views usage
- Array and JSON operations

## Implementation Details

### Asset Registry Structure

```python
ASSET_REGISTRY = {
    "torus": {
        "name": "Torus",
        "symbol": "TORUS",
        "native_asset": "TOR",
        "supported_assets": [
            {
                "symbol": "TOR",
                "name": "Torus",
                "type": "native",
                "description": "Native token of Torus network"
            }
        ]
    },
    "bittensor": {
        "name": "Bittensor", 
        "symbol": "BITTENSOR",
        "native_asset": "TAO",
        "supported_assets": [
            {
                "symbol": "TAO",
                "name": "Bittensor",
                "type": "native",
                "description": "Native token of Bittensor network"
            }
        ]
    },
    "polkadot": {
        "name": "Polkadot",
        "symbol": "POLKADOT",
        "native_asset": "DOT",
        "supported_assets": [
            {
                "symbol": "DOT",
                "name": "Polkadot",
                "type": "native",
                "description": "Native token of Polkadot network"
            }
        ]
    }
}
```

### Schema Method Response Structure

```json
{
  "networks": {
    // Complete network and asset information
  },
  "global_asset_support": {
    "description": "All schemas support multi-asset tracking. Asset filtering is available across all data models.",
    "note": "Asset support is implemented globally - all tables, nodes, and edges include asset information"
  },
  "schemas": {
    "money_flow": {
      "description": "Graph database schema for money flow analysis",
      "database": {
        "type": "Memgraph",
        "note": "Uses Memgraph-specific Cypher dialect"
      },
      "memgraph_cypher_cheatsheet": {
        // Extensive Memgraph examples
      }
    },
    "balance_tracking": {
      "description": "Time-series balance tracking schema",
      "database": {
        "type": "ClickHouse",
        "note": "OLAP database optimized for analytical queries"
      },
      "clickhouse_sql_cheatsheet": {
        // Extensive ClickHouse examples
      }
    },
    "similarity_search": {
      // Vector search schema
    }
  },
  "usage_examples": {
    // Practical examples for each capability
  }
}
```

## Memgraph-Specific Examples

### Path Expansion
```cypher
-- Basic expansion
MATCH (a:Address {address: $addr}) 
CALL path.expand(a, ['TO'], [], 1, 3) 
YIELD result 
RETURN result

-- Filtered expansion with asset
MATCH (a:Address {address: $addr}) 
CALL path.expand(a, ['TO'], [], 1, 5) 
YIELD result 
WHERE ALL(r IN relationships(result) WHERE r.asset = $asset) 
RETURN result
```

### BFS/DFS Traversal
```cypher
-- BFS with depth limit and filter
MATCH path = (a:Address {address: $addr})-[*BFS 1..3 (r, n | r.asset = $asset)]->(b:Address) 
RETURN path

-- Weighted shortest path
MATCH path = (a:Address {address: $from})-[*WSHORTEST 1..10 (r, n | r.volume) volume]->(b:Address {address: $to}) 
RETURN path, volume
```

### Real-time Processing
```cypher
-- Create trigger for large transfers
CREATE TRIGGER large_transfer 
ON CREATE EDGE TO 
EXECUTE IF NEW.volume > 1000000 
THEN CREATE (n:Alert {
  address: NEW.from_address, 
  amount: NEW.volume, 
  timestamp: timestamp()
})

-- Kafka stream
CREATE STREAM money_flow_stream 
TOPICS money_flow_events 
TRANSFORM money_flow.transform
```

### Graph Algorithms
```cypher
-- PageRank
CALL pagerank.pagerank() 
YIELD node, rank 
SET node.page_rank = rank

-- Community detection
CALL community_detection.louvain() 
YIELD node, community_id 
SET node.community = community_id
```

## ClickHouse-Specific Examples

### MergeTree Optimizations
```sql
-- FINAL for deduplication
SELECT * FROM balance_changes FINAL 
WHERE address = $addr AND asset = $asset 
ORDER BY timestamp DESC

-- PREWHERE optimization
SELECT * FROM balance_transfers 
PREWHERE asset = $asset 
WHERE from_address = $addr OR to_address = $addr

-- Partition pruning
SELECT * FROM balance_changes 
WHERE toYYYYMM(timestamp) = 202401 
AND address = $addr
```

### Advanced Aggregations
```sql
-- Moving averages
SELECT 
  timestamp, 
  asset, 
  amount,
  avg(amount) OVER (
    PARTITION BY asset 
    ORDER BY timestamp 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as moving_avg_7d
FROM balance_transfers FINAL

-- Quantiles
SELECT 
  asset,
  quantile(0.5)(amount) as median,
  quantile(0.95)(amount) as p95,
  quantile(0.99)(amount) as p99
FROM balance_transfers FINAL
GROUP BY asset
```

### Time-Series Analysis
```sql
-- Time buckets with gap filling
WITH (SELECT min(timestamp) FROM balance_changes) as min_time
SELECT 
  time_bucket,
  COALESCE(sum_amount, 0) as amount
FROM (
  SELECT 
    toStartOfHour(timestamp) as time_bucket,
    SUM(amount) as sum_amount
  FROM balance_changes FINAL
  WHERE asset = $asset
  GROUP BY time_bucket
) RIGHT JOIN (
  SELECT toStartOfHour(min_time + number * 3600) as time_bucket
  FROM numbers(24*30)
) USING time_bucket
ORDER BY time_bucket

-- Cumulative sums
SELECT 
  timestamp,
  address,
  asset,
  amount,
  SUM(amount) OVER (
    PARTITION BY address, asset 
    ORDER BY timestamp
  ) as cumulative_amount
FROM balance_transfers FINAL
```

### Performance Patterns
```sql
-- Sampling for estimates
SELECT 
  asset,
  COUNT(*) * 100 as estimated_count
FROM balance_transfers SAMPLE 0.01
GROUP BY asset

-- Efficient JOIN with known addresses
SELECT * 
FROM balance_transfers FINAL AS t
INNER JOIN known_addresses AS k 
  ON t.from_address = k.address
WHERE t.asset = $asset 
  AND k.label != ''
```

## Benefits

1. **Single Entry Point**: One call to get all schema information
2. **Database Awareness**: Clear documentation of database-specific features
3. **Practical Examples**: Extensive real-world query examples
4. **Performance Guidance**: Optimization tips for both databases
5. **Asset Support Clarity**: Global asset support is clearly documented

## Migration Notes

- The `networks` tool becomes deprecated in favor of the comprehensive `schema` method
- Existing schema methods remain functional but users are encouraged to use the unified `schema` method
- No breaking changes to existing functionality