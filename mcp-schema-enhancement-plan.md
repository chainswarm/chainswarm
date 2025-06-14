# Money Flow Query Examples Enhancement Plan

## Overview

This document outlines the plan for enhancing the MCP server by adding comprehensive query examples to the money flow section of the schema. These examples will be derived from the `money_flow_service.py` file, which contains sophisticated query patterns that can serve as excellent real-world examples.

## Current Implementation

In `mcp_server.py`, there's a commented line that would add Cypher query examples to the money flow schema:

```python
# Line 306 in mcp_server.py
#money_flow_schema["memgraph_cypher_cheatsheet"] = get_memgraph_cypher_cheatsheet()
```

The current `get_memgraph_cypher_cheatsheet()` function (lines 55-146) contains examples organized into categories like path expansion, traversal methods, result limits, etc.

## Required Changes

1. Replace the content of the `get_memgraph_cypher_cheatsheet()` function with new examples derived from `money_flow_service.py`
2. Uncomment line 306 to include the cheatsheet in the schema

## New Cypher Cheatsheet Structure

The new cheatsheet will be organized into the following categories:

1. **Path Analysis Queries**: For exploring connections between addresses
2. **Community Analysis Queries**: For analyzing community structures and relationships
3. **Pattern Detection Queries**: For detecting specific transaction patterns
4. **Temporal Analysis Queries**: For time-based analysis of transaction patterns
5. **Performance Optimization**: Best practices for efficient queries

## Detailed Implementation

### New `get_memgraph_cypher_cheatsheet()` Function

```python
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
```

### Line to Uncomment

```python
# Line 306 in mcp_server.py - Remove the # at the beginning
money_flow_schema["memgraph_cypher_cheatsheet"] = get_memgraph_cypher_cheatsheet()
```

## Implementation Steps

1. Switch to Code mode to make the changes
2. Replace the existing `get_memgraph_cypher_cheatsheet()` function with the new implementation
3. Uncomment line 306 to include the cheatsheet in the schema
4. Test the changes by running the MCP server

## Benefits

The enhanced schema with these query examples will:

1. Provide users with real-world, practical query patterns
2. Showcase the advanced capabilities of the money flow graph database
3. Help users understand how to construct complex queries for various analysis scenarios
4. Demonstrate best practices for query optimization and performance

## Next Steps

After implementing these changes, we should:

1. Test the schema output to ensure it includes the new examples
2. Consider adding more examples as new query patterns are developed
3. Document the available query patterns for users