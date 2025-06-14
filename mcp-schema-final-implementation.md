# MCP Schema Enhancement - Final Implementation

This document provides the exact code changes needed to implement the money flow query examples enhancement in the MCP server.

## 1. Replace `get_memgraph_cypher_cheatsheet()` Function

Replace the existing function (lines 55-146 in `mcp_server.py`) with this new implementation:

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

## 2. Uncomment Line 306

Change line 306 in `mcp_server.py` from:

```python
#money_flow_schema["memgraph_cypher_cheatsheet"] = get_memgraph_cypher_cheatsheet()
```

To:

```python
money_flow_schema["memgraph_cypher_cheatsheet"] = get_memgraph_cypher_cheatsheet()
```

## 3. Implementation Steps

1. Switch to Code mode using:
   ```
   <switch_mode>
   <mode_slug>code</mode_slug>
   <reason>Need to implement the code changes to enhance the MCP schema with money flow query examples</reason>
   </switch_mode>
   ```

2. In Code mode:
   - Replace the `get_memgraph_cypher_cheatsheet()` function
   - Uncomment line 306
   - Test the changes

## 4. Testing

After implementation, test the schema output by running:

```python
if __name__ == "__main__":
    schema_result = asyncio.run(_schema())
    import json
    schema_result = json.dumps(schema_result, indent=2)
    print("MCP Server Schema:")
    print(schema_result)
```

Verify that the money flow schema includes the new cypher cheatsheet with the examples from `money_flow_service.py`.