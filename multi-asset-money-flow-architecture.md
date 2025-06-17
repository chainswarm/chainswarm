# Multi-Asset Money Flow Architecture

## Problem Statement
The current money flow system aggregates volumes from different assets (TOR, TAO, DOT) into single properties on Address nodes, making volume-based metrics meaningless when low-value assets are summed with high-value assets. This affects embeddings and similarity search accuracy.

## Solution: Edge-Based Multi-Asset Model

### Core Design Principles
1. **No new node types** - Keep the graph model simple
2. **Address nodes** store only asset-agnostic properties
3. **TO edges** store all asset-specific data
4. **Network analysis** (PageRank, communities) based on connections, not volumes
5. **Embeddings** use ratios and normalized values for asset-agnostic comparisons

### Graph Structure

#### Address Node Properties
```cypher
(:Address {
    // Identity
    address: "0x123...",
    
    // Temporal properties
    first_seen_timestamp: 1234567890,
    last_seen_timestamp: 1234567890,
    first_seen_block_height: 100,
    last_seen_block_height: 500,
    
    // Network metrics (asset-agnostic)
    neighbor_count: 10,        // Unique addresses connected to
    in_transaction_count: 50,  // Total incoming transactions (all assets)
    out_transaction_count: 45, // Total outgoing transactions (all assets)
    
    // Labels
    labels: ["validator", "genesis"],
    
    // Network analysis results
    community_id: 5,           // Based on connection structure
    pagerank: 0.0023,         // Based on transaction activity
    
    // Embeddings
    financial_embedding: [...],  // 6 dimensions
    temporal_embedding: [...],   // 4 dimensions
    network_embedding: [...],    // 4 dimensions
    joint_embedding: [...]       // 14 dimensions
})
```

#### TO Edge Properties
```cypher
(:Address)-[:TO {
    // Identity
    from: "addr1",
    to: "addr2",
    asset: "TOR",              // Asset identifier
    
    // Volume metrics
    volume: 1000.5,            // Total volume transferred
    transfer_count: 5,         // Number of transfers
    min_amount: 100,
    max_amount: 300,
    avg_amount: 200.1,
    
    // Temporal properties
    first_transfer_timestamp: 1234567890,
    last_transfer_timestamp: 1234567890,
    first_transfer_block_height: 100,
    last_transfer_block_height: 500
}]->(:Address)
```

### Implementation Changes

#### 1. Indexer Modifications

Remove volume tracking from Address nodes and move to edges:

```python
def _process_transfer_events(self, transaction, timestamp, events):
    """Process transfer events with edge-based volume tracking"""
    for event in events:
        attrs = event['attributes']
        amount = float(convert_to_decimal_units(attrs['amount'], self.network))
        
        query = """
        // Create/update addresses without volume properties
        MERGE (sender:Address { address: $from })
        ON CREATE SET
            sender.first_seen_timestamp = $timestamp,
            sender.first_seen_block_height = $block_height,
            sender.neighbor_count = 0,
            sender.in_transaction_count = 0,
            sender.out_transaction_count = 0
            
        MERGE (receiver:Address { address: $to })
        ON CREATE SET
            receiver.first_seen_timestamp = $timestamp,
            receiver.first_seen_block_height = $block_height,
            receiver.neighbor_count = 0,
            receiver.in_transaction_count = 0,
            receiver.out_transaction_count = 0
        
        // Create/update edge with asset-specific volume
        MERGE (sender)-[r:TO { 
            from: $from,
            to: $to,
            asset: $asset 
        }]->(receiver)
        ON CREATE SET
            r.volume = $amount,
            r.transfer_count = 1,
            r.min_amount = $amount,
            r.max_amount = $amount,
            r.first_transfer_timestamp = $timestamp,
            r.last_transfer_timestamp = $timestamp,
            sender.neighbor_count = sender.neighbor_count + 1,
            receiver.neighbor_count = receiver.neighbor_count + 1
        ON MATCH SET
            r.volume = r.volume + $amount,
            r.transfer_count = r.transfer_count + 1,
            r.min_amount = CASE WHEN $amount < r.min_amount THEN $amount ELSE r.min_amount END,
            r.max_amount = CASE WHEN $amount > r.max_amount THEN $amount ELSE r.max_amount END,
            r.last_transfer_timestamp = $timestamp
            
        // Update transaction counts
        SET sender.out_transaction_count = sender.out_transaction_count + 1,
            receiver.in_transaction_count = receiver.in_transaction_count + 1
        """
```

#### 2. Community Detection (Connection-Based)

Communities are detected based on network structure, not volumes:

```python
def community_detection(self):
    """Run community detection based on connections"""
    query = """
    // Build unweighted connection graph
    MATCH (source:Address)-[:TO]->(target:Address)
    WITH DISTINCT source, target
    WITH collect(DISTINCT source) + collect(DISTINCT target) AS nodes,
         collect(DISTINCT {source: source, target: target}) AS edges
    
    CALL leiden_community_detection.get_subgraph(nodes, edges)
    YIELD node, community_id
    SET node.community_id = community_id
    """
```

#### 3. PageRank (Activity-Based)

PageRank is calculated based on transaction frequency, not wealth:

```python
def page_rank_with_community(self):
    """Calculate PageRank based on transaction count"""
    query = """
    MATCH (a1:Address {community_id: $community_id})-[r:TO]->(a2:Address)
    // Weight by transaction count across all assets
    WITH a1, a2, sum(r.transfer_count) as weight
    WITH collect({source: a1, target: a2, weight: weight}) as weighted_edges
    
    CALL pagerank.get_subgraph(weighted_edges) 
    YIELD node, rank
    SET node.pagerank = rank
    """
```

#### 4. Embeddings (Hybrid Approach)

Embeddings combine address properties with edge patterns:

```python
def update_embeddings(self):
    """Calculate embeddings from both address and edge properties"""
    
    query = """
    MATCH (a:Address)
    
    // Aggregate patterns from edges
    OPTIONAL MATCH (a)-[out:TO]->()
    WITH a, 
         sum(out.volume) as total_out_volume,
         sum(out.transfer_count) as total_out_transfers,
         count(DISTINCT out.asset) as out_asset_count
         
    OPTIONAL MATCH ()-[in:TO]->(a)
    WITH a, total_out_volume, total_out_transfers, out_asset_count,
         sum(in.volume) as total_in_volume,
         sum(in.transfer_count) as total_in_transfers,
         count(DISTINCT in.asset) as in_asset_count
    
    SET a.financial_embedding = [
        // Volume patterns (from edges)
        CASE WHEN total_in_volume + total_out_volume > 0 
             THEN total_in_volume / (total_in_volume + total_out_volume)
             ELSE 0.5 END,                                    // In/Out ratio
        log(total_in_volume + total_out_volume + 1) / 20,   // Volume scale
        // Activity patterns (from address)
        CASE WHEN a.out_transaction_count > 0
             THEN a.in_transaction_count::float / a.out_transaction_count
             ELSE a.in_transaction_count END,                // Tx direction ratio
        log(a.in_transaction_count + a.out_transaction_count + 1) / 10,
        // Diversity
        (in_asset_count + out_asset_count) / 10.0,          // Asset diversity
        a.neighbor_count / 100.0                             // Connectivity
    ],
    
    a.network_embedding = [
        coalesce(a.pagerank, 0) * 1000,      // Network importance
        a.community_id / 100.0,              // Community membership
        a.neighbor_count / 100.0,            // Degree centrality
        log(a.in_transaction_count + a.out_transaction_count + 1) / 10
    ]
    """
```

### Query Patterns

#### Basic Queries
```cypher
-- Get all transfers for an address
MATCH (a:Address {address: 'addr1'})-[r:TO]->(b:Address)
RETURN a.address, r.asset, r.volume, r.transfer_count, b.address

-- Get asset-specific transfers
MATCH (a:Address {address: 'addr1'})-[r:TO {asset: 'TOR'}]->(b:Address)
RETURN a, r, b

-- Calculate volumes by asset
MATCH (a:Address {address: 'addr1'})-[r:TO]->()
RETURN r.asset, 
       sum(r.volume) as out_volume, 
       sum(r.transfer_count) as out_transfers
GROUP BY r.asset
```

#### Advanced Queries
```cypher
-- Find similar addresses by pattern
CALL vector_search.search("FinancialEmbeddings", 10, [
    0.8,   -- High incoming ratio
    15.0,  -- High volume scale
    2.0,   -- Balanced transaction direction
    5.0,   -- Medium activity
    0.3,   -- Multi-asset
    0.5    -- Medium connectivity
])
YIELD node, similarity
RETURN node.address, node.community_id, similarity

-- Shortest path for specific asset
MATCH path = shortestPath(
    (a:Address {address: 'addr1'})-[:TO*..10 {asset: 'TOR'}]->(b:Address {address: 'addr2'})
)
RETURN path

-- Community members by PageRank
MATCH (a:Address {community_id: 5})
RETURN a.address, a.pagerank
ORDER BY a.pagerank DESC
LIMIT 20
```

### Indexes

```cypher
-- Address indexes
CREATE INDEX ON :Address(address);
CREATE INDEX ON :Address(community_id);
CREATE INDEX ON :Address(pagerank);

-- Edge indexes
CREATE EDGE INDEX ON :TO(asset);
CREATE EDGE INDEX ON :TO(from, to, asset);
CREATE EDGE INDEX ON :TO(asset, volume);
CREATE EDGE INDEX ON :TO(asset, transfer_count);

-- Vector indexes (unchanged)
CREATE VECTOR INDEX FinancialEmbeddings ON :Address(financial_embedding);
CREATE VECTOR INDEX TemporalEmbeddings ON :Address(temporal_embedding);
CREATE VECTOR INDEX NetworkEmbeddings ON :Address(network_embedding);
CREATE VECTOR INDEX JointEmbeddings ON :Address(joint_embedding);
```

### Benefits

1. **Clean Separation**: Each asset's volumes are isolated on edges
2. **No Data Corruption**: Different asset values don't mix
3. **Fair Network Analysis**: PageRank based on activity, not wealth
4. **Meaningful Embeddings**: Use ratios and normalized values
5. **Flexible Queries**: Easy to filter by asset or aggregate
6. **Minimal Changes**: Reuses existing graph structure

### Migration Notes

Since we "always start from 0", no migration is needed. The indexer will create the new structure from the beginning with:
- Address nodes without volume properties
- Edges with asset-specific volumes
- Community and PageRank calculations based on connections
- Embeddings using the hybrid approach