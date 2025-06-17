# Plan: Edge-Based Volume Tracking

## Overview
Move `volume_in`, `volume_out`, `first_transfer_timestamp`, and `last_transfer_timestamp` from Address nodes to edge-based calculations. This enables proper multi-asset support without mixing different asset values.

## Current State vs Target State

### Current (Problematic)
```cypher
(:Address {
    address: "0x123",
    volume_in: 1500,      // Mixed TOR + TAO + DOT!
    volume_out: 1000,     // Mixed assets
    first_transfer_timestamp: 123456,
    last_transfer_timestamp: 789012
})
```

### Target (Clean)
```cypher
(:Address {
    address: "0x123",
    // Only network structure properties
    neighbor_count: 10,
    in_transaction_count: 50,
    out_transaction_count: 45,
    community_id: 5,
    pagerank: 0.0023
})

// Volumes stored on edges
(:Address)-[:TO {
    asset: "TOR",
    volume: 1000,
    transfer_count: 5,
    first_transfer_timestamp: 123456,
    last_transfer_timestamp: 789012,
    // Transaction frequency data
    tx_frequency: [3600, 7200, 1800, 3600],  // Time gaps between transfers
    avg_time_gap: 4050,
    transfer_pattern: "regular"  // regular, burst, irregular
}]->(:Address)
```

## Transaction Frequency Tracking

### Edge Properties for Frequency Analysis
```cypher
(:Address)-[:TO {
    // Basic properties
    asset: "TOR",
    volume: 1000,
    transfer_count: 5,
    
    // Timestamps
    first_transfer_timestamp: 123456,
    last_transfer_timestamp: 789012,
    
    // Frequency tracking (NEW)
    tx_frequency: [3600, 7200, 1800, 3600],  // Array of time gaps
    avg_time_gap: 4050,                      // Average gap in seconds
    std_time_gap: 2100,                      // Standard deviation
    min_time_gap: 1800,                      // Minimum gap
    max_time_gap: 7200,                      // Maximum gap
    transfer_pattern: "regular",             // Pattern classification
    
    // Optional: Block-based frequency
    block_frequency: [10, 20, 5, 10],        // Gaps in blocks
    avg_block_gap: 11.25
}]->(:Address)
```

### Indexer Updates for Frequency Tracking

```python
def _process_transfer_events(self, transaction, timestamp, events):
    """Process transfers with frequency tracking"""
    query = """
    MERGE (sender:Address { address: $from })
    ON CREATE SET
        sender.first_seen_timestamp = $timestamp,
        sender.neighbor_count = 0,
        sender.in_transaction_count = 0,
        sender.out_transaction_count = 0
        
    MERGE (receiver:Address { address: $to })
    ON CREATE SET
        receiver.first_seen_timestamp = $timestamp,
        receiver.neighbor_count = 0,
        receiver.in_transaction_count = 0,
        receiver.out_transaction_count = 0
        
    // Edge with frequency tracking
    MERGE (sender)-[r:TO { 
        from: $from,
        to: $to,
        asset: $asset 
    }]->(receiver)
    ON CREATE SET
        r.volume = $amount,
        r.transfer_count = 1,
        r.first_transfer_timestamp = $timestamp,
        r.last_transfer_timestamp = $timestamp,
        r.first_transfer_block_height = $block_height,
        r.last_transfer_block_height = $block_height,
        // Initialize frequency arrays
        r.tx_frequency = [],
        r.block_frequency = [],
        sender.neighbor_count = sender.neighbor_count + 1,
        receiver.neighbor_count = receiver.neighbor_count + 1
    ON MATCH SET
        r.volume = r.volume + $amount,
        r.transfer_count = r.transfer_count + 1,
        // Update frequency arrays
        r.tx_frequency = r.tx_frequency + [$timestamp - r.last_transfer_timestamp],
        r.block_frequency = r.block_frequency + [$block_height - r.last_transfer_block_height],
        r.last_transfer_timestamp = $timestamp,
        r.last_transfer_block_height = $block_height
        
    // Update transaction counts
    SET sender.out_transaction_count = sender.out_transaction_count + 1,
        receiver.in_transaction_count = receiver.in_transaction_count + 1
    """
```

### Periodic Frequency Analysis

```python
def update_frequency_metrics(self):
    """Calculate frequency metrics for edges"""
    query = """
    MATCH ()-[r:TO]->()
    WHERE size(r.tx_frequency) > 0
    WITH r,
         reduce(sum = 0, gap IN r.tx_frequency | sum + gap) / size(r.tx_frequency) as avg_gap,
         r.tx_frequency as gaps
    
    // Calculate standard deviation
    WITH r, avg_gap, gaps,
         sqrt(reduce(sum = 0.0, gap IN gaps | 
              sum + (gap - avg_gap) * (gap - avg_gap)) / size(gaps)) as std_gap
    
    SET r.avg_time_gap = avg_gap,
        r.std_time_gap = std_gap,
        r.min_time_gap = reduce(min = 999999999, gap IN gaps | 
                               CASE WHEN gap < min THEN gap ELSE min END),
        r.max_time_gap = reduce(max = 0, gap IN gaps | 
                               CASE WHEN gap > max THEN gap ELSE max END),
        r.transfer_pattern = CASE
            WHEN std_gap < avg_gap * 0.3 THEN 'regular'
            WHEN r.max_time_gap > avg_gap * 5 THEN 'burst'
            ELSE 'irregular'
        END
    """
```

## Implementation Plan

### Phase 1: Update Graph Model

#### 1.1 Modify Indexer (`money_flow_indexer.py`)
```python
def _process_transfer_events(self, transaction, timestamp, events):
    """Remove volume updates from Address nodes"""
    query = """
    MERGE (sender:Address { address: $from })
    ON CREATE SET
        sender.first_seen_timestamp = $timestamp,
        sender.neighbor_count = 0,
        sender.in_transaction_count = 0,
        sender.out_transaction_count = 0
    SET 
        sender.last_seen_timestamp = $timestamp
        // REMOVE: volume_in, volume_out updates
        
    MERGE (receiver:Address { address: $to })
    ON CREATE SET
        receiver.first_seen_timestamp = $timestamp,
        receiver.neighbor_count = 0,
        receiver.in_transaction_count = 0,
        receiver.out_transaction_count = 0
    SET 
        receiver.last_seen_timestamp = $timestamp
        // REMOVE: volume_in, volume_out updates
        
    // Edge keeps all volume data
    MERGE (sender)-[r:TO { 
        from: $from,
        to: $to,
        asset: $asset 
    }]->(receiver)
    ON CREATE SET
        r.volume = $amount,
        r.transfer_count = 1,
        r.first_transfer_timestamp = $timestamp,
        r.last_transfer_timestamp = $timestamp,
        r.tx_frequency = [],
        sender.neighbor_count = sender.neighbor_count + 1,
        receiver.neighbor_count = receiver.neighbor_count + 1
    ON MATCH SET
        r.volume = r.volume + $amount,
        r.transfer_count = r.transfer_count + 1,
        r.tx_frequency = r.tx_frequency + [$timestamp - r.last_transfer_timestamp],
        r.last_transfer_timestamp = $timestamp
        
    // Update transaction counts
    SET sender.out_transaction_count = sender.out_transaction_count + 1,
        receiver.in_transaction_count = receiver.in_transaction_count + 1
    """
```

### Phase 2: Update API Layer

#### 2.1 Enhance Money Flow Service (`money_flow_service.py`)

Add volume calculation methods with frequency data:

```python
def calculate_address_volumes(self, address: str, assets: List[str] = None) -> List[Dict[str, Any]]:
    """Calculate volumes for an address across specified assets"""
    
    asset_filter = ""
    params = {'address': address}
    
    if assets and assets != ["all"]:
        asset_filter = "WHERE r.asset IN $assets"
        params['assets'] = assets
    
    query = f"""
    MATCH (a:Address {{address: $address}})
    OPTIONAL MATCH (a)-[out:TO]->() {asset_filter}
    WITH a, out.asset as asset, 
         sum(out.volume) as out_volume, 
         sum(out.transfer_count) as out_count,
         min(out.first_transfer_timestamp) as first_out,
         max(out.last_transfer_timestamp) as last_out,
         avg(out.avg_time_gap) as avg_out_frequency,
         collect(out.transfer_pattern) as out_patterns
    WHERE asset IS NOT NULL
    
    WITH collect({{
        asset: asset, 
        volume_out: out_volume,
        transfer_count_out: out_count,
        first_transfer_out: first_out,
        last_transfer_out: last_out,
        avg_out_frequency: avg_out_frequency,
        dominant_out_pattern: head(reverse(sort(out_patterns)))
    }}) as outgoing
    
    MATCH (a:Address {{address: $address}})
    OPTIONAL MATCH ()-[in:TO]->(a) {asset_filter}
    WITH outgoing, in.asset as asset, 
         sum(in.volume) as in_volume,
         sum(in.transfer_count) as in_count,
         min(in.first_transfer_timestamp) as first_in,
         max(in.last_transfer_timestamp) as last_in,
         avg(in.avg_time_gap) as avg_in_frequency,
         collect(in.transfer_pattern) as in_patterns
    WHERE asset IS NOT NULL
    
    RETURN outgoing, collect({{
        asset: asset,
        volume_in: in_volume,
        transfer_count_in: in_count,
        first_transfer_in: first_in,
        last_transfer_in: last_in,
        avg_in_frequency: avg_in_frequency,
        dominant_in_pattern: head(reverse(sort(in_patterns)))
    }}) as incoming
    """
    
    with self.graph_database.session() as session:
        result = session.run(query, params).single()
        
        # Merge incoming and outgoing data
        volumes = {}
        
        for out in result['outgoing']:
            asset = out['asset']
            volumes[asset] = {
                'asset': asset,
                'volume_in': 0,
                'volume_out': out['volume_out'],
                'transfer_count_in': 0,
                'transfer_count_out': out['transfer_count_out'],
                'first_transfer': out['first_transfer_out'],
                'last_transfer': out['last_transfer_out'],
                'avg_out_frequency': out['avg_out_frequency'],
                'avg_in_frequency': None,
                'transfer_pattern': out['dominant_out_pattern']
            }
            
        for inc in result['incoming']:
            asset = inc['asset']
            if asset in volumes:
                volumes[asset]['volume_in'] = inc['volume_in']
                volumes[asset]['transfer_count_in'] = inc['transfer_count_in']
                volumes[asset]['avg_in_frequency'] = inc['avg_in_frequency']
                volumes[asset]['first_transfer'] = min(
                    volumes[asset]['first_transfer'] or float('inf'),
                    inc['first_transfer_in'] or float('inf')
                )
                volumes[asset]['last_transfer'] = max(
                    volumes[asset]['last_transfer'] or 0,
                    inc['last_transfer_in'] or 0
                )
            else:
                volumes[asset] = {
                    'asset': asset,
                    'volume_in': inc['volume_in'],
                    'volume_out': 0,
                    'transfer_count_in': inc['transfer_count_in'],
                    'transfer_count_out': 0,
                    'first_transfer': inc['first_transfer_in'],
                    'last_transfer': inc['last_transfer_in'],
                    'avg_in_frequency': inc['avg_in_frequency'],
                    'avg_out_frequency': None,
                    'transfer_pattern': inc['dominant_in_pattern']
                }
                
        return list(volumes.values())
```

### Phase 3: Update Embeddings with Frequency Features

```python
def update_embeddings(self, addresses: Optional[List[str]] = None):
    """Calculate embeddings using edge-based volumes and frequencies"""
    
    query = """
    MATCH (a:Address)
    {address_filter}
    
    // Calculate frequency metrics
    OPTIONAL MATCH (a)-[out:TO]->()
    WITH a,
         avg(out.avg_time_gap) as avg_out_frequency,
         avg(out.transfer_count / ((out.last_transfer_timestamp - out.first_transfer_timestamp) / 86400000.0 + 1)) as out_tx_rate
         
    OPTIONAL MATCH ()-[in:TO]->(a)
    WITH a, avg_out_frequency, out_tx_rate,
         avg(in.avg_time_gap) as avg_in_frequency,
         avg(in.transfer_count / ((in.last_transfer_timestamp - in.first_transfer_timestamp) / 86400000.0 + 1)) as in_tx_rate
    
    SET a.temporal_embedding = [
        // Frequency features
        log(coalesce(avg_out_frequency, 86400) + 1) / 20,  // Normalized out frequency
        log(coalesce(avg_in_frequency, 86400) + 1) / 20,   // Normalized in frequency
        coalesce(out_tx_rate, 0) / 100,                    // Transactions per day (out)
        coalesce(in_tx_rate, 0) / 100                      // Transactions per day (in)
    ]
    """
```

## Frequency Analysis Queries

### Find Regular Transfer Patterns
```cypher
MATCH (a:Address)-[r:TO {transfer_pattern: 'regular'}]->(b:Address)
WHERE r.transfer_count > 10
RETURN a.address, b.address, r.asset, r.avg_time_gap, r.transfer_count
ORDER BY r.transfer_count DESC
```

### Detect Burst Activity
```cypher
MATCH (a:Address)-[r:TO {transfer_pattern: 'burst'}]->(b:Address)
WHERE r.last_transfer_timestamp > timestamp() - 86400000  // Last 24 hours
RETURN a.address, b.address, r.asset, r.volume, r.max_time_gap
ORDER BY r.volume DESC
```

### High-Frequency Trading Detection
```cypher
MATCH (a:Address)-[r:TO]->(b:Address)
WHERE r.avg_time_gap < 300  // Less than 5 minutes average
  AND r.transfer_count > 100
RETURN a.address, b.address, r.asset, r.avg_time_gap, r.transfer_count
ORDER BY r.avg_time_gap ASC
```

## Benefits

### What We Win
1. **Accurate Multi-Asset Support**: Each asset's volumes tracked separately
2. **Transaction Frequency Analysis**: Pattern detection per asset pair
3. **No Data Corruption**: Low-value assets don't mix with high-value ones
4. **Flexible Queries**: Can analyze single asset or aggregate across assets
5. **Better Embeddings**: Include temporal patterns and frequencies
6. **Cleaner Model**: Address nodes only store network properties

### What We Lose
1. **Query Complexity**: Need to aggregate from edges for total volumes
2. **Storage**: Frequency arrays can grow (but can be capped)
3. **Performance**: Some queries become more complex (but can be optimized)
4. **Migration Effort**: Need to rebuild graph from scratch

## Migration Strategy

Since "we always start from 0":

1. Update indexer code to new model with frequency tracking
2. Clear existing graph database
3. Re-index from block 0 with new structure
4. Update all API endpoints to use edge-based calculations
5. Test thoroughly with multi-asset scenarios

## Example Queries

### Get Address with Volumes and Frequencies
```cypher
MATCH (a:Address {address: '0x123'})
OPTIONAL MATCH (a)-[out:TO]->()
WITH a, out.asset as asset, 
     sum(out.volume) as volume_out,
     avg(out.avg_time_gap) as avg_frequency,
     collect(DISTINCT out.transfer_pattern) as patterns
WHERE asset IS NOT NULL
RETURN a, collect({
    asset: asset, 
    volume_out: volume_out,
    avg_frequency: avg_frequency,
    patterns: patterns
}) as asset_metrics
```

### Find High-Volume Regular Traders
```cypher
MATCH (a:Address)-[r:TO {asset: 'TOR', transfer_pattern: 'regular'}]-()
WITH a, sum(r.volume) as tor_volume, avg(r.avg_time_gap) as avg_frequency
WHERE tor_volume > 100000
RETURN a.address, tor_volume, avg_frequency
ORDER BY tor_volume DESC
```

## Conclusion

This approach cleanly separates concerns:
- **Address nodes**: Network structure and position
- **Edges**: Asset-specific volumes, transfers, and frequency patterns
- **API layer**: Dynamic calculation and aggregation

The result is a truly multi-asset aware system that maintains data integrity while providing rich temporal analysis capabilities.