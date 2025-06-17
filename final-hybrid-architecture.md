# Final Hybrid Architecture: Graph + ClickHouse

## Overview
- **Graph (Memgraph)**: Aggregated data for fast traversals and network analysis
- **ClickHouse**: Detailed transfer history, frequency analysis, and embeddings for search

## Graph Model

### Address Nodes
```cypher
(:Address {
    address: "0x123",
    
    // Activity timestamps (keep for quick filtering)
    first_seen_timestamp: 1701432000000,
    last_seen_timestamp: 1701450000000,
    first_seen_block: 1000,
    last_seen_block: 5000,
    
    // Network metrics (asset-agnostic)
    neighbor_count: 10,
    in_transaction_count: 50,
    out_transaction_count: 45,
    
    // Network analysis results
    community_id: 5,
    pagerank: 0.0023,
    
    // Remove: volume_in, volume_out (calculate from edges)
})
```

### Edge Relationships
```cypher
(:Address)-[:TO {
    // Identity
    from: "addr1",
    to: "addr2", 
    asset: "TOR",
    
    // Aggregated volume (keep for fast queries)
    volume: 10000.5,
    transfer_count: 25,
    
    // Activity timestamps (keep for filtering)
    first_transfer_timestamp: 1701432000000,
    last_transfer_timestamp: 1701450000000,
    first_transfer_block: 1000,
    last_transfer_block: 5000,
    
    // Remove: arrays, detailed frequencies (move to ClickHouse)
}]->(:Address)
```

## ClickHouse Schema

### Enhanced Transfers Table
```sql
-- Detailed transfer records (existing balance_transfers enhanced)
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
    
    -- Additional metadata
    transfer_type String DEFAULT 'standard',  -- standard, stake, reward, etc.
    
    -- Indexes
    INDEX idx_from (from_address) TYPE bloom_filter(0.01),
    INDEX idx_to (to_address) TYPE bloom_filter(0.01),
    INDEX idx_pair (from_address, to_address) TYPE bloom_filter(0.01),
    INDEX idx_asset (asset) TYPE bloom_filter(0.01)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_timestamp, from_address, to_address, asset);
```

### Materialized Views for Analytics

#### 1. Transfer Frequencies
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS transfer_frequencies_mv
ENGINE = AggregatingMergeTree()
ORDER BY (from_address, to_address, asset)
AS
WITH ordered_transfers AS (
    SELECT 
        from_address,
        to_address,
        asset,
        block_timestamp,
        block_height,
        amount,
        ROW_NUMBER() OVER (
            PARTITION BY from_address, to_address, asset 
            ORDER BY block_timestamp
        ) as transfer_num
    FROM transfers
),
transfer_gaps AS (
    SELECT 
        t1.from_address,
        t1.to_address,
        t1.asset,
        t1.block_timestamp as timestamp,
        t2.block_timestamp as next_timestamp,
        t1.block_height as block,
        t2.block_height as next_block,
        t2.block_timestamp - t1.block_timestamp as time_gap_ms,
        t2.block_height - t1.block_height as block_gap
    FROM ordered_transfers t1
    JOIN ordered_transfers t2 ON 
        t1.from_address = t2.from_address AND
        t1.to_address = t2.to_address AND
        t1.asset = t2.asset AND
        t1.transfer_num = t2.transfer_num - 1
)
SELECT
    from_address,
    to_address,
    asset,
    count() + 1 as transfer_count,  -- +1 for first transfer
    avg(time_gap_ms) as avg_time_gap,
    stddevPop(time_gap_ms) as time_gap_stddev,
    min(time_gap_ms) as min_time_gap,
    max(time_gap_ms) as max_time_gap,
    median(time_gap_ms) as median_time_gap,
    avg(block_gap) as avg_block_gap,
    -- Pattern detection
    CASE
        WHEN stddevPop(time_gap_ms) < avg(time_gap_ms) * 0.3 THEN 'regular'
        WHEN max(time_gap_ms) > avg(time_gap_ms) * 5 THEN 'burst'
        ELSE 'irregular'
    END as transfer_pattern
FROM transfer_gaps
GROUP BY from_address, to_address, asset;
```

#### 2. Address Activity Profiles
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS address_activity_mv
ENGINE = AggregatingMergeTree()
ORDER BY (address, asset)
AS
SELECT
    address,
    asset,
    countIf(direction = 'out') as out_transfers,
    countIf(direction = 'in') as in_transfers,
    sumIf(amount, direction = 'out') as volume_out,
    sumIf(amount, direction = 'in') as volume_in,
    uniqIf(counterparty, direction = 'out') as unique_recipients,
    uniqIf(counterparty, direction = 'in') as unique_senders,
    minIf(block_timestamp, direction = 'out') as first_out_transfer,
    maxIf(block_timestamp, direction = 'out') as last_out_transfer,
    minIf(block_timestamp, direction = 'in') as first_in_transfer,
    maxIf(block_timestamp, direction = 'in') as last_in_transfer,
    -- Activity metrics
    (maxIf(block_timestamp, direction = 'out') - minIf(block_timestamp, direction = 'out')) / 86400000 as out_activity_days,
    (maxIf(block_timestamp, direction = 'in') - minIf(block_timestamp, direction = 'in')) / 86400000 as in_activity_days
FROM (
    SELECT 
        from_address as address, 
        to_address as counterparty,
        asset, 
        amount, 
        block_timestamp, 
        'out' as direction
    FROM transfers
    UNION ALL
    SELECT 
        to_address as address,
        from_address as counterparty,
        asset, 
        amount, 
        block_timestamp, 
        'in' as direction
    FROM transfers
)
GROUP BY address, asset;
```

#### 3. Address Embeddings Table
```sql
-- Store pre-calculated embeddings in ClickHouse
CREATE TABLE IF NOT EXISTS address_embeddings (
    address String,
    embedding_type String,  -- 'financial', 'temporal', 'behavioral', 'joint'
    embedding Array(Float32),
    
    -- Metadata
    calculated_at DateTime DEFAULT now(),
    last_activity_block UInt32,
    
    -- Features used (for debugging/analysis)
    primary_asset String,
    total_volume Float64,
    activity_score Float32,
    
    -- Indexes for similarity search
    INDEX embedding_idx embedding TYPE annoy(100, 'L2Distance')
) ENGINE = MergeTree()
ORDER BY (address, embedding_type)
SETTINGS index_granularity = 8192;
```

## Implementation Updates

### 1. Indexer Modifications
```python
def _process_transfer_events(self, transaction, timestamp, events):
    """Process transfers with aggregated edge data"""
    query = """
    MERGE (sender:Address { address: $from })
    ON CREATE SET
        sender.first_seen_timestamp = $timestamp,
        sender.first_seen_block = $block_height,
        sender.neighbor_count = 0,
        sender.in_transaction_count = 0,
        sender.out_transaction_count = 0
    SET 
        sender.last_seen_timestamp = $timestamp,
        sender.last_seen_block = $block_height
        
    MERGE (receiver:Address { address: $to })
    ON CREATE SET
        receiver.first_seen_timestamp = $timestamp,
        receiver.first_seen_block = $block_height,
        receiver.neighbor_count = 0,
        receiver.in_transaction_count = 0,
        receiver.out_transaction_count = 0
    SET 
        receiver.last_seen_timestamp = $timestamp,
        receiver.last_seen_block = $block_height
        
    // Edge with aggregated data only
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
        r.first_transfer_block = $block_height,
        r.last_transfer_block = $block_height,
        sender.neighbor_count = sender.neighbor_count + 1,
        receiver.neighbor_count = receiver.neighbor_count + 1
    ON MATCH SET
        r.volume = r.volume + $amount,
        r.transfer_count = r.transfer_count + 1,
        r.last_transfer_timestamp = $timestamp,
        r.last_transfer_block = $block_height
        
    // Update transaction counts
    SET sender.out_transaction_count = sender.out_transaction_count + 1,
        receiver.in_transaction_count = receiver.in_transaction_count + 1
    """
```

### 2. API Service Updates
```python
class MoneyFlowService:
    def __init__(self, graph_database: Driver, clickhouse_client):
        self.graph_database = graph_database
        self.clickhouse = clickhouse_client
        
    def calculate_address_volumes(self, address: str, assets: List[str] = None):
        """Calculate volumes from graph edges"""
        
        asset_filter = ""
        params = {'address': address}
        
        if assets and assets != ["all"]:
            asset_filter = "WHERE r.asset IN $assets"
            params['assets'] = assets
        
        # Get aggregated volumes from graph edges
        query = f"""
        MATCH (a:Address {{address: $address}})
        OPTIONAL MATCH (a)-[out:TO]->() {asset_filter}
        WITH a, out.asset as asset, 
             sum(out.volume) as out_volume,
             sum(out.transfer_count) as out_count,
             min(out.first_transfer_timestamp) as first_out,
             max(out.last_transfer_timestamp) as last_out
        WHERE asset IS NOT NULL
        
        WITH collect({{
            asset: asset,
            volume_out: out_volume,
            transfer_count_out: out_count,
            first_out: first_out,
            last_out: last_out
        }}) as outgoing
        
        MATCH (a:Address {{address: $address}})
        OPTIONAL MATCH ()-[in:TO]->(a) {asset_filter}
        WITH outgoing, in.asset as asset,
             sum(in.volume) as in_volume,
             sum(in.transfer_count) as in_count,
             min(in.first_transfer_timestamp) as first_in,
             max(in.last_transfer_timestamp) as last_in
        WHERE asset IS NOT NULL
        
        RETURN outgoing, collect({{
            asset: asset,
            volume_in: in_volume,
            transfer_count_in: in_count,
            first_in: first_in,
            last_in: last_in
        }}) as incoming
        """
        
        with self.graph_database.session() as session:
            result = session.run(query, params).single()
            
        # Merge results
        volumes = self._merge_volume_data(result['outgoing'], result['incoming'])
        
        # Enrich with frequency data from ClickHouse if needed
        if self._should_include_frequencies():
            frequencies = self._get_frequencies_from_clickhouse(address, assets)
            volumes = self._enrich_with_frequencies(volumes, frequencies)
            
        return volumes
    
    def _get_frequencies_from_clickhouse(self, address: str, assets: List[str] = None):
        """Get detailed frequency patterns from ClickHouse"""
        
        asset_filter = ""
        if assets and assets != ["all"]:
            asset_filter = f"AND asset IN {tuple(assets)}"
            
        query = f"""
        SELECT 
            CASE 
                WHEN from_address = '{address}' THEN to_address
                ELSE from_address
            END as counterparty,
            asset,
            transfer_count,
            avg_time_gap,
            transfer_pattern,
            CASE 
                WHEN from_address = '{address}' THEN 'out'
                ELSE 'in'
            END as direction
        FROM transfer_frequencies_mv
        WHERE (from_address = '{address}' OR to_address = '{address}')
        {asset_filter}
        """
        
        return self.clickhouse.query(query).result_rows
```

### 3. Embedding Calculation Service
```python
class EmbeddingService:
    def __init__(self, graph_database: Driver, clickhouse_client):
        self.graph_database = graph_database
        self.clickhouse = clickhouse_client
        
    def calculate_and_store_embeddings(self, addresses: List[str] = None):
        """Calculate embeddings and store in ClickHouse"""
        
        # Get data from graph
        graph_query = """
        MATCH (a:Address)
        WHERE a.address IN $addresses OR $addresses IS NULL
        OPTIONAL MATCH (a)-[r:TO]-()
        WITH a,
             sum(r.volume) as total_volume,
             count(DISTINCT r.asset) as asset_diversity,
             a.pagerank as pagerank,
             a.community_id as community_id
        RETURN a.address as address,
               total_volume,
               asset_diversity,
               pagerank,
               community_id,
               a.in_transaction_count as in_txs,
               a.out_transaction_count as out_txs
        """
        
        with self.graph_database.session() as session:
            graph_data = session.run(graph_query, {'addresses': addresses}).data()
        
        # Get additional data from ClickHouse
        ch_data = self._get_activity_profiles_from_clickhouse(addresses)
        
        # Calculate embeddings
        embeddings = []
        for node in graph_data:
            address = node['address']
            ch_profile = ch_data.get(address, {})
            
            # Financial embedding
            financial_emb = [
                np.log(node['total_volume'] + 1) / 20,
                node['asset_diversity'] / 10,
                ch_profile.get('volume_concentration', 0),
                ch_profile.get('in_out_ratio', 0.5),
                ch_profile.get('avg_tx_size', 0) / 1000,
                ch_profile.get('tx_size_variance', 0)
            ]
            
            # Store in ClickHouse
            embeddings.append({
                'address': address,
                'embedding_type': 'financial',
                'embedding': financial_emb,
                'primary_asset': ch_profile.get('primary_asset', 'unknown'),
                'total_volume': node['total_volume'],
                'activity_score': ch_profile.get('activity_score', 0)
            })
        
        # Batch insert to ClickHouse
        self._store_embeddings_in_clickhouse(embeddings)
```

### 4. Similarity Search Using ClickHouse
```python
def find_similar_addresses(self, target_address: str, embedding_type: str = 'financial', limit: int = 10):
    """Find similar addresses using ClickHouse vector search"""
    
    # Get target embedding
    target_query = f"""
    SELECT embedding
    FROM address_embeddings
    WHERE address = '{target_address}' 
      AND embedding_type = '{embedding_type}'
    ORDER BY calculated_at DESC
    LIMIT 1
    """
    
    target_embedding = self.clickhouse.query(target_query).result_rows[0][0]
    
    # Search similar addresses
    search_query = f"""
    SELECT 
        address,
        embedding,
        L2Distance(embedding, {target_embedding}) as distance,
        primary_asset,
        total_volume,
        activity_score
    FROM address_embeddings
    WHERE embedding_type = '{embedding_type}'
      AND address != '{target_address}'
    ORDER BY distance ASC
    LIMIT {limit}
    """
    
    results = self.clickhouse.query(search_query).result_rows
    
    # Enrich with graph data
    addresses = [r[0] for r in results]
    graph_data = self._get_graph_properties(addresses)
    
    return self._combine_search_results(results, graph_data)
```

## Benefits of This Architecture

1. **Balanced Storage**: 
   - Graph keeps aggregated data for fast traversals
   - ClickHouse handles unbounded detailed history

2. **Performance**:
   - Graph queries remain fast with lightweight edges
   - ClickHouse materialized views provide instant analytics

3. **Flexibility**:
   - Can add new analytics without changing graph structure
   - Embeddings in ClickHouse enable fast similarity search

4. **Scalability**:
   - No array growth issues in graph
   - ClickHouse handles billions of transfers efficiently

5. **Multi-Asset Support**:
   - Each edge tracks single asset volumes
   - No mixing of different asset values

## Query Examples

### Get Address with Volumes (Graph Only)
```cypher
MATCH (a:Address {address: '0x123'})
OPTIONAL MATCH (a)-[out:TO]->()
WITH a, out.asset as asset, sum(out.volume) as volume_out
WHERE asset IS NOT NULL
RETURN a, collect({asset: asset, volume_out: volume_out}) as volumes
```

### Get Transfer Patterns (ClickHouse)
```sql
SELECT * FROM transfer_frequencies_mv
WHERE from_address = '0x123' OR to_address = '0x123'
ORDER BY transfer_count DESC
```

### Find Similar Addresses (Hybrid)
```python
# Uses ClickHouse for embedding search, then enriches with graph data
similar = similarity_service.find_similar_addresses('0x123', 'financial', 20)
```

This architecture provides the best balance between performance, scalability, and functionality!