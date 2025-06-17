# ClickHouse Similarity Search Integration Plan

## Overview
This plan details how to update the similarity search services to store and query embeddings from ClickHouse instead of the graph database, while maintaining the existing API interface.

## Current State Analysis

### 1. Current Architecture
- **Embeddings stored in graph**: 4 types (financial, temporal, network, joint) stored as properties on Address nodes
- **Vector indexes in Memgraph**: Using Memgraph's built-in vector search with cosine similarity
- **Service structure**:
  - `similarity_search_service.py`: Core logic for vector search
  - `similarity_search.py`: MCP tool wrapper
  - `money_flow_indexer.py`: Calculates and stores embeddings

### 2. Embedding Dimensions
- **Financial**: 6 dimensions (volumes, transfer counts, ratios)
- **Temporal**: 4 dimensions (timestamps, frequencies)
- **Network**: 4 dimensions (PageRank, community, unique connections)
- **Joint**: 14 dimensions (concatenation of all above)

## Proposed Architecture

### 1. ClickHouse Schema

```sql
-- Main embeddings table
CREATE TABLE address_embeddings (
    address String,
    asset String,
    embedding_type Enum('financial', 'temporal', 'network', 'joint'),
    embedding Array(Float32),
    
    -- Denormalized properties for filtering
    volume_in Float64,
    volume_out Float64,
    transfer_count UInt64,
    first_seen_timestamp DateTime64(3),
    last_seen_timestamp DateTime64(3),
    community_id Int32,
    
    -- Metadata
    updated_at DateTime64(3) DEFAULT now64(3),
    
    -- Primary key for deduplication
    PRIMARY KEY (address, asset, embedding_type)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (asset, embedding_type, address);

-- ANNOY index for each embedding type
ALTER TABLE address_embeddings 
ADD INDEX financial_idx embedding TYPE annoy(100, 'cosine') 
GRANULARITY 1000
WHERE embedding_type = 'financial';

ALTER TABLE address_embeddings 
ADD INDEX temporal_idx embedding TYPE annoy(100, 'cosine') 
GRANULARITY 1000
WHERE embedding_type = 'temporal';

ALTER TABLE address_embeddings 
ADD INDEX network_idx embedding TYPE annoy(100, 'cosine') 
GRANULARITY 1000
WHERE embedding_type = 'network';

ALTER TABLE address_embeddings 
ADD INDEX joint_idx embedding TYPE annoy(100, 'cosine') 
GRANULARITY 1000
WHERE embedding_type = 'joint';
```

### 2. Service Architecture Updates

#### A. New ClickHouse Client Service
```python
# packages/api/services/clickhouse_client.py
class ClickHouseClient:
    def __init__(self, host, port, database):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database
        )
    
    def insert_embeddings(self, embeddings_data):
        """Batch insert embeddings"""
        
    def search_similar(self, embedding_type, query_vector, limit, min_similarity):
        """Vector similarity search using ANNOY index"""
```

#### B. Updated SimilaritySearchService
```python
class SimilaritySearchService:
    def __init__(self, graph_database: Driver, clickhouse_client: ClickHouseClient):
        self.graph_database = graph_database
        self.clickhouse = clickhouse_client
```

## Implementation Steps

### Phase 1: ClickHouse Setup
1. **Create ClickHouse tables and indexes**
   - Address embeddings table with proper partitioning
   - ANNOY indexes for each embedding type
   - Materialized views for common aggregations

2. **Update configuration**
   - Add ClickHouse connection parameters to `.env`
   - Update service initialization

### Phase 2: Indexer Updates
1. **Modify `money_flow_indexer.py`**:
   - Keep embedding calculation logic
   - Add batch insertion to ClickHouse
   - Remove embedding storage in graph
   - Keep only timestamps and basic metrics on Address nodes

2. **Create embedding sync job**:
   - One-time migration of existing embeddings
   - Ongoing sync for new/updated addresses

### Phase 3: Search Service Updates
1. **Update `similarity_search_service.py`**:
   - Replace Memgraph vector search with ClickHouse queries
   - Implement hybrid queries (ClickHouse similarity + graph traversal)
   - Maintain backward-compatible API

2. **Query flow**:
   ```python
   # 1. Search similar addresses in ClickHouse
   similar_addresses = clickhouse.search_similar(
       embedding_type, query_vector, limit, min_similarity
   )
   
   # 2. Enrich with graph data
   enriched_results = graph.enrich_addresses(
       [addr['address'] for addr in similar_addresses]
   )
   
   # 3. Combine similarity scores with graph properties
   return combine_results(similar_addresses, enriched_results)
   ```

### Phase 4: API Updates
1. **Update MCP tool** (`similarity_search.py`):
   - No changes needed if service interface remains the same
   - Add optional parameters for new features

2. **Add new endpoints**:
   - Bulk similarity search
   - Time-range filtered search
   - Multi-asset similarity comparison

## Migration Strategy

### 1. Data Migration
```python
# One-time migration script
def migrate_embeddings_to_clickhouse():
    # 1. Read embeddings from graph in batches
    # 2. Transform to ClickHouse format
    # 3. Bulk insert with progress tracking
    # 4. Verify data integrity
```

### 2. Rollout Plan
1. **Dual-write phase**: Write to both systems
2. **Shadow reads**: Compare results from both systems
3. **Gradual migration**: Route % of traffic to new system
4. **Full cutover**: Remove graph embedding storage

## Performance Optimizations

### 1. ClickHouse Optimizations
- **Batch insertions**: Group embedding updates
- **Async indexing**: Update ANNOY indexes periodically
- **Query caching**: Cache frequent similarity searches
- **Compression**: Use appropriate codecs for embeddings

### 2. Query Optimizations
- **Pre-filtering**: Use asset/time filters before vector search
- **Approximate search**: Use ANNOY's n_trees parameter
- **Result limiting**: Implement early termination
- **Parallel queries**: Search multiple embedding types concurrently

## Monitoring and Observability

### 1. Metrics to Track
- Embedding calculation time
- ClickHouse insertion latency
- Vector search query time
- Result quality (similarity score distribution)
- Index memory usage

### 2. Logging
- Embedding updates
- Search queries and results
- Performance anomalies
- Data consistency checks

## Benefits of This Architecture

1. **Scalability**: ClickHouse handles billions of embeddings efficiently
2. **Performance**: ANNOY indexes provide sub-second similarity search
3. **Flexibility**: Easy to add new embedding types or dimensions
4. **Analytics**: Leverage ClickHouse for embedding analysis
5. **Cost**: Reduced memory usage in graph database
6. **Maintenance**: Separate concerns between graph and embeddings

## Example Queries

### 1. Basic Similarity Search
```sql
SELECT 
    address,
    cosineDistance(embedding, [0.1, 0.2, ...]) as distance,
    1 - distance as similarity
FROM address_embeddings
WHERE 
    asset = 'TAO' 
    AND embedding_type = 'financial'
ORDER BY distance ASC
LIMIT 10
```

### 2. Time-Filtered Search
```sql
SELECT 
    address,
    cosineDistance(embedding, ?) as distance
FROM address_embeddings
WHERE 
    asset = ? 
    AND embedding_type = ?
    AND last_seen_timestamp > now() - INTERVAL 7 DAY
ORDER BY distance ASC
LIMIT ?
```

### 3. Multi-Asset Comparison
```sql
WITH similar_addresses AS (
    SELECT 
        address,
        asset,
        cosineDistance(embedding, ?) as distance
    FROM address_embeddings
    WHERE embedding_type = 'joint'
    ORDER BY distance ASC
    LIMIT 100
)
SELECT 
    address,
    groupArray(asset) as assets,
    avg(distance) as avg_distance
FROM similar_addresses
GROUP BY address
HAVING length(assets) > 1
ORDER BY avg_distance ASC
```

## Next Steps

1. **Review and approve this plan**
2. **Create ClickHouse schema migrations**
3. **Implement ClickHouseClient service**
4. **Update indexer for dual-write**
5. **Modify similarity search service**
6. **Test and benchmark**
7. **Plan rollout strategy**