# Complete Hybrid Architecture Implementation Plan

## Overview
This plan details all changes needed to implement the hybrid Graph + ClickHouse architecture for multi-asset money flow tracking, starting from a clean state.

## Architecture Summary
- **Graph (Memgraph)**: Lightweight network structure with aggregated metrics
- **ClickHouse**: Detailed transaction history, analytics, and embeddings
- **Multi-asset support**: Each edge tracks one asset, no mixing of values

## 1. Graph Database Changes

### A. Update Address Node Structure
```cypher
-- Address nodes will store only:
{
  address: String,
  first_seen_timestamp: Int,
  first_seen_block: Int,
  last_seen_timestamp: Int,
  last_seen_block: Int,
  neighbor_count: Int,
  community_id: Int,
  community_page_rank: Float,
  fraud: Int
}
-- Remove: volume_in, volume_out, transfer_count, all embeddings
```

### B. Update Edge Structure
```cypher
-- TO edges will store:
{
  id: String,  -- "from-{from}-to-{to}-{asset}"
  asset: String,  -- Asset type (TOR, TAO, DOT)
  volume: Float,  -- Total volume for this asset
  transfer_count: Int,  -- Number of transfers
  first_transfer_timestamp: Int,
  first_transfer_block: Int,
  last_transfer_timestamp: Int,
  last_transfer_block: Int
}
-- Remove: min_amount, max_amount, tx_frequency arrays
```

### C. Update Indexes
```cypher
-- Address indexes
CREATE INDEX ON :Address(address);
CREATE INDEX ON :Address(first_seen_timestamp);
CREATE INDEX ON :Address(last_seen_timestamp);
CREATE INDEX ON :Address(community_id);

-- Edge indexes
CREATE EDGE INDEX ON :TO(id);
CREATE EDGE INDEX ON :TO(asset);
CREATE EDGE INDEX ON :TO(volume);
CREATE EDGE INDEX ON :TO(last_transfer_timestamp);

-- Remove all vector indexes
```

## 2. ClickHouse Schema

### A. Core Tables

```sql
-- 1. Detailed transfer history
CREATE TABLE transfers (
    from_address String,
    to_address String,
    asset String,
    amount Float64,
    block_height UInt64,
    timestamp DateTime64(3),
    transaction_hash String,
    
    -- Indexes
    INDEX idx_from_addr from_address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_to_addr to_address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_asset asset TYPE minmax GRANULARITY 1,
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (asset, timestamp, from_address, to_address);

-- 2. Address embeddings
CREATE TABLE address_embeddings (
    address String,
    asset String,
    embedding_type Enum('financial', 'temporal', 'network', 'joint'),
    embedding Array(Float32),
    
    -- Denormalized metrics for filtering
    volume_in Float64,
    volume_out Float64,
    transfer_count UInt64,
    unique_senders UInt32,
    unique_receivers UInt32,
    
    updated_at DateTime64(3) DEFAULT now64(3),
    
    PRIMARY KEY (address, asset, embedding_type)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (asset, embedding_type, address);

-- 3. Address metrics (for fast lookups)
CREATE TABLE address_metrics (
    address String,
    asset String,
    
    -- Aggregated volumes
    volume_in Float64,
    volume_out Float64,
    volume_differential Float64,
    
    -- Transfer statistics
    transfer_count UInt64,
    unique_senders UInt32,
    unique_receivers UInt32,
    
    -- Temporal metrics
    first_seen_timestamp DateTime64(3),
    last_seen_timestamp DateTime64(3),
    avg_incoming_frequency Float32,
    avg_outgoing_frequency Float32,
    
    updated_at DateTime64(3) DEFAULT now64(3),
    
    PRIMARY KEY (address, asset)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (asset, address);
```

### B. Materialized Views

```sql
-- 1. Transfer frequency analysis
CREATE MATERIALIZED VIEW transfer_frequencies_mv
ENGINE = AggregatingMergeTree()
ORDER BY (asset, from_address, to_address, toStartOfHour(timestamp))
AS SELECT
    asset,
    from_address,
    to_address,
    toStartOfHour(timestamp) as hour,
    count() as transfer_count,
    sum(amount) as total_volume,
    avg(amount) as avg_amount,
    min(timestamp) as first_transfer,
    max(timestamp) as last_transfer
FROM transfers
GROUP BY asset, from_address, to_address, hour;

-- 2. Address activity summary
CREATE MATERIALIZED VIEW address_activity_mv
ENGINE = SummingMergeTree()
ORDER BY (asset, address, toStartOfDay(timestamp))
AS SELECT
    asset,
    address,
    toStartOfDay(timestamp) as day,
    sumIf(amount, direction = 'in') as daily_volume_in,
    sumIf(amount, direction = 'out') as daily_volume_out,
    countIf(direction = 'in') as daily_transfers_in,
    countIf(direction = 'out') as daily_transfers_out
FROM (
    SELECT asset, to_address as address, amount, timestamp, 'in' as direction FROM transfers
    UNION ALL
    SELECT asset, from_address as address, amount, timestamp, 'out' as direction FROM transfers
)
GROUP BY asset, address, day;

-- 3. ANNOY indexes for similarity search
ALTER TABLE address_embeddings 
ADD INDEX embedding_idx embedding TYPE annoy(100, 'cosine') GRANULARITY 1000;
```

## 3. Code Changes

### A. Update money_flow_indexer.py

```python
class BaseMoneyFlowIndexer:
    def __init__(self, graph_database: Driver, clickhouse_client: ClickHouseClient, network: str):
        self.graph_database = graph_database
        self.clickhouse = clickhouse_client
        self.network = network
        self.asset = get_network_asset(network)
    
    def _process_transfer_events(self, transaction, timestamp, events):
        """Process transfers with edge-based volume tracking"""
        transfers_batch = []
        
        for event in events:
            attrs = event['attributes']
            amount = float(convert_to_decimal_units(attrs['amount'], self.network))
            
            # Graph update - only timestamps and edge volumes
            query = """
            MERGE (sender:Address { address: $from })
              ON CREATE SET
                sender.first_seen_timestamp = $timestamp,
                sender.first_seen_block = $block_height
              SET sender.last_seen_timestamp = $timestamp,
                  sender.last_seen_block = $block_height
                  
            MERGE (receiver:Address { address: $to })
              ON CREATE SET
                receiver.first_seen_timestamp = $timestamp,
                receiver.first_seen_block = $block_height
              SET receiver.last_seen_timestamp = $timestamp,
                  receiver.last_seen_block = $block_height
            
            MERGE (sender)-[r:TO { id: $edge_id, asset: $asset }]->(receiver)
              ON CREATE SET
                r.volume = $amount,
                r.transfer_count = 1,
                r.first_transfer_timestamp = $timestamp,
                r.first_transfer_block = $block_height,
                r.last_transfer_timestamp = $timestamp,
                r.last_transfer_block = $block_height,
                sender.neighbor_count = coalesce(sender.neighbor_count, 0) + 1,
                receiver.neighbor_count = coalesce(receiver.neighbor_count, 0) + 1
              ON MATCH SET
                r.volume = r.volume + $amount,
                r.transfer_count = r.transfer_count + 1,
                r.last_transfer_timestamp = $timestamp,
                r.last_transfer_block = $block_height
            """
            
            transaction.run(query, {
                'block_height': event['block_height'],
                'timestamp': timestamp,
                'from': attrs['from'],
                'to': attrs['to'],
                'amount': amount,
                'asset': self.asset,
                'edge_id': f"from-{attrs['from']}-to-{attrs['to']}-{self.asset}"
            })
            
            # Prepare for ClickHouse batch insert
            transfers_batch.append({
                'from_address': attrs['from'],
                'to_address': attrs['to'],
                'asset': self.asset,
                'amount': amount,
                'block_height': event['block_height'],
                'timestamp': timestamp,
                'transaction_hash': event.get('transaction_hash', '')
            })
        
        # Batch insert to ClickHouse
        if transfers_batch:
            self.clickhouse.insert_transfers(transfers_batch)
    
    def update_embeddings(self, addresses: Optional[List[str]] = None):
        """Calculate and store embeddings in ClickHouse"""
        # Get address metrics from ClickHouse
        metrics = self.clickhouse.get_address_metrics(addresses, self.asset)
        
        # Calculate embeddings
        embeddings_batch = []
        for address, data in metrics.items():
            financial_embedding = self._calculate_financial_embedding(data)
            temporal_embedding = self._calculate_temporal_embedding(data)
            network_embedding = self._calculate_network_embedding(data)
            joint_embedding = financial_embedding + temporal_embedding + network_embedding
            
            # Prepare all embedding types for batch insert
            for emb_type, emb_vector in [
                ('financial', financial_embedding),
                ('temporal', temporal_embedding),
                ('network', network_embedding),
                ('joint', joint_embedding)
            ]:
                embeddings_batch.append({
                    'address': address,
                    'asset': self.asset,
                    'embedding_type': emb_type,
                    'embedding': emb_vector,
                    'volume_in': data['volume_in'],
                    'volume_out': data['volume_out'],
                    'transfer_count': data['transfer_count'],
                    'unique_senders': data['unique_senders'],
                    'unique_receivers': data['unique_receivers']
                })
        
        # Batch insert to ClickHouse
        if embeddings_batch:
            self.clickhouse.insert_embeddings(embeddings_batch)
```

### B. Create clickhouse_client.py

```python
import clickhouse_connect
from typing import List, Dict, Optional
from loguru import logger

class ClickHouseClient:
    def __init__(self, host: str, port: int, database: str):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database
        )
    
    def insert_transfers(self, transfers: List[Dict]):
        """Batch insert transfer records"""
        self.client.insert('transfers', transfers)
    
    def insert_embeddings(self, embeddings: List[Dict]):
        """Batch insert embeddings"""
        self.client.insert('address_embeddings', embeddings)
    
    def get_address_metrics(self, addresses: Optional[List[str]], asset: str) -> Dict:
        """Get aggregated metrics for addresses"""
        if addresses:
            query = """
            SELECT 
                address,
                sumIf(amount, direction = 'in') as volume_in,
                sumIf(amount, direction = 'out') as volume_out,
                count() as transfer_count,
                uniqIf(counterparty, direction = 'in') as unique_senders,
                uniqIf(counterparty, direction = 'out') as unique_receivers,
                min(timestamp) as first_seen,
                max(timestamp) as last_seen
            FROM (
                SELECT to_address as address, from_address as counterparty, 
                       amount, timestamp, 'in' as direction 
                FROM transfers WHERE asset = %(asset)s AND to_address IN %(addresses)s
                UNION ALL
                SELECT from_address as address, to_address as counterparty, 
                       amount, timestamp, 'out' as direction 
                FROM transfers WHERE asset = %(asset)s AND from_address IN %(addresses)s
            )
            GROUP BY address
            """
            params = {'asset': asset, 'addresses': addresses}
        else:
            # Query all addresses for the asset
            query = """
            SELECT 
                address,
                sumIf(amount, direction = 'in') as volume_in,
                sumIf(amount, direction = 'out') as volume_out,
                count() as transfer_count,
                uniqIf(counterparty, direction = 'in') as unique_senders,
                uniqIf(counterparty, direction = 'out') as unique_receivers,
                min(timestamp) as first_seen,
                max(timestamp) as last_seen
            FROM (
                SELECT to_address as address, from_address as counterparty, 
                       amount, timestamp, 'in' as direction 
                FROM transfers WHERE asset = %(asset)s
                UNION ALL
                SELECT from_address as address, to_address as counterparty, 
                       amount, timestamp, 'out' as direction 
                FROM transfers WHERE asset = %(asset)s
            )
            GROUP BY address
            """
            params = {'asset': asset}
        
        result = self.client.query(query, params)
        return {row['address']: row for row in result.result_rows}
    
    def search_similar_embeddings(self, embedding_type: str, query_vector: List[float], 
                                 asset: str, limit: int = 10, 
                                 min_similarity: float = 0.0) -> List[Dict]:
        """Search for similar addresses using ANNOY index"""
        query = """
        SELECT 
            address,
            cosineDistance(embedding, %(query_vector)s) as distance,
            1 - distance as similarity,
            volume_in,
            volume_out,
            transfer_count
        FROM address_embeddings
        WHERE 
            asset = %(asset)s 
            AND embedding_type = %(embedding_type)s
            AND similarity >= %(min_similarity)s
        ORDER BY distance ASC
        LIMIT %(limit)s
        """
        
        params = {
            'query_vector': query_vector,
            'asset': asset,
            'embedding_type': embedding_type,
            'min_similarity': min_similarity,
            'limit': limit
        }
        
        result = self.client.query(query, params)
        return result.result_rows
```

### C. Update similarity_search_service.py

```python
class SimilaritySearchService:
    def __init__(self, graph_database: Driver, clickhouse_client: ClickHouseClient):
        self.graph_database = graph_database
        self.clickhouse = clickhouse_client
    
    def find_similar_addresses(self, embedding_type: str, query_type: str, 
                             reference_address: Optional[str] = None,
                             financial_pattern: Optional[dict] = None,
                             temporal_pattern: Optional[dict] = None,
                             network_pattern: Optional[dict] = None,
                             combined_pattern: Optional[dict] = None,
                             limit: int = 10,
                             similarity_metric: str = "cosine",
                             min_similarity_score: Optional[float] = None,
                             asset: str = "TAO"):
        """Find similar addresses using ClickHouse embeddings"""
        
        # Get query vector based on query type
        if query_type == 'by_address':
            # Get embedding from ClickHouse
            query_vector = self._get_embedding_from_clickhouse(
                reference_address, embedding_type, asset
            )
        else:
            # Construct vector from pattern
            query_vector = self._construct_vector_from_pattern(
                query_type, embedding_type, financial_pattern, 
                temporal_pattern, network_pattern, combined_pattern
            )
        
        # Search in ClickHouse
        similar_addresses = self.clickhouse.search_similar_embeddings(
            embedding_type=embedding_type,
            query_vector=query_vector,
            asset=asset,
            limit=limit,
            min_similarity=min_similarity_score or 0.0
        )
        
        # Enrich with graph data
        address_list = [row['address'] for row in similar_addresses]
        enriched_data = self._enrich_from_graph(address_list, asset)
        
        # Combine results
        results = []
        for ch_row in similar_addresses:
            addr = ch_row['address']
            graph_data = enriched_data.get(addr, {})
            
            results.append({
                'id': addr,
                'type': 'node',
                'label': 'address',
                'address': addr,
                'similarity_score': ch_row['similarity'],
                
                # From ClickHouse
                'volume_in': ch_row['volume_in'],
                'volume_out': ch_row['volume_out'],
                'transfer_count': ch_row['transfer_count'],
                
                # From Graph
                'first_seen_timestamp': graph_data.get('first_seen_timestamp'),
                'last_seen_timestamp': graph_data.get('last_seen_timestamp'),
                'neighbor_count': graph_data.get('neighbor_count', 0),
                'community_id': graph_data.get('community_id', 0),
                'community_page_rank': graph_data.get('community_page_rank', 0.0),
                'badges': graph_data.get('labels', [])
            })
        
        return results
    
    def _get_embedding_from_clickhouse(self, address: str, embedding_type: str, asset: str):
        """Retrieve embedding vector from ClickHouse"""
        query = """
        SELECT embedding 
        FROM address_embeddings 
        WHERE address = %(address)s 
          AND embedding_type = %(embedding_type)s 
          AND asset = %(asset)s
        LIMIT 1
        """
        result = self.clickhouse.client.query(query, {
            'address': address,
            'embedding_type': embedding_type,
            'asset': asset
        })
        
        if not result.result_rows:
            raise ValueError(f"No {embedding_type} embedding found for address {address}")
        
        return result.result_rows[0]['embedding']
    
    def _enrich_from_graph(self, addresses: List[str], asset: str) -> Dict:
        """Get additional data from graph database"""
        with self.graph_database.session() as session:
            result = session.run("""
                MATCH (a:Address)
                WHERE a.address IN $addresses
                RETURN a.address as address,
                       a.first_seen_timestamp as first_seen_timestamp,
                       a.last_seen_timestamp as last_seen_timestamp,
                       a.neighbor_count as neighbor_count,
                       a.community_id as community_id,
                       a.community_page_rank as community_page_rank,
                       a.labels as labels
            """, {'addresses': addresses})
            
            return {record['address']: dict(record) for record in result}
```

### D. Update money_flow_service.py

```python
class MoneyFlowService:
    def __init__(self, graph_database: Driver, clickhouse_client: ClickHouseClient):
        self.graph_database = graph_database
        self.clickhouse = clickhouse_client
    
    def get_address_volumes(self, address: str, asset: Optional[str] = None):
        """Calculate volumes from edges instead of node properties"""
        with self.graph_database.session() as session:
            # Build asset filter
            asset_filter = "AND r.asset = $asset" if asset else ""
            
            query = f"""
            MATCH (a:Address {{address: $address}})
            
            // Calculate outgoing volumes
            OPTIONAL MATCH (a)-[out:TO]->()
            WHERE true {asset_filter}
            WITH a, out.asset as asset, sum(out.volume) as volume_out
            
            // Calculate incoming volumes
            OPTIONAL MATCH ()-[in:TO]->(a)
            WHERE in.asset = coalesce(asset, in.asset) {asset_filter.replace('r.', 'in.')}
            
            RETURN 
                a.address as address,
                asset,
                sum(in.volume) as volume_in,
                volume_out
            """
            
            params = {'address': address}
            if asset:
                params['asset'] = asset
                
            result = session.run(query, params)
            return result.data()
    
    def get_transfer_frequencies(self, from_address: str, to_address: str, asset: str):
        """Get detailed transfer frequency data from ClickHouse"""
        query = """
        SELECT 
            toStartOfHour(timestamp) as hour,
            count() as transfer_count,
            sum(amount) as total_volume,
            avg(amount) as avg_amount,
            min(timestamp) as first_transfer,
            max(timestamp) as last_transfer
        FROM transfers
        WHERE 
            from_address = %(from_address)s
            AND to_address = %(to_address)s
            AND asset = %(asset)s
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 100
        """
        
        result = self.clickhouse.client.query(query, {
            'from_address': from_address,
            'to_address': to_address,
            'asset': asset
        })
        
        return result.result_rows
```

## 4. API Updates

### A. Update MCP Server Configuration
```python
# packages/api/mcp_server.py
# Add ClickHouse initialization
clickhouse_client = ClickHouseClient(
    host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
    port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
    database=os.getenv('CLICKHOUSE_DATABASE', 'chainswarm')
)

# Update service initialization
similarity_service = SimilaritySearchService(graph_database, clickhouse_client)
money_flow_service = MoneyFlowService(graph_database, clickhouse_client)
```

### B. Environment Configuration
```env
# .env additions
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=chainswarm
```

## 5. Testing Strategy

### A. Unit Tests
- Test ClickHouse client methods
- Test embedding calculations
- Test similarity search logic
- Test volume calculations from edges

### B. Integration Tests
- Test full indexing flow
- Test similarity search end-to-end
- Test money flow queries
- Test multi-asset scenarios

### C. Performance Tests
- Benchmark ClickHouse insertion rates
- Test similarity search performance
- Compare with previous architecture
- Load test with millions of addresses

## 6. Deployment Steps

1. **Setup ClickHouse**
   - Install ClickHouse server
   - Create database and tables
   - Configure ANNOY indexes

2. **Deploy Code Changes**
   - Update all services
   - Configure environment
   - Run tests

3. **Start Indexing**
   - Begin from block 0
   - Monitor performance
   - Verify data integrity

4. **Enable APIs**
   - Test all endpoints
   - Monitor query performance
   - Check result quality

## Benefits of This Implementation

1. **Clean Separation**: Graph for structure, ClickHouse for analytics
2. **Multi-Asset Support**: No value mixing, clean per-asset tracking
3. **Scalability**: Can handle billions of transfers and embeddings
4. **Performance**: Fast queries with proper indexes
5. **Flexibility**: Easy to add new metrics or embedding types
6. **No Migration**: Start fresh with optimal structure