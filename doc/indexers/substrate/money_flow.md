# Money Flow Schema Documentation

## Overview

The Money Flow indexer implements a graph-based approach to tracking and analyzing cryptocurrency transactions across different blockchain networks. It consumes data from the Block Stream indexer and unlike traditional relational database approaches, the graph model represents blockchain addresses as nodes and transactions as directed edges, enabling powerful network analysis capabilities.

This document provides technical details about the Money Flow schema, its implementation, and how to effectively query and analyze transaction networks.

## Purpose and Approach

The Money Flow indexer serves several key purposes:

1. **Transaction Network Mapping**: Creates a comprehensive graph of all transactions between addresses, enabling visualization and analysis of money flows.

2. **Network Analysis**: Supports advanced graph algorithms like community detection, PageRank, and path finding to identify important addresses and communities within the network.

3. **Address Profiling**: Builds rich profiles of addresses based on their transaction patterns, including metrics like transfer counts, unique senders/receivers, and community membership.

4. **Cross-Network Support**: Provides implementations for multiple Substrate-based networks (Torus, Bittensor, Polkadot) with network-specific event handling.

5. **Vector Embeddings**: Generates embeddings for addresses based on their network characteristics, enabling similarity search and clustering.

## Core Graph Data Model

### Nodes

| Node Type | Description | Key Properties |
|-----------|-------------|----------------|
| `Address` | Represents a blockchain address | `address`, `transfer_count`, `neighbor_count`, `unique_senders`, `unique_receivers`, `first_activity_timestamp`, `last_activity_timestamp`, `community_id`, `community_page_rank`, `network_embedding` |
| `Community` | Represents a detected community of addresses | `community_id` |
| `GlobalState` | Tracks indexing state | `name`, `block_height` |
| `Agent` (Torus) | Represents a registered agent on Torus | `address`, `labels` |
| `Neuron` (Bittensor) | Represents a registered neuron on Bittensor | `network_id`, `neuron_id`, `owner_address` |
| `Subnet` (Bittensor) | Represents a subnet on Bittensor | `network_id`, `created_timestamp`, `label` |
| `Genesis` | Represents addresses with genesis balances | `address`, `labels` |

### Relationships

| Relationship Type | Description | Key Properties |
|-------------------|-------------|----------------|
| `TO` | Represents a transfer from one address to another | `id`, `asset`, `volume`, `transfer_count`, `first_activity_timestamp`, `last_activity_timestamp` |
| `OWNS` (Bittensor) | Connects an address to a neuron it owns | `last_updated_timestamp` |
| `CREATED` (Bittensor) | Connects an address to a subnet it created | `timestamp` |

### Indexes

The schema includes various indexes to optimize query performance:

1. **Node Property Indexes**: For properties like `address`, `labels`, `transfer_count`, `community_id`, etc.
2. **Edge Property Indexes**: For properties like `id`, `asset`, `volume`, `transfer_count`, etc.
3. **Vector Indexes**: For the `network_embedding` property to enable efficient similarity search.

## Network-Specific Implementations

The Money Flow indexer provides specialized implementations for different Substrate-based networks:

### Torus

The Torus implementation:
- Processes Torus-specific events like `AgentRegistered`
- Initializes genesis balances from a JSON file
- Labels addresses as agents when they register

```python
# Example of Torus-specific event processing
def _process_agent_registered_events(self, transaction, timestamp, events):
    for event in events:
        agent = event['attributes']
        query = """
        MERGE (agent:Address { address: $agent })
        SET agent:Agent,
            agent.labels = coalesce(agent.labels, []) + ['agent']
        """
        transaction.run(query, {
            'agent': agent,
            'asset': self.asset
        })
```

### Bittensor

The Bittensor implementation:
- Processes Bittensor-specific events like `NeuronRegistered` and `NetworkAdded`
- Creates additional node types like `Neuron` and `Subnet`
- Establishes relationships like `OWNS` (between addresses and neurons) and `CREATED` (between addresses and subnets)
- Tracks subnet and neuron ownership

```python
# Example of Bittensor-specific event processing
def _process_neuron_registered_events(self, transaction, timestamp, events):
    for event in events:
        network_id = event['attributes'][0]
        neuron_id = event['attributes'][1]
        owner_address = event['attributes'][2]
        
        # Create or update the owner address node with neuron owner label
        query = """
        MERGE (addr:Address { address: $owner_address })
        ON CREATE SET
            addr.labels = ['neuron_owner', 'neuron_owner_sn' + $network_id],
            addr.first_seen_timestamp = $timestamp,
            addr.subnets = [$network_id],
            addr.neurons = [{ network_id: $network_id, neuron_id: $neuron_id }]
        # ... additional SET operations ...
        """
        # ... transaction execution ...
        
        # Create or update the neuron node
        # ... additional queries ...
        
        # Create relationship between owner and neuron
        # ... additional queries ...
```

### Polkadot

The Polkadot implementation is currently a placeholder for future development:
- Inherits from the base implementation
- Will eventually handle Polkadot-specific events like staking and nomination

## Analytics Capabilities

The Money Flow schema supports several advanced analytics capabilities:

### 1. Community Detection

Identifies clusters of addresses that frequently interact with each other:

```python
def community_detection(self):
    query = """
        MATCH (source:Address)-[r:TO]->(target:Address)
        WITH collect(DISTINCT source) + collect(DISTINCT target) AS nodes, collect(DISTINCT r) AS relationships
        CALL community_detection.get_subgraph(nodes, relationships)
        YIELD node, community_id
        SET node.community_id = community_id
        WITH DISTINCT community_id
        WHERE community_id IS NOT NULL
        MERGE (c:Community { community_id: community_id });
    """
    # ... execution code ...
```

### 2. PageRank

Calculates importance scores for addresses within communities:

```python
def page_rank_with_community(self):
    # ... code to get communities ...
    
    for community in communities:
        subgraph_query = f"""
            MATCH p=(a1:Address {{community_id: {community!r}}})-[r:TO*1..3]->(a2:Address)
            WITH project(p) AS community_graph
            CALL pagerank.get(community_graph) YIELD node, rank
            SET node.community_page_rank = rank
        """
        # ... execution code ...
```

### 3. Vector Embeddings

Creates vector embeddings for addresses based on network metrics:

```python
def update_embeddings(self, addresses: Optional[List[str]] = None):
    base_query = """
    MATCH (a:Address)
    {address_filter}
    SET
    a.network_embedding = [
        coalesce(a.transfer_count, 0),                        // Total number of transfers in and out
        coalesce(a.unique_senders, 0),                         // Number of unique addresses that sent to this address
        coalesce(a.unique_receivers, 0),                        // Number of unique addresses this address sent to
        coalesce(a.neighbor_count, 0),                        // Number of neighbors (connected addresses)
        coalesce(a.community_id, 0),                           // Community membership
        coalesce(a.community_page_rank, 0)                              // Community PageRank score
    ]
    """
    # ... execution code ...
```

## Data Processing and Indexing

### Processing Flow

1. **Block Fetching**: The `MoneyFlowConsumer` fetches blocks with address interactions from the block stream.
2. **Event Grouping**: Events are grouped by type (e.g., `Balances.Transfer`, `Balances.Endowed`).
3. **Common Event Processing**: Common events like transfers and endowments are processed by the base class.
4. **Network-Specific Processing**: Network-specific events are handled by the appropriate subclass.
5. **Periodic Analytics**: Community detection, PageRank calculation, and embedding generation are performed periodically.

### Indexing Process

```python
def index_block(self, session, block):
    # Group events by type
    events_by_type = self._group_events(block.get('events', []))
    block_height = block.get('block_height')
    timestamp = block.get('timestamp')
    
    # Process common events for all networks
    self._process_endowed_events(transaction, timestamp, events_by_type.get('Balances.Endowed', []))
    self._process_transfer_events(transaction, timestamp, events_by_type.get('Balances.Transfer', []))
    
    # Process network-specific events
    self._process_network_specific_events(transaction, timestamp, events_by_type)
```

### Transfer Processing and Aggregation

The core of the Money Flow indexer is the processing and aggregation of transfer events:

```python
def _process_transfer_events(self, transaction, timestamp, events):
    for event in events:
        attrs = event['attributes']
        amount = float(convert_to_decimal_units(attrs['amount'], self.network))
        
        query = """
        MERGE (sender:Address { address: $from })
          ON CREATE SET
            sender.first_activity_timestamp = $timestamp,
            sender.last_activity_timestamp = $timestamp,
            sender.first_activity_block_height = $block_height,
            sender.last_activity_block_height = $block_height,
            sender.transfer_count = 1
          SET
            sender.last_activity_timestamp = $timestamp,
            sender.last_activity_block_height = $block_height,
            sender.transfer_count = coalesce(sender.transfer_count, 0) + 1
              
        MERGE (receiver:Address { address: $to })
          ON CREATE SET
            receiver.first_activity_timestamp = $timestamp,
            receiver.last_activity_timestamp = $timestamp,
            receiver.first_activity_block_height = $block_height,
            receiver.last_activity_block_height = $block_height,
            receiver.transfer_count = 1
          SET
            receiver.last_activity_timestamp = $timestamp,
            receiver.last_activity_block_height = $block_height,
            receiver.transfer_count = coalesce(receiver.transfer_count, 0) + 1

        MERGE (sender)-[r:TO { id: $to_id, asset: $asset }]->(receiver)
          ON CREATE SET
              r.volume = $amount,
              r.transfer_count = 1,
              r.first_activity_timestamp = $timestamp,
              r.last_activity_timestamp = $timestamp,
              
              r.first_activity_block_height = $block_height,
              r.last_activity_block_height = $block_height,
              
              sender.neighbor_count = coalesce(sender.neighbor_count, 0) + 1,
              sender.unique_receivers = coalesce(sender.unique_receivers, 0) + 1,
              
              receiver.neighbor_count = coalesce(receiver.neighbor_count, 0) + 1,
              receiver.unique_senders = coalesce(receiver.unique_senders, 0) + 1
              
          ON MATCH SET
              r.volume = r.volume + $amount,
              r.transfer_count = r.transfer_count + 1,
              r.last_activity_timestamp = $timestamp,
              r.last_activity_block_height = $block_height
        """
        # ... execution code ...
```

**Key Aggregation Mechanism**: The code above demonstrates how the Money Flow indexer aggregates transactions:

1. When a transfer occurs between two addresses, the indexer first ensures both addresses exist as nodes in the graph.
2. Then it looks for an existing relationship (edge) between these addresses using the `MERGE` operation.
3. If this is the first transfer between these addresses (`ON CREATE SET`), it creates a new relationship with initial values.
4. If a relationship already exists (`ON MATCH SET`), it **updates the existing relationship** by:
   - Adding the new amount to the total volume (`r.volume = r.volume + $amount`)
   - Incrementing the transfer count (`r.transfer_count = r.transfer_count + 1`)
   - Updating the timestamp of the last activity

This aggregation approach means that no matter how many transfers occur between the same two addresses, there will only be a single edge in the graph representing their relationship, with accumulated metrics.

## Comparison with Balance Transfers Indexer

While both the Money Flow and Balance Transfers indexers track cryptocurrency transactions, they serve different purposes and use different approaches:

| Feature | Money Flow Indexer | Balance Transfers Indexer |
|---------|-------------------|--------------------------|
| **Storage Model** | Graph database (Neo4j) | Relational database (ClickHouse) |
| **Transaction Storage** | Aggregates transactions between the same addresses | Stores each individual transaction separately |
| **Primary Focus** | Network structure and relationships | Temporal patterns and statistical analysis |
| **Edge Representation** | One edge per address pair with accumulated metrics | N/A (uses relational tables) |
| **Query Capabilities** | Path finding, community detection, network analysis | Time-series analysis, volume trends, address profiling |

This fundamental difference in design makes the Money Flow indexer ideal for analyzing the structure of transaction networks, while the Balance Transfers indexer excels at temporal analysis and individual transaction history.

## Common Query Patterns

### 1. Shortest Path Between Addresses

Find the shortest path between two addresses (each edge represents an aggregated flow of funds):

```cypher
MATCH path = (start:Address {address: 'SOURCE_ADDRESS'})-[rels:TO*BFS]->(target:Address {address: 'TARGET_ADDRESS'})
WHERE ALL(rel IN rels WHERE rel.asset = 'TOR')
RETURN path
```

### 2. Explore Address Connections

Explore connections from a set of addresses with depth and direction control:

```cypher
MATCH (a:Address) WHERE a.address IN ['ADDRESS1', 'ADDRESS2']
CALL path.expand(a, ['TO>'], [], 0, 3) YIELD result as path
WHERE ALL(rel IN relationships(path) WHERE rel.asset = 'TOR')
RETURN path
```

### 3. Community Analysis

Analyze addresses within a specific community:

```cypher
MATCH (a:Address {community_id: 5})
RETURN a.address, a.community_page_rank
ORDER BY a.community_page_rank DESC
LIMIT 10
```

### 4. High-Value Transfer Relationships

Find high-value aggregated transfer relationships:

```cypher
MATCH (sender:Address)-[r:TO]->(receiver:Address)
WHERE r.asset = 'TOR' AND r.volume > 1000
RETURN sender.address, receiver.address, r.volume, r.transfer_count, r.volume / r.transfer_count as avg_transfer
ORDER BY r.volume DESC
LIMIT 20
```

This query returns address pairs with high total volume, showing the aggregated volume, number of transfers, and average transfer amount between them.

### 5. Address Similarity Search

Find addresses with similar network characteristics:

```cypher
MATCH (a:Address {address: 'TARGET_ADDRESS'})
CALL vector_search.find_similar_nodes('Address', 'network_embedding', a.network_embedding, 10)
YIELD node, score
RETURN node.address, score
```

## Conclusion

The Money Flow schema provides a powerful graph-based approach to analyzing cryptocurrency transactions. By modeling addresses as nodes and aggregated transfers as edges, it enables advanced network analysis capabilities like path finding, community detection, and PageRank scoring.

The key advantage of this aggregation approach is that it creates a more concise and meaningful representation of the transaction network, focusing on the relationships between addresses rather than individual transactions. This makes complex network analyses more efficient and reveals patterns that might be obscured in transaction-by-transaction analysis.

The schema supports multiple Substrate-based networks with network-specific event handling, making it a versatile tool for blockchain analytics.