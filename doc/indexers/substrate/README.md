# Substrate Indexers

## Overview

The Substrate Indexers ecosystem provides a comprehensive solution for capturing, processing, and analyzing blockchain data from Substrate-based networks. This modular system consists of five specialized indexers that work together to transform raw blockchain data into structured, queryable information for various analytical purposes.

The ecosystem follows a layered architecture where each indexer builds upon the foundation provided by others, creating a rich data pipeline that enables everything from basic blockchain exploration to advanced network analysis.

## Indexers

### [Block Stream Indexer](./block_stream.md)

**Purpose**: Serves as the foundation for all other indexers by capturing comprehensive blockchain data.

**Key Features**:
- Comprehensive data capture of blocks, transactions, events, and addresses
- Nested data structures for efficient storage and querying
- Partition-based architecture for scalable processing
- Flexible query capabilities for accessing blockchain data

### [Balance Transfers Indexer](./balance_transfers.md)

**Purpose**: Tracks individual transfer transactions between addresses to analyze transaction patterns and network activity.

**Key Features**:
- Asset-agnostic design with universal histogram bins
- Multi-level time aggregation (4-hour, daily, weekly, monthly)
- Optimized materialized views for efficient querying
- Address behavior profiling for identifying different types of network participants

### [Balance Series Indexer](./balance_series.md)

**Purpose**: Monitors account balance changes over time to track wealth distribution and account activity.

**Key Features**:
- Time-series tracking with fixed 4-hour interval snapshots
- Multi-balance type support (free, reserved, staked, total)
- Change tracking between periods with absolute and percentage metrics
- Multi-level time aggregation for temporal analysis

### [Known Addresses Indexer](./known_addresses.md)

**Purpose**: Maps cryptographic addresses to human-readable labels and metadata to improve data readability.

**Key Features**:
- Address labeling for improved readability and identification
- Metadata management including source tracking and categorization
- Versioned updates to maintain data consistency
- External data integration from repository sources

### [Money Flow Indexer](./money_flow.md)

**Purpose**: Implements a graph-based approach to analyzing transaction networks and fund flows.

**Key Features**:
- Transaction network mapping with addresses as nodes and transactions as edges
- Advanced network analysis (community detection, PageRank, path finding)
- Address profiling based on transaction patterns
- Vector embeddings for similarity search and clustering

## How the Indexers Work Together

The Substrate Indexers form an integrated ecosystem where data flows from raw blockchain events to specialized analytical structures:

1. **Data Capture Layer**: The Block Stream Indexer serves as the entry point, capturing raw blockchain data from Substrate nodes and storing it in a structured format.

2. **Specialized Processing Layer**: Three specialized indexers consume and transform the block stream data:
   - Balance Transfers Indexer extracts and analyzes transfer events
   - Balance Series Indexer tracks balance changes over time
   - Money Flow Indexer builds a transaction graph for network analysis

3. **Enrichment Layer**: The Known Addresses Indexer enhances all other indexers by providing human-readable labels for cryptographic addresses, improving the readability and usability of the data.

4. **API Layer**: Each indexer exposes its data through specialized API endpoints, enabling applications to access the processed data for various use cases.

## Data Flow Between Indexers

```
┌─────────────────┐
│  Substrate Node │
└────────┬────────┘
         │ Raw Blockchain Data
         ▼
┌─────────────────┐
│  Block Stream   │◄───┐
│    Indexer      │    │
└────────┬────────┘    │ Address
         │             │ Labels
         ├─────────────┘
         │
         ├─────────────┐
         │             │
         ▼             ▼
┌─────────────────┐ ┌─────────────────┐
│ Balance Series  │ │Balance Transfers│
│    Indexer      │ │    Indexer      │
└────────┬────────┘ └────────┬────────┘
         │                    │
         │                    │
         │                    ▼
         │          ┌─────────────────┐
         │          │   Money Flow    │
         │          │    Indexer      │
         │          └────────┬────────┘
         │                   │
         ▼                   ▼
┌──────────────────────────────────────┐
│              API Layer               │
└──────────────────────────────────────┘
         │                   │
         ▼                   ▼
┌──────────────────────────────────────┐
│           Applications               │
└──────────────────────────────────────┘
```

### Key Data Flows:

1. **Block Stream → Balance Transfers**: The Block Stream Indexer provides transaction and event data that the Balance Transfers Indexer uses to extract and analyze transfer events.

2. **Block Stream → Balance Series**: The Block Stream Indexer provides block and event data that the Balance Series Indexer uses to track account balance changes over time.

3. **Block Stream → Money Flow**: The Block Stream Indexer provides transaction data that the Money Flow Indexer uses to build a graph representation of the transaction network.

4. **Known Addresses → All Indexers**: The Known Addresses Indexer provides human-readable labels that enhance the data from all other indexers, making it more accessible and meaningful.

5. **Balance Transfers → Money Flow**: While the Money Flow Indexer primarily consumes data from the Block Stream Indexer, it can also leverage the aggregated transfer data from the Balance Transfers Indexer for additional insights.

## Use Cases

The Substrate Indexers ecosystem supports a wide range of analytical use cases:

1. **Blockchain Exploration**: Navigate through blocks, transactions, and events with the Block Stream Indexer.

2. **Transaction Analysis**: Analyze transfer patterns, volumes, and participant behavior with the Balance Transfers Indexer.

3. **Balance Monitoring**: Track account balance changes and identify significant fluctuations with the Balance Series Indexer.

4. **Entity Recognition**: Identify and label significant addresses (exchanges, whales, etc.) with the Known Addresses Indexer.

5. **Network Analysis**: Visualize transaction networks, detect communities, and identify important addresses with the Money Flow Indexer.

6. **Forensic Investigation**: Trace fund flows between addresses and identify suspicious patterns with the combined capabilities of all indexers.

7. **Economic Research**: Analyze wealth distribution, transaction volumes, and network activity for economic research purposes.

## Conclusion

The Substrate Indexers ecosystem provides a powerful and flexible framework for blockchain data analysis. By breaking down the complex task of blockchain data processing into specialized components, it enables efficient and targeted analysis while maintaining a cohesive overall system. Each indexer contributes unique capabilities to the ecosystem, creating a comprehensive solution for blockchain data analytics.