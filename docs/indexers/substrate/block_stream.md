# Block Stream Indexer

## Overview

The Block Stream indexer provides a comprehensive solution for storing and querying blockchain data from Substrate networks. It serves as the foundation for other specialized indexers by capturing the complete blockchain state with the following key features:

- **Comprehensive data capture** of blocks, transactions, events, and addresses
- **Nested data structures** for efficient storage and querying of related data
- **Partition-based architecture** for scalable processing of large blockchains
- **Flexible query capabilities** for accessing blockchain data by various criteria
- **Real-time and historical** data access patterns

## Core Table Structure

The foundation of the indexer is the `block_stream` table:

```sql
CREATE TABLE IF NOT EXISTS block_stream (
    -- Block information
    block_height UInt64,
    block_hash String,
    block_timestamp UInt64,

    -- Nested structure for transactions (extrinsics)
    transactions Nested (
        extrinsic_id String,      -- Format: {block_height}-{index}
        extrinsic_hash String,    -- Transaction hash
        signer String,            -- Address of the transaction signer
        call_module String,       -- Module name of the call
        call_function String,     -- Function name of the call
        status String             -- Transaction status: Success/Failed
    ),

    -- Array of all addresses involved in this block
    addresses Array(String),

    -- Nested structure for events
    events Nested (
        event_idx String,         -- Format: {block_height}-{index}
        extrinsic_id String,      -- Reference to the extrinsic that triggered this event
        module_id String,         -- Module that emitted the event
        event_id String,          -- Event name
        attributes String         -- JSON-serialized event attributes
    ),

    -- Version for ReplacingMergeTree
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY block_height
PARTITION BY intDiv(block_height, {partition_size});
```

### Key Fields

#### Block Information
- **block_height**: The height/number of the block in the blockchain
- **block_hash**: Unique identifier hash for the block
- **block_timestamp**: Unix timestamp when the block was created

#### Transaction Structure
- **extrinsic_id**: Unique identifier for the transaction (format: `{block_height}-{index}`)
- **extrinsic_hash**: Cryptographic hash of the transaction
- **signer**: Address that signed and submitted the transaction
- **call_module**: The module being called (e.g., "Balances", "Staking")
- **call_function**: The specific function being called (e.g., "transfer", "bond")
- **status**: Transaction execution status ("Success" or "Failed")

#### Address Tracking
- **addresses**: Array of all addresses involved in the block's transactions and events

#### Event Structure
- **event_idx**: Unique identifier for the event (format: `{block_height}-{index}`)
- **extrinsic_id**: Reference to the transaction that triggered this event
- **module_id**: Module that emitted the event
- **event_id**: Type of event emitted
- **attributes**: JSON-serialized event parameters and data

#### Versioning
- **_version**: Used for update management with ReplacingMergeTree engine

## Data Partitioning and Organization

The Block Stream indexer uses a partition-based architecture to efficiently manage large blockchain datasets:

### Partition Strategy
- Blocks are partitioned by height using the formula `intDiv(block_height, partition_size)`
- Each partition contains a fixed number of blocks (defined by `partition_size`)
- This enables parallel processing and efficient querying of specific block ranges

### Partition Management
- The system tracks the progress of each partition independently
- Partitions can be in various states: `completed`, `not_started`, `incomplete`, or `incomplete_with_gaps`
- This allows for targeted reindexing of specific block ranges when needed

### Data Organization
- Block data is stored with nested structures for transactions and events
- This approach maintains the hierarchical relationship between blocks, transactions, and events
- The nested structure enables efficient storage while preserving the ability to query at any level

## Nested Structures for Blockchain Data

The schema uses nested data structures to efficiently represent the hierarchical nature of blockchain data:

### Transaction (Extrinsic) Structure
Transactions are stored as nested columns within each block record, preserving their relationship to the parent block while enabling efficient queries:

```
block → transactions[
    {extrinsic_id, extrinsic_hash, signer, call_module, call_function, status}
]
```

### Event Structure
Events are similarly stored as nested columns, maintaining their relationship to both blocks and transactions:

```
block → events[
    {event_idx, extrinsic_id, module_id, event_id, attributes}
]
```

This nested approach provides several benefits:
- **Storage efficiency**: Related data is stored together
- **Query performance**: Enables efficient filtering and aggregation
- **Relationship preservation**: Maintains the blockchain's hierarchical structure
- **Flexible access patterns**: Supports queries at block, transaction, or event level

## Common Query Patterns

### Querying Blocks by Height Range

```sql
-- Get all blocks between heights 1000000 and 1000010
SELECT
    block_height,
    block_hash,
    block_timestamp
FROM block_stream
WHERE block_height >= 1000000 AND block_height <= 1000010
ORDER BY block_height
```

### Querying Transactions by Module and Function

```sql
-- Find all balance transfer transactions
SELECT
    block_height,
    block_timestamp,
    transactions.extrinsic_id,
    transactions.signer,
    transactions.status
FROM block_stream
ARRAY JOIN transactions
WHERE transactions.call_module = 'Balances' 
  AND transactions.call_function = 'transfer'
ORDER BY block_height DESC
LIMIT 100
```

### Finding Events by Type

```sql
-- Find all staking reward events
SELECT
    block_height,
    block_timestamp,
    events.event_idx,
    events.attributes
FROM block_stream
ARRAY JOIN events
WHERE events.module_id = 'Staking' 
  AND events.event_id = 'Reward'
ORDER BY block_height DESC
LIMIT 100
```

### Tracking Address Activity

```sql
-- Find all blocks where a specific address was involved
SELECT
    block_height,
    block_timestamp
FROM block_stream
WHERE hasAny(addresses, ['5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'])
ORDER BY block_height DESC
LIMIT 100
```

### Time-Based Queries

```sql
-- Get blocks from a specific time period
SELECT
    block_height,
    block_hash,
    block_timestamp
FROM block_stream
WHERE block_timestamp >= 1609459200 -- 2021-01-01 00:00:00
  AND block_timestamp <= 1612137600 -- 2021-02-01 00:00:00
ORDER BY block_timestamp
```

## Integration with Other Indexers

The Block Stream indexer serves as the foundation for other specialized indexers in the system:

1. **Balance Transfers Indexer**: Uses block stream data to track token transfers between addresses
2. **Balance Series Indexer**: Builds on block stream data to track account balance changes over time
3. **Money Flow Indexer**: Analyzes transaction patterns from the block stream to identify fund flows

These specialized indexers consume data from the Block Stream indexer, extracting and transforming it for specific analytical purposes while maintaining a consistent view of the blockchain state.

## Implementation Details

### Data Processing Flow

1. **Block Retrieval**: Raw blocks are fetched from the Substrate node
2. **Data Extraction**: Relevant data is extracted from blocks, transactions, and events
3. **Address Identification**: Addresses are extracted from transaction signers and event attributes
4. **Data Transformation**: Data is transformed into the nested structure format
5. **Storage**: Processed data is stored in the ClickHouse database using the ReplacingMergeTree engine

### Versioning Strategy

The schema uses a versioning approach with the ReplacingMergeTree engine:
- Each record includes a `_version` field based on the insertion timestamp
- When reindexing occurs, newer versions replace older ones
- This ensures data consistency while allowing for reprocessing when needed

### Partition Management

The system includes tools for managing partitions:
- **Partition Progress Tracking**: Monitors the indexing status of each partition
- **Gap Detection**: Identifies missing blocks within partially indexed partitions
- **Targeted Reindexing**: Allows for reindexing specific partitions or block ranges

## Usage Examples

### Basic Block Retrieval

```python
# Get blocks by height range
blocks = block_stream_manager.get_blocks_by_block_height_range(1000000, 1000010)

# Process block data
for block in blocks:
    block_height = block['block_height']
    transactions = block['extrinsics']
    events = block['events']
    # Process block data...
```

### Event Analysis

```python
# Find specific events and analyze their attributes
query = """
SELECT
    block_height,
    block_timestamp,
    events.event_idx,
    events.module_id,
    events.event_id,
    events.attributes
FROM block_stream
ARRAY JOIN events
WHERE events.module_id = 'Balances' 
  AND events.event_id = 'Transfer'
ORDER BY block_height DESC
LIMIT 1000
"""
result = client.query(query)

# Process transfer events
for row in result.result_rows:
    attributes = json.loads(row[5])
    from_address = attributes.get('from')
    to_address = attributes.get('to')
    amount = attributes.get('amount')
    # Analyze transfer data...
```

### Address Activity Monitoring

```python
# Monitor activity for a specific address
address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
query = f"""
SELECT
    block_height,
    block_timestamp,
    transactions.extrinsic_id,
    transactions.call_module,
    transactions.call_function
FROM block_stream
ARRAY JOIN transactions
WHERE transactions.signer = '{address}'
ORDER BY block_height DESC
LIMIT 100
"""
result = client.query(query)

# Analyze address activity
for row in result.result_rows:
    # Process activity data...