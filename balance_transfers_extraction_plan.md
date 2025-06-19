# Balance Transfers Module Extraction Implementation Plan

## Overview
This document outlines the detailed implementation plan for extracting balance_transfers indexing functionality from the existing `balance_tracking` module into a new standalone `balance_transfers` module.

## Current State Analysis

### Existing Components in balance_tracking
- **BalanceTrackingIndexerBase**: Contains transfer processing logic in `_process_events()` and `index_blocks()` methods
- **Network-specific indexers**: Each has `_process_network_specific_events()` for custom transfer types
- **BalanceTrackingConsumer**: Orchestrates the processing and calls `index_blocks()`
- **Schema**: Contains `balance_transfers` table and related views/indexes

### Transfer-Related Functionality to Extract
1. **Event Processing**: `_process_events()` method (lines 365-412 in balance_tracking_indexer_base.py)
2. **Block Indexing**: `index_blocks()` method (lines 414-468 in balance_tracking_indexer_base.py)
3. **Helper Methods**: `_validate_event_structure()`, `_group_events()`
4. **Network-Specific Processing**: Transfer-related parts of `_process_network_specific_events()`
5. **Database Schema**: `balance_transfers` table and related views

## Implementation Steps

### Step 1: Create Module Structure
```
packages/indexers/substrate/balance_transfers/
├── __init__.py ✓
├── schema.sql
├── balance_transfers_indexer_base.py
├── balance_transfers_indexer.py
├── balance_transfers_indexer_bittensor.py
├── balance_transfers_indexer_polkadot.py
├── balance_transfers_indexer_torus.py
└── balance_transfers_consumer.py
```

### Step 2: Extract Schema Components

#### From balance_tracking/schema.sql, extract:
- `balance_transfers` table (lines 61-86)
- `balance_transfers_statistics_view` (lines 120-137)
- `balance_transfers_daily_volume_mv` (lines 156-172)
- Related indexes for balance_transfers table (lines 177-194)

#### Remove from balance_tracking/schema.sql:
- All balance_transfers related components
- Update `get_latest_processed_block_height()` to exclude balance_transfers

### Step 3: Create BalanceTransfersIndexerBase

#### Core Methods to Extract:
```python
class BalanceTransfersIndexerBase:
    def __init__(self, connection_params, partitioner, network)
    def _init_tables(self)  # Schema initialization
    def _validate_event_structure(self, event, required_attrs)
    def _group_events(self, events)
    def _process_events(self, events)  # Transfer processing only
    def index_blocks(self, blocks)  # Transfer insertion only
    def get_latest_processed_block_height(self)  # Transfers table only
    def close(self)
```

#### Key Changes from Original:
- Remove balance state tracking methods
- Remove blockchain balance querying
- Remove genesis balance handling
- Focus only on event-to-transfer conversion
- Simplified `get_latest_processed_block_height()` to query only balance_transfers table

### Step 4: Create Network-Specific Indexers

#### BalanceTransfersIndexer (Generic)
- Inherits from BalanceTransfersIndexerBase
- Implements basic transfer processing
- Provides `_process_network_specific_events()` stub

#### BittensorBalanceTransfersIndexer
Extract from balance_tracking_indexer_bittensor.py:
- `SubtensorModule.StakeAdded` events → transfers from coldkey to hotkey
- `SubtensorModule.StakeRemoved` events → transfers from hotkey to coldkey  
- `SubtensorModule.EmissionReceived` events → transfers from "emission" to hotkey

#### PolkadotBalanceTransfersIndexer
Extract from balance_tracking_indexer_polkadot.py:
- `Staking.Rewarded` events → transfers from "staking" to stash
- `Treasury.Awarded` events → transfers from "treasury" to recipient
- `Crowdloan.Contributed` events → transfers from contributor to crowdloan fund
- `Auctions.BidAccepted` events → transfers from bidder to auction system

#### TorusBalanceTransfersIndexer
Extract from balance_tracking_indexer_torus.py:
- `Staking.Reward` events → transfers from "system" to stash
- `Treasury.Awarded` events → transfers from "treasury" to recipient

### Step 5: Create BalanceTransfersConsumer

#### Simplified Consumer Logic:
```python
class BalanceTransfersConsumer:
    def __init__(self, block_stream_manager, balance_transfers_indexer, terminate_event, network, batch_size)
    def run(self)  # Main processing loop
    def _process_block(self, block_height)  # Simplified - no balance queries
    def _cleanup(self)
```

#### Key Simplifications:
- Remove `_get_affected_addresses()` method
- Remove `_query_blockchain_balances()` method
- Remove substrate_node dependency
- Focus only on event processing from block_stream data
- Simplified `_process_block()` that only calls `index_blocks()`

#### Factory Function:
```python
def get_balance_transfers_indexer(network: str, connection_params, partitioner):
    """Factory function to get appropriate transfers indexer based on network"""
    if network in [Network.TORUS.value, Network.TORUS_TESTNET.value]:
        return TorusBalanceTransfersIndexer(connection_params, partitioner, network)
    elif network in [Network.BITTENSOR.value, Network.BITTENSOR_TESTNET.value]:
        return BittensorBalanceTransfersIndexer(connection_params, partitioner, network)
    elif network == Network.POLKADOT.value:
        return PolkadotBalanceTransfersIndexer(connection_params, partitioner, network)
    else:
        raise ValueError(f"Unsupported network: {network}")
```

### Step 6: Update balance_tracking Module

#### Remove from BalanceTrackingIndexerBase:
- `_process_events()` method (transfer processing)
- Transfer-related parts of `index_blocks()` method
- Balance transfers insertion logic
- References to balance_transfers in `get_latest_processed_block_height()`

#### Remove from Network-Specific Indexers:
- All `_process_network_specific_events()` implementations
- Transfer-related event processing

#### Update BalanceTrackingConsumer:
- Remove call to `index_blocks()` for transfers
- Keep only balance state tracking functionality

### Step 7: Schema Migration

#### balance_transfers/schema.sql (New):
```sql
-- Balance Transfers Table
CREATE TABLE IF NOT EXISTS balance_transfers (
    extrinsic_id String,
    event_idx String,
    block_height UInt32,
    block_timestamp UInt64,
    from_address String,
    to_address String,
    asset String,
    amount Decimal128(18),
    fee Decimal128(18),
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, {PARTITION_SIZE})
ORDER BY (extrinsic_id, event_idx, asset);

-- Transfer Statistics View
CREATE VIEW IF NOT EXISTS balance_transfers_statistics_view AS ...

-- Daily Transfer Volume Materialized View  
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_daily_volume_mv ...

-- Indexes
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_from_address ...
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_to_address ...
-- ... other indexes
```

#### balance_tracking/schema.sql (Updated):
- Remove balance_transfers table
- Remove transfer-related views and materialized views
- Remove transfer-related indexes
- Update any references to balance_transfers

### Step 8: Command Line Interface

#### balance_transfers_consumer.py main section:
```python
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Balance Transfers Consumer')
    parser.add_argument('--network', type=str, required=True, choices=networks)
    parser.add_argument('--batch-size', type=int, default=100)
    args = parser.parse_args()

    service_name = f'substrate-{args.network}-balance-transfers'
    setup_logger(service_name)
    
    # Initialize components (no substrate_node needed)
    clickhouse_params = get_clickhouse_connection_string(args.network)
    create_clickhouse_database(clickhouse_params)
    
    partitioner = get_partitioner(args.network)
    balance_transfers_indexer = get_balance_transfers_indexer(args.network, clickhouse_params, partitioner)
    block_stream_manager = BlockStreamManager(clickhouse_params, args.network, terminate_event)
    
    consumer = BalanceTransfersConsumer(
        block_stream_manager,
        balance_transfers_indexer,
        terminate_event,
        args.network,
        args.batch_size
    )
    
    consumer.run()
```

## Benefits of This Extraction

### 1. Separation of Concerns
- **balance_tracking**: Focus on balance state management
- **balance_transfers**: Focus on transfer event processing

### 2. Performance Optimization
- **balance_transfers**: No expensive blockchain balance queries
- **balance_tracking**: No redundant transfer processing

### 3. Independent Scaling
- Each module can be deployed and scaled independently
- Different resource requirements (transfers vs balance queries)

### 4. Simplified Maintenance
- Clearer code organization
- Easier debugging and testing
- Focused functionality per module

## Testing Strategy

### 1. Unit Tests
- Test transfer event processing for each network
- Test schema initialization
- Test bulk insertion logic

### 2. Integration Tests
- Test with real blockchain data
- Verify transfer extraction accuracy
- Performance benchmarking

### 3. Migration Testing
- Verify existing balance_tracking functionality still works
- Ensure no data loss during extraction
- Validate schema changes

## Deployment Considerations

### 1. Backward Compatibility
- Existing balance_tracking deployments continue to work
- Gradual migration possible

### 2. Data Consistency
- Both modules can run simultaneously during transition
- Consistent partitioning strategy
- Same network configurations

### 3. Monitoring
- Separate monitoring for each module
- Transfer processing metrics
- Performance comparison

## File Structure Summary

### New Files Created:
- `packages/indexers/substrate/balance_transfers/__init__.py`
- `packages/indexers/substrate/balance_transfers/schema.sql`
- `packages/indexers/substrate/balance_transfers/balance_transfers_indexer_base.py`
- `packages/indexers/substrate/balance_transfers/balance_transfers_indexer.py`
- `packages/indexers/substrate/balance_transfers/balance_transfers_indexer_bittensor.py`
- `packages/indexers/substrate/balance_transfers/balance_transfers_indexer_polkadot.py`
- `packages/indexers/substrate/balance_transfers/balance_transfers_indexer_torus.py`
- `packages/indexers/substrate/balance_transfers/balance_transfers_consumer.py`

### Modified Files:
- `packages/indexers/substrate/balance_tracking/schema.sql` (remove transfers components)
- `packages/indexers/substrate/balance_tracking/balance_tracking_indexer_base.py` (remove transfer logic)
- `packages/indexers/substrate/balance_tracking/balance_tracking_indexer.py` (remove transfer methods)
- `packages/indexers/substrate/balance_tracking/balance_tracking_indexer_*.py` (remove transfer events)
- `packages/indexers/substrate/balance_tracking/balance_tracking_consumer.py` (remove transfer indexing call)

This plan provides a complete roadmap for extracting the balance_transfers functionality while maintaining the existing balance_tracking conventions and ensuring both modules can operate independently.