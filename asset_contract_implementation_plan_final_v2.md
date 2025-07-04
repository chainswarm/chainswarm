# Asset Contract and Verification Implementation Plan (Final v2)

## Overview

This plan introduces a centralized `assets` dictionary table that serves as the source of truth for asset information and verification status. The primary key is based on network + asset_contract, as contract addresses are unique identifiers. Multiple assets can have the same symbol/name.

## Architecture

### Core Concept
1. **Indexers** discover and insert new assets into the `assets` table with `unknown` status
2. **Assets table** uses network + asset_contract as the unique identifier
3. **Views and queries** JOIN with the assets table to get real-time verification status
4. **Manual updates** can be done directly to the assets table via SQL

## New Assets Dictionary Table

### Schema (`packages/indexers/substrate/assets/schema.sql`)
```sql
CREATE TABLE IF NOT EXISTS assets (
    network String,
    asset String,  -- Symbol/ticker (e.g., USDT, WBTC)
    asset_contract String,  -- Unique contract address (primary key with network)
    asset_verified String DEFAULT 'unknown',
    asset_name String DEFAULT '',  -- Full display name (e.g., "Tether USD")
    decimals UInt8 DEFAULT 0,
    first_seen_block UInt32,
    first_seen_timestamp DateTime,
    last_updated DateTime DEFAULT now(),
    updated_by String DEFAULT 'system',
    notes String DEFAULT '',
    PRIMARY KEY (network, asset_contract),
    CONSTRAINT valid_asset_verified CHECK asset_verified IN ('verified', 'unknown', 'malicious')
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (network, asset_contract);

-- Indexes for performance
ALTER TABLE assets ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE assets ADD INDEX IF NOT EXISTS idx_verified asset_verified TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE assets ADD INDEX IF NOT EXISTS idx_network network TYPE bloom_filter(0.01) GRANULARITY 1;

-- Insert native assets as verified (empty contract address for native tokens)
INSERT INTO assets (network, asset, asset_contract, asset_verified, asset_name, decimals, first_seen_block, first_seen_timestamp)
VALUES 
    ('torus', 'TOR', '', 'verified', 'Torus', 18, 0, '2024-01-01 00:00:00'),
    ('bittensor', 'TAO', '', 'verified', 'Bittensor', 9, 0, '2021-01-01 00:00:00'),
    ('polkadot', 'DOT', '', 'verified', 'Polkadot', 10, 0, '2020-01-01 00:00:00');
```

## Important Design Decisions

### Why asset_contract as Primary Key?
1. **Uniqueness**: Contract addresses are globally unique on a network
2. **Multiple symbols**: Different tokens can have the same symbol (many "USDT" clones)
3. **Immutability**: Contract addresses don't change, symbols might

### Field Purposes:
- `asset`: The symbol/ticker used in balance_series and balance_transfers tables
- `asset_contract`: The unique identifier (contract address or empty for native)
- `asset_name`: Human-readable full name for display purposes

## Changes Required by Component

### 1. Updated Asset Manager Service

```python
class AssetManager:
    def __init__(self, connection_params: Dict[str, Any], network: str):
        self.client = clickhouse_connect.get_client(**connection_params)
        self.network = network
        self._init_native_assets()
    
    def ensure_asset_exists(self, asset: str, asset_contract: str = '', 
                          asset_verified: str = 'unknown', **kwargs):
        """Ensure an asset exists in the assets table"""
        try:
            # For native assets, contract is empty string
            if not asset_contract and asset in ['TOR', 'TAO', 'DOT']:
                asset_contract = ''
            
            # Check if asset already exists by contract
            result = self.client.query(f"""
                SELECT asset FROM assets 
                WHERE network = '{self.network}' 
                  AND asset_contract = '{asset_contract}'
                LIMIT 1
            """)
            
            if not result.result_rows:
                # Insert new asset
                data = [(
                    self.network,
                    asset,
                    asset_contract,
                    asset_verified,
                    kwargs.get('asset_name', asset),  # Default name to symbol
                    kwargs.get('decimals', 0),
                    kwargs.get('first_seen_block', 0),
                    datetime.now(),
                    datetime.now(),
                    'indexer',
                    ''
                )]
                
                self.client.insert('assets', data, column_names=[
                    'network', 'asset', 'asset_contract', 'asset_verified',
                    'asset_name', 'decimals', 'first_seen_block', 
                    'first_seen_timestamp', 'last_updated', 'updated_by', 'notes'
                ])
                
                logger.info(f"New asset discovered: {asset} ({asset_contract}) on {self.network}")
        except Exception as e:
            logger.error(f"Error ensuring asset exists: {e}")
```

### 2. Update Indexers to Track Contract Addresses

Indexers need to extract contract addresses when processing events. For example:

```python
# In balance transfers indexer
def process_transfer_event(self, event):
    # Extract asset info from event
    asset_symbol = self.extract_asset_symbol(event)
    asset_contract = self.extract_asset_contract(event)  # New: extract contract address
    
    # Ensure asset exists
    self.asset_manager.ensure_asset_exists(
        asset=asset_symbol,
        asset_contract=asset_contract,
        first_seen_block=event.block_height
    )
    
    # Continue with transfer processing...
```

### 3. Update Database Views with Proper JOINs

Since balance_series and balance_transfers tables store asset symbols, we need to handle the JOIN carefully:

```sql
-- For native assets (empty contract), join on asset symbol
-- For token assets, we might need to store contract in the base tables too
-- OR maintain a mapping table

CREATE VIEW IF NOT EXISTS balance_series_latest_view AS
SELECT
    bs.address,
    bs.asset,
    COALESCE(a.asset_contract, '') as asset_contract,
    COALESCE(a.asset_verified, 'unknown') as asset_verified,
    COALESCE(a.asset_name, bs.asset) as asset_name,
    bs.latest_period_start,
    bs.latest_period_end,
    bs.latest_block_height,
    bs.free_balance,
    bs.reserved_balance,
    bs.staked_balance,
    bs.total_balance
FROM (
    SELECT
        address,
        asset,
        argMax(period_start_timestamp, period_start_timestamp) as latest_period_start,
        argMax(period_end_timestamp, period_start_timestamp) as latest_period_end,
        argMax(block_height, period_start_timestamp) as latest_block_height,
        argMax(free_balance, period_start_timestamp) as free_balance,
        argMax(reserved_balance, period_start_timestamp) as reserved_balance,
        argMax(staked_balance, period_start_timestamp) as staked_balance,
        argMax(total_balance, period_start_timestamp) as total_balance
    FROM balance_series
    GROUP BY address, asset
) bs
-- This JOIN needs refinement - see note below
LEFT JOIN assets a ON bs.asset = a.asset AND a.network = 'torus';
```

### Important: Base Table Modification Needed

Given that asset_contract is the unique identifier, we have two options:

**Option 1: Add asset_contract to base tables** (Recommended)
```sql
-- Modify balance_series and balance_transfers to include asset_contract
ALTER TABLE balance_series ADD COLUMN asset_contract String DEFAULT '';
ALTER TABLE balance_transfers ADD COLUMN asset_contract String DEFAULT '';
```

**Option 2: Create a mapping table**
```sql
-- Alternative: Asset symbol to contract mapping
CREATE TABLE asset_symbol_mapping (
    network String,
    asset String,
    asset_contract String,
    PRIMARY KEY (network, asset, asset_contract)
) ENGINE = MergeTree();
```

### 4. Manual Asset Verification

With contract-based primary key:

```sql
-- Mark a specific contract as malicious
INSERT INTO assets (network, asset, asset_contract, asset_verified, last_updated, updated_by, notes)
VALUES ('torus', 'FAKE_USDT', '0x123...scam', 'malicious', now(), 'manual', 'Fake USDT contract');

-- Update verification for a specific contract
INSERT INTO assets (network, asset_contract, asset_verified, last_updated, updated_by, notes)
SELECT network, asset_contract, 'verified', now(), 'manual', 'Verified by team'
FROM assets 
WHERE network = 'torus' AND asset_contract = '0x456...legit';

-- Query all versions of USDT
SELECT * FROM assets 
WHERE network = 'torus' AND asset = 'USDT'
ORDER BY asset_verified DESC, first_seen_timestamp;
```

## Recommendation: Add asset_contract to Base Tables

To properly implement this, I recommend adding `asset_contract` to the balance_series and balance_transfers tables:

1. **Unique identification**: Each balance/transfer record linked to specific contract
2. **Accurate JOINs**: Can JOIN on (network, asset_contract) for exact matches
3. **Handle duplicates**: Multiple USDT contracts tracked separately
4. **Future-proof**: Ready for cross-chain assets and bridges

This means updating the indexers to:
- Extract contract addresses from events
- Store both asset (symbol) and asset_contract in base tables
- Use asset_contract as the primary identifier

## Benefits

1. **Accurate tracking**: Each token contract tracked separately
2. **No symbol conflicts**: Multiple tokens with same symbol handled correctly
3. **Scam detection**: Can identify fake tokens mimicking popular symbols
4. **Contract-level verification**: Verify specific contracts, not just symbols

This approach provides the most accurate and maintainable solution for asset tracking and verification.