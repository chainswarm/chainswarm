# Asset Contract and Verification Implementation Plan (Final)

## Overview

This plan introduces a centralized `assets` dictionary table that serves as the source of truth for asset information and verification status. Indexers will insert new assets they discover with 'unknown' status, and all views will JOIN with this table to get the current verification status. Manual updates to the assets table can be done directly via SQL.

## Architecture

### Core Concept
1. **Indexers** discover and insert new assets into the `assets` table with `unknown` status
2. **Assets table** maintains the current verification status for each asset
3. **Views and queries** JOIN with the assets table to get real-time verification status
4. **Manual updates** can be done directly to the assets table via SQL

## New Assets Dictionary Table

### Schema (`packages/indexers/substrate/assets/schema.sql`)
```sql
CREATE TABLE IF NOT EXISTS assets (
    network String,
    asset String,
    asset_contract String DEFAULT '',
    asset_verified String DEFAULT 'unknown',
    asset_name String DEFAULT '',
    decimals UInt8 DEFAULT 0,
    first_seen_block UInt32,
    first_seen_timestamp DateTime,
    last_updated DateTime DEFAULT now(),
    updated_by String DEFAULT 'system',
    notes String DEFAULT '',
    PRIMARY KEY (network, asset),
    CONSTRAINT valid_asset_verified CHECK asset_verified IN ('verified', 'unknown', 'malicious')
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (network, asset);

-- Indexes for performance
ALTER TABLE assets ADD INDEX IF NOT EXISTS idx_verified asset_verified TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE assets ADD INDEX IF NOT EXISTS idx_network network TYPE bloom_filter(0.01) GRANULARITY 1;

-- Insert native assets as verified
INSERT INTO assets (network, asset, asset_contract, asset_verified, asset_name, decimals, first_seen_block, first_seen_timestamp)
VALUES 
    ('torus', 'TOR', '', 'verified', 'Torus', 18, 0, '2024-01-01 00:00:00'),
    ('bittensor', 'TAO', '', 'verified', 'Bittensor', 9, 0, '2021-01-01 00:00:00'),
    ('polkadot', 'DOT', '', 'verified', 'Polkadot', 10, 0, '2020-01-01 00:00:00');
```

## Changes Required by Component

### 1. Asset Manager Service

Create `packages/indexers/substrate/assets/asset_manager.py`:

```python
import clickhouse_connect
from typing import Dict, Any
from datetime import datetime
from loguru import logger

class AssetManager:
    def __init__(self, connection_params: Dict[str, Any], network: str):
        self.client = clickhouse_connect.get_client(**connection_params)
        self.network = network
        self._init_native_assets()
    
    def _init_native_assets(self):
        """Initialize native assets as verified"""
        native_assets = {
            'torus': ('TOR', 'Torus', 18),
            'bittensor': ('TAO', 'Bittensor', 9),
            'polkadot': ('DOT', 'Polkadot', 10)
        }
        
        if self.network in native_assets:
            asset, name, decimals = native_assets[self.network]
            self.ensure_asset_exists(
                asset=asset,
                asset_contract='',
                asset_verified='verified',
                asset_name=name,
                decimals=decimals,
                first_seen_block=0
            )
    
    def ensure_asset_exists(self, asset: str, asset_contract: str = '', 
                          asset_verified: str = 'unknown', **kwargs):
        """Ensure an asset exists in the assets table"""
        try:
            # Check if asset already exists
            result = self.client.query(f"""
                SELECT asset FROM assets 
                WHERE network = '{self.network}' AND asset = '{asset}'
                LIMIT 1
            """)
            
            if not result.result_rows:
                # Insert new asset
                data = [(
                    self.network,
                    asset,
                    asset_contract,
                    asset_verified,
                    kwargs.get('asset_name', ''),
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
                
                logger.info(f"New asset discovered: {asset} on {self.network}")
        except Exception as e:
            logger.error(f"Error ensuring asset exists: {e}")
```

### 2. Update Indexers

#### Balance Series Indexer (`packages/indexers/substrate/balance_series/balance_series_indexer_base.py`)

Add AssetManager integration:
```python
from packages.indexers.substrate.assets.asset_manager import AssetManager

class BalanceSeriesIndexerBase:
    def __init__(self, connection_params: Dict[str, Any], metrics: IndexerMetrics, network: str, period_hours: int = 4):
        # ... existing init code ...
        self.asset_manager = AssetManager(connection_params, network)
    
    def record_balance_series(self, period_start_timestamp: int, period_end_timestamp: int, 
                            block_height: int, address_balances: Dict[str, Dict[str, int]]):
        # Ensure asset exists in dictionary
        self.asset_manager.ensure_asset_exists(
            asset=self.asset,
            first_seen_block=block_height
        )
        
        # Continue with existing logic - no changes to data insertion
        # The base table remains simple, only storing the asset symbol
```

### 3. Update Database Views

All views need to be updated to JOIN with the assets table. Example for balance_series_latest_view:

```sql
CREATE VIEW IF NOT EXISTS balance_series_latest_view AS
SELECT
    bs.address,
    bs.asset,
    COALESCE(a.asset_contract, '') as asset_contract,
    COALESCE(a.asset_verified, 'unknown') as asset_verified,
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
LEFT JOIN assets a ON bs.asset = a.asset AND a.network = 'torus'; -- Network will be parameterized
```

### 4. Update Service Layer

All service methods need to be updated to include the JOIN and return new fields. Example:

```python
def get_address_balance_series(self, address: str, page: int, page_size: int, 
                             assets: List[str] = None, network: str = 'torus'):
    # ... existing code ...
    
    data_query = f"""
        SELECT bs.period_start_timestamp,
               bs.period_end_timestamp,
               bs.block_height,
               bs.address,
               bs.asset,
               COALESCE(a.asset_contract, '') as asset_contract,
               COALESCE(a.asset_verified, 'unknown') as asset_verified,
               bs.free_balance,
               bs.reserved_balance,
               bs.staked_balance,
               bs.total_balance,
               bs.free_balance_change,
               bs.reserved_balance_change,
               bs.staked_balance_change,
               bs.total_balance_change,
               bs.total_balance_percent_change
        FROM (SELECT * FROM balance_series FINAL) AS bs
        LEFT JOIN assets a ON bs.asset = a.asset AND a.network = '{network}'
        WHERE bs.address = {{address:String}}{asset_filter}{timestamp_filter}
        ORDER BY bs.period_start_timestamp DESC
        LIMIT {{limit:Int}} OFFSET {{offset:Int}}
    """
    
    # Update columns list to include new fields
    columns = [
        "period_start_timestamp",
        "period_end_timestamp", 
        "block_height",
        "address",
        "asset",
        "asset_contract",  # New
        "asset_verified",  # New
        # ... rest of columns ...
    ]
```

### 5. Update MCP Tools

Update schema descriptions to include the new fields:

```python
# In balance_series.py schema method
if "asset_contract" in schema["balance_series"]["columns"]:
    schema["balance_series"]["columns"]["asset_contract"]["description"] = "Contract address that created/owns the asset (empty for native assets)"

if "asset_verified" in schema["balance_series"]["columns"]:
    schema["balance_series"]["columns"]["asset_verified"]["description"] = "Verification status: verified, unknown, or malicious"
```

## Manual Asset Verification

Since there's no admin API, asset verification updates will be done directly via SQL:

```sql
-- Mark an asset as malicious
INSERT INTO assets (network, asset, asset_verified, last_updated, updated_by, notes)
VALUES ('torus', 'SCAM_TOKEN', 'malicious', now(), 'manual', 'Confirmed rug pull');

-- Update an existing asset to verified
INSERT INTO assets (network, asset, asset_verified, last_updated, updated_by, notes)
VALUES ('torus', 'LEGIT_TOKEN', 'verified', now(), 'manual', 'Verified by team');

-- Query malicious assets
SELECT * FROM assets 
WHERE asset_verified = 'malicious' 
ORDER BY last_updated DESC;
```

## Implementation Order

1. **Phase 1**: Create assets table and populate native assets
2. **Phase 2**: Create AssetManager service
3. **Phase 3**: Update indexers to use AssetManager
4. **Phase 4**: Update all views to JOIN with assets table
5. **Phase 5**: Update API services to include asset info in queries
6. **Phase 6**: Update MCP tools to expose new fields

## Benefits

1. **Single Source of Truth**: Assets table maintains current verification status
2. **Real-time Updates**: Changes to asset verification immediately reflected
3. **No Data Duplication**: Base tables remain simple
4. **Clean Architecture**: Separation between indexing and verification
5. **Performance**: Efficient LEFT JOIN with small assets table

## Migration for Existing Data

If there's existing data, run this one-time migration:

```sql
-- Extract unique assets from balance_series
INSERT INTO assets (network, asset, first_seen_block, first_seen_timestamp)
SELECT 
    'torus' as network,
    asset,
    min(block_height) as first_seen_block,
    fromUnixTimestamp64Milli(min(period_start_timestamp)) as first_seen_timestamp
FROM balance_series
WHERE asset NOT IN (SELECT asset FROM assets WHERE network = 'torus')
GROUP BY asset;

-- Extract unique assets from balance_transfers
INSERT INTO assets (network, asset, first_seen_block, first_seen_timestamp)
SELECT 
    'torus' as network,
    asset,
    min(block_height) as first_seen_block,
    fromUnixTimestamp64Milli(min(block_timestamp)) as first_seen_timestamp
FROM balance_transfers
WHERE asset NOT IN (SELECT asset FROM assets WHERE network = 'torus')
GROUP BY asset;
```

This approach provides a clean, maintainable solution for asset verification without requiring complex admin interfaces.