# Asset Contract and Verification Implementation Plan (Version 2)

## Overview

This updated plan introduces a centralized `assets` dictionary table that serves as the source of truth for asset information and verification status. Indexers will insert new assets they discover, and administrators can later update the verification status. All views and queries will join with this table to get the current verification status.

## New Architecture

### Core Concept
1. **Indexers** discover and insert new assets into the `assets` table with `unknown` status
2. **Assets table** maintains the current verification status for each asset
3. **Views and queries** join with the assets table to get real-time verification status
4. **Admin processes** can update asset verification status at any time

## Field Specifications

### Assets Dictionary Table:
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
```

### Native Assets Initial Data:
```sql
-- Insert native assets as verified
INSERT INTO assets (network, asset, asset_contract, asset_verified, asset_name, decimals, first_seen_block, first_seen_timestamp)
VALUES 
    ('torus', 'TOR', '', 'verified', 'Torus', 18, 0, '2024-01-01 00:00:00'),
    ('bittensor', 'TAO', '', 'verified', 'Bittensor', 9, 0, '2021-01-01 00:00:00'),
    ('polkadot', 'DOT', '', 'verified', 'Polkadot', 10, 0, '2020-01-01 00:00:00');
```

## Changes Required by Component

### 1. Database Schema Changes

#### 1.1 New Assets Table (`packages/indexers/substrate/assets/schema.sql`)

```sql
-- Assets dictionary table
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

-- Index for quick lookups
ALTER TABLE assets ADD INDEX IF NOT EXISTS idx_verified asset_verified TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE assets ADD INDEX IF NOT EXISTS idx_network network TYPE bloom_filter(0.01) GRANULARITY 1;
```

#### 1.2 Balance Series Schema Updates

**No changes to base table** - keep it simple with just asset field.

**Update Views to Join with Assets Table:**

```sql
-- Example: balance_series_latest_view with asset info
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

#### 1.3 Balance Transfers Schema Updates

**No changes to base table** - keep it simple.

**Update Views to Join with Assets Table** - Similar pattern as balance series.

### 2. Indexer Changes

#### 2.1 Asset Management Service (`packages/indexers/substrate/assets/asset_manager.py`)

```python
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

#### 2.2 Balance Series Indexer Updates

```python
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
        
        # Continue with existing logic - no need to store asset_contract/verified here
        # ... rest of existing method ...
```

### 3. API Service Layer Changes

#### 3.1 Balance Series Service Updates

Update queries to join with assets table:

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

### 4. Asset Management API

#### 4.1 New Asset Management Service (`packages/api/services/asset_management_service.py`)

```python
class AssetManagementService:
    def __init__(self, connection_params: Dict[str, Any]):
        self.client = clickhouse_connect.get_client(**connection_params)
    
    def get_assets(self, network: str, verified_only: bool = False, 
                   page: int = 1, page_size: int = 100):
        """Get assets with optional filtering"""
        filters = [f"network = '{network}'"]
        if verified_only:
            filters.append("asset_verified = 'verified'")
        
        where_clause = " WHERE " + " AND ".join(filters)
        
        # ... implement pagination query ...
    
    def update_asset_verification(self, network: str, asset: str, 
                                verified: str, updated_by: str, notes: str = ''):
        """Update asset verification status"""
        if verified not in ['verified', 'unknown', 'malicious']:
            raise ValueError("Invalid verification status")
        
        data = [(
            network,
            asset,
            verified,
            datetime.now(),
            updated_by,
            notes
        )]
        
        # This will update the existing record due to ReplacingMergeTree
        self.client.insert('assets', data, column_names=[
            'network', 'asset', 'asset_verified', 
            'last_updated', 'updated_by', 'notes'
        ])
        
        return {"status": "updated", "asset": asset, "verified": verified}
```

#### 4.2 New Asset Management Router (`packages/api/routers/asset_management.py`)

```python
@router.get("/{network}/assets")
async def get_assets(
    network: str = Path(..., description="Network identifier"),
    verified_only: bool = Query(False, description="Return only verified assets"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000)
):
    """Get list of assets with their verification status"""
    # ... implementation ...

@router.put("/{network}/assets/{asset}/verification")
async def update_asset_verification(
    network: str = Path(..., description="Network identifier"),
    asset: str = Path(..., description="Asset symbol"),
    verification_update: AssetVerificationUpdate = Body(...)
):
    """Update asset verification status (admin only)"""
    # ... implementation with auth check ...
```

### 5. Benefits of This Approach

1. **Single Source of Truth**: Assets table maintains current verification status
2. **Real-time Updates**: Changes to asset verification immediately reflected in all queries
3. **No Data Duplication**: Base tables remain simple, verification info centralized
4. **Audit Trail**: Track who updated verification and when
5. **Extensible**: Easy to add more asset metadata in the future
6. **Performance**: LEFT JOIN with small assets table is efficient

### 6. Migration Strategy

1. Create assets table first
2. Populate with native assets
3. Run a one-time script to extract unique assets from existing data
4. Update all views to join with assets table
5. Deploy updated indexers that use AssetManager
6. Deploy API updates

### 7. Example Queries

```sql
-- Get all malicious assets
SELECT * FROM assets 
WHERE asset_verified = 'malicious' 
ORDER BY last_updated DESC;

-- Get balance series with verification info
SELECT 
    bs.*,
    a.asset_verified,
    a.asset_contract,
    a.notes as verification_notes
FROM balance_series bs
LEFT JOIN assets a ON bs.asset = a.asset AND a.network = 'torus'
WHERE bs.address = '5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f'
  AND a.asset_verified != 'malicious';  -- Exclude malicious assets

-- Update asset as malicious
INSERT INTO assets (network, asset, asset_verified, last_updated, updated_by, notes)
VALUES ('torus', 'SCAM', 'malicious', now(), 'admin', 'Confirmed rug pull');
```

## Implementation Order

1. **Phase 1**: Create assets table and populate native assets
2. **Phase 2**: Create AssetManager service
3. **Phase 3**: Update indexers to use AssetManager
4. **Phase 4**: Update all views to join with assets table
5. **Phase 5**: Update API services to include asset info
6. **Phase 6**: Add asset management API endpoints
7. **Phase 7**: Create admin UI for asset verification

This approach provides maximum flexibility while keeping the core data model simple and efficient.