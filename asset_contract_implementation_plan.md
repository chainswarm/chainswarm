# Asset Contract and Verification Implementation Plan

## Overview

This document outlines all the changes required to add `asset_contract` and `asset_verified` fields throughout the ChainSwarm codebase. These fields will help identify asset ownership/creation and determine if assets are legitimate or potentially malicious.

## Field Specifications

### New Fields:
1. **asset_contract** (String)
   - Purpose: Defines the owner/creator contract address of the asset
   - Default: Empty string for native assets (TOR, TAO, DOT)
   
2. **asset_verified** (String)
   - Purpose: Indicates the verification status of the asset
   - Values: `verified` | `unknown` | `malicious`
   - Default: `verified` for native assets, `unknown` for others

## Changes Required by Component

### 1. Database Schema Changes

#### 1.1 Balance Series Schema (`packages/indexers/substrate/balance_series/schema.sql`)

**Current Structure:**
```sql
CREATE TABLE IF NOT EXISTS balance_series (
    -- ... existing fields ...
    asset String,
    -- ... other fields ...
)
```

**Proposed Changes:**
```sql
CREATE TABLE IF NOT EXISTS balance_series (
    -- ... existing fields ...
    asset String,
    asset_contract String DEFAULT '',
    asset_verified String DEFAULT 'unknown',
    -- ... other fields ...
    CONSTRAINT valid_asset_verified CHECK asset_verified IN ('verified', 'unknown', 'malicious')
)
```

**Views to Update:**
- `balance_series_latest_view` - Add asset_contract and asset_verified to SELECT
- `balance_series_daily_view` - Add asset_contract and asset_verified to GROUP BY and SELECT
- `balance_series_weekly_view` - Update materialized view to include new fields
- `balance_series_monthly_view` - Update materialized view to include new fields

#### 1.2 Balance Transfers Schema (`packages/indexers/substrate/balance_transfers/schema.sql`)

**Current Structure:**
```sql
CREATE TABLE IF NOT EXISTS balance_transfers (
    -- ... existing fields ...
    asset String,
    -- ... other fields ...
)
```

**Proposed Changes:**
```sql
CREATE TABLE IF NOT EXISTS balance_transfers (
    -- ... existing fields ...
    asset String,
    asset_contract String DEFAULT '',
    asset_verified String DEFAULT 'unknown',
    -- ... other fields ...
    CONSTRAINT valid_asset_verified CHECK asset_verified IN ('verified', 'unknown', 'malicious')
)
```

**Views and Materialized Views to Update:**
- `balance_transfers_volume_series_mv_internal` - Add asset_contract and asset_verified
- `balance_transfers_volume_series_view` - Include new fields
- `balance_transfers_network_daily_view` - Add to GROUP BY and SELECT
- `balance_transfers_network_weekly_view` - Add to GROUP BY and SELECT
- `balance_transfers_network_monthly_view` - Add to GROUP BY and SELECT
- `balance_transfers_volume_daily_view` - Add to GROUP BY and SELECT
- `balance_transfers_volume_weekly_view` - Add to GROUP BY and SELECT
- `balance_transfers_volume_monthly_view` - Add to GROUP BY and SELECT
- `balance_transfers_address_analytics_view` - Include new fields in CTEs
- `balance_transfers_volume_trends_view` - Include new fields
- `balance_transfers_address_daily_internal` - Add to materialized view
- `balance_transfers_address_weekly_internal` - Add to materialized view
- `balance_transfers_address_monthly_internal` - Add to materialized view

### 2. Indexer Changes

#### 2.1 Balance Series Indexer Base (`packages/indexers/substrate/balance_series/balance_series_indexer_base.py`)

**Method to Update: `record_balance_series`**

Current data insertion:
```python
balance_data.append((
    period_start_timestamp,
    period_end_timestamp,
    block_height,
    address,
    self.asset,
    # ... balance fields ...
))
```

Proposed changes:
```python
# Determine asset_contract and asset_verified based on network and asset
asset_contract = ''
asset_verified = 'verified' if self.asset in ['TOR', 'TAO', 'DOT'] else 'unknown'

balance_data.append((
    period_start_timestamp,
    period_end_timestamp,
    block_height,
    address,
    self.asset,
    asset_contract,  # New field
    asset_verified,  # New field
    # ... balance fields ...
))

# Update column_names in insert
self.client.insert('balance_series', balance_data, column_names=[
    'period_start_timestamp', 'period_end_timestamp', 'block_height',
    'address', 'asset', 'asset_contract', 'asset_verified',  # Added new fields
    'free_balance', 'reserved_balance', 'staked_balance', 'total_balance',
    # ... rest of columns ...
])
```

#### 2.2 Balance Transfers Indexer Base (`packages/indexers/substrate/balance_transfers/balance_transfers_indexer_base.py`)

Similar changes needed in the transfer recording methods to include asset_contract and asset_verified fields.

### 3. API Service Layer Changes

#### 3.1 Balance Series Service (`packages/api/services/balance_series_service.py`)

**Methods to Update:**

1. **`get_address_balance_series`** - Add new fields to SELECT and columns list
2. **`get_current_balances`** - Add new fields to SELECT and columns list
3. **`get_balance_changes`** - Add new fields to SELECT and columns list
4. **`get_balance_aggregations`** - Update all period queries to include new fields
5. **`get_balance_volume_series`** - Add new fields to aggregation query

Example change for `get_address_balance_series`:
```python
data_query = f"""
    SELECT bs.period_start_timestamp,
           -- ... existing fields ...
           bs.asset,
           bs.asset_contract,  -- New field
           bs.asset_verified,  -- New field
           -- ... rest of fields ...
    FROM (SELECT * FROM balance_series FINAL) AS bs
    -- ... rest of query ...
"""

columns = [
    "period_start_timestamp",
    # ... existing columns ...
    "asset",
    "asset_contract",  # New column
    "asset_verified",  # New column
    # ... rest of columns ...
]
```

#### 3.2 Balance Transfers Service (`packages/api/services/balance_transfers_service.py`)

**Methods to Update:**

1. **`get_address_transactions`** - Add new fields to SELECT and columns list
2. **`get_balance_volume_series`** - Update for all period types
3. **`get_network_analytics`** - Include new fields in results
4. **`get_address_analytics`** - Include new fields
5. **`get_volume_aggregations`** - Include new fields
6. **`get_addresses_analytics`** - Include new fields

### 4. API Router Changes

No changes needed in routers as they pass through the service layer responses.

### 5. MCP Server Tool Changes

#### 5.1 Balance Series Tool (`packages/api/tools/balance_series.py`)

**Update `schema` method** to include descriptions for new fields:

```python
if "asset_contract" in schema["balance_series"]["columns"]:
    schema["balance_series"]["columns"]["asset_contract"]["description"] = "Contract address that created/owns the asset (empty for native assets)"

if "asset_verified" in schema["balance_series"]["columns"]:
    schema["balance_series"]["columns"]["asset_verified"]["description"] = "Verification status: verified, unknown, or malicious"
```

#### 5.2 Balance Transfers Tool (`packages/api/tools/balance_transfers.py`)

Similar schema description updates needed.

### 6. Default Values for Native Assets

Create a configuration or constant mapping:

```python
NATIVE_ASSETS = {
    'torus': {'asset': 'TOR', 'asset_contract': '', 'asset_verified': 'verified'},
    'bittensor': {'asset': 'TAO', 'asset_contract': '', 'asset_verified': 'verified'},
    'polkadot': {'asset': 'DOT', 'asset_contract': '', 'asset_verified': 'verified'}
}
```

### 7. Implementation Order

1. **Phase 1: Schema Updates**
   - Update all SQL schema files
   - Drop and recreate tables/views (since we're reindexing from 0)

2. **Phase 2: Indexer Updates**
   - Update balance series indexers
   - Update balance transfers indexers
   - Add logic to determine asset_contract and asset_verified values

3. **Phase 3: Service Layer Updates**
   - Update all service methods to include new fields
   - Ensure backward compatibility in API responses

4. **Phase 4: MCP Tool Updates**
   - Update schema descriptions
   - Ensure tools can query new fields

5. **Phase 5: Documentation**
   - Update API documentation
   - Update MCP tool documentation

## Notes

1. Since we're reindexing from 0, no migration scripts are needed
2. The asset_verified field should be updatable in the future through an admin interface
3. Consider adding an asset registry table in the future to manage asset metadata centrally
4. The asset_contract field will be crucial for identifying token standards and ownership

## Future Enhancements

1. **Asset Registry Table**: Create a separate table to manage asset metadata:
   ```sql
   CREATE TABLE asset_registry (
       network String,
       asset String,
       asset_contract String,
       asset_verified String,
       asset_name String,
       decimals UInt8,
       created_at DateTime,
       updated_at DateTime,
       PRIMARY KEY (network, asset)
   )
   ```

2. **Automated Verification**: Implement logic to automatically verify assets based on:
   - Contract age
   - Transaction volume
   - Community reports
   - External verification services

3. **API Endpoints**: Add endpoints to:
   - Query assets by verification status
   - Update asset verification status
   - Report suspicious assets