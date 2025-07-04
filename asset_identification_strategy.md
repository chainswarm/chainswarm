# Asset Identification Strategy

## Overview

This document clarifies how to identify and handle different types of assets across the system, particularly distinguishing between native assets (TOR, TAO, DOT) and token assets (like Bittensor subnet tokens).

## Asset Types and Identification

### 1. Native Assets (Main Chain Assets)
- **Examples**: TOR (Torus), TAO (Bittensor), DOT (Polkadot)
- **asset_contract**: `'native'`
- **Identification**: By asset_symbol when asset_contract = 'native'

### 2. Token Assets (Smart Contract Based)
- **Examples**: Bittensor subnet tokens, wrapped tokens, custom tokens
- **asset_contract**: Contract address (e.g., `'5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'`)
- **Identification**: By combination of asset_symbol + asset_contract

## Database Schema Updates

### Assets Dictionary Table
```sql
CREATE TABLE IF NOT EXISTS assets (
    network String,
    asset_symbol String,  -- Symbol/ticker (e.g., USDT, TAO, TOR)
    asset_contract String,  -- Contract address (empty for native assets)
    asset_verified String DEFAULT 'unknown',
    asset_name String DEFAULT '',  -- Full display name
    asset_type String DEFAULT 'token',  -- 'native' or 'token'
    decimals UInt8 DEFAULT 0,
    first_seen_block UInt32,
    first_seen_timestamp DateTime,
    last_updated DateTime DEFAULT now(),
    updated_by String DEFAULT 'system',
    notes String DEFAULT '',
    PRIMARY KEY (network, asset_contract),
    CONSTRAINT valid_asset_verified CHECK asset_verified IN ('verified', 'unknown', 'malicious'),
    CONSTRAINT valid_asset_type CHECK asset_type IN ('native', 'token')
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (network, asset_contract);

-- Insert native assets
INSERT INTO assets (network, asset_symbol, asset_contract, asset_verified, asset_name, asset_type, decimals)
VALUES
    ('torus', 'TOR', 'native', 'verified', 'Torus', 'native', 18),
    ('bittensor', 'TAO', 'native', 'verified', 'Bittensor', 'native', 9),
    ('polkadot', 'DOT', 'native', 'verified', 'Polkadot', 'native', 10);
```

### Balance Series Table Update
```sql
ALTER TABLE balance_series
ADD COLUMN asset_contract String DEFAULT 'native' AFTER asset;

-- Rename for clarity
ALTER TABLE balance_series 
RENAME COLUMN asset TO asset_symbol;
```

### Balance Transfers Table Update
```sql
ALTER TABLE balance_transfers
ADD COLUMN asset_contract String DEFAULT 'native' AFTER asset;

-- Rename for clarity
ALTER TABLE balance_transfers 
RENAME COLUMN asset TO asset_symbol;
```

## Identification Logic in Views

### For JOINs with Assets Table
```sql
-- Example: balance_series_latest_view
CREATE VIEW IF NOT EXISTS balance_series_latest_view AS
SELECT
    bs.address,
    bs.asset_symbol,
    bs.asset_contract,
    a.asset_verified,
    a.asset_name,
    a.asset_type,
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
        asset_symbol,
        asset_contract,
        argMax(period_start_timestamp, period_start_timestamp) as latest_period_start,
        -- ... other fields ...
    FROM balance_series
    GROUP BY address, asset_symbol, asset_contract
) bs
LEFT JOIN assets a ON 
    a.network = 'torus' AND 
    a.asset_contract = bs.asset_contract;  -- This works for both native and tokens
```

## Handling in Indexers

### Asset Manager Logic
```python
class AssetManager:
    def ensure_asset_exists(self, asset_symbol: str, asset_contract: str = '', 
                          asset_type: str = None, **kwargs):
        """Ensure an asset exists in the assets table"""
        
        # Determine asset type
        if not asset_type:
            asset_type = 'native' if not asset_contract else 'token'
        
        # For native assets, ensure 'native' contract
        if asset_type == 'native':
            asset_contract = 'native'
        
        # Check existence by contract (works for both native and tokens)
        result = self.client.query(f"""
            SELECT asset_symbol FROM assets 
            WHERE network = '{self.network}' 
              AND asset_contract = '{asset_contract}'
            LIMIT 1
        """)
        
        if not result.result_rows:
            # Insert new asset
            self._insert_asset(asset_symbol, asset_contract, asset_type, **kwargs)
```

### Indexer Implementation
```python
# In balance series indexer
def record_balance_series(self, block_data, address_balances):
    for address, balance_info in address_balances.items():
        asset_symbol = balance_info['asset_symbol']
        asset_contract = balance_info.get('asset_contract', 'native')
        
        # Ensure asset exists
        self.asset_manager.ensure_asset_exists(
            asset_symbol=asset_symbol,
            asset_contract=asset_contract,
            first_seen_block=block_data.height
        )
        
        # Insert balance data with both symbol and contract
        balance_data.append((
            period_start_timestamp,
            period_end_timestamp,
            block_height,
            address,
            asset_symbol,
            asset_contract,  # Now included in base table
            # ... rest of balance fields ...
        ))
```

## Query Examples

### 1. Get all TAO balances (native Bittensor)
```sql
SELECT * FROM balance_series
WHERE asset_symbol = 'TAO' AND asset_contract = 'native';
```

### 2. Get all subnet tokens on Bittensor
```sql
SELECT DISTINCT 
    bs.asset_symbol,
    bs.asset_contract,
    a.asset_name,
    a.asset_verified
FROM balance_series bs
JOIN assets a ON a.asset_contract = bs.asset_contract
WHERE a.network = 'bittensor' 
  AND a.asset_type = 'token';
```

### 3. Get balances for any asset (native or token)
```sql
-- By symbol only (might return multiple if tokens share symbol)
SELECT * FROM balance_series 
WHERE asset_symbol = 'USDT';

-- By specific contract (unique)
SELECT * FROM balance_series 
WHERE asset_contract = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY';
```

### 4. Check if an asset is verified
```sql
SELECT 
    bs.*,
    a.asset_verified,
    a.asset_type,
    CASE 
        WHEN a.asset_type = 'native' THEN 'Native chain asset'
        WHEN a.asset_verified = 'verified' THEN 'Verified token'
        WHEN a.asset_verified = 'malicious' THEN 'WARNING: Malicious token'
        ELSE 'Unverified token'
    END as asset_status
FROM balance_series bs
LEFT JOIN assets a ON a.asset_contract = bs.asset_contract
WHERE bs.address = '5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f';
```

## API Response Structure

```json
{
  "items": [
    {
      "address": "5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f",
      "asset_symbol": "TAO",
      "asset_contract": "native",
      "asset_type": "native",
      "asset_verified": "verified",
      "asset_name": "Bittensor",
      "total_balance": "1000.5"
    },
    {
      "address": "5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f",
      "asset_symbol": "SUBNET1",
      "asset_contract": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
      "asset_type": "token",
      "asset_verified": "unknown",
      "asset_name": "Subnet 1 Token",
      "total_balance": "500.0"
    }
  ]
}
```

## Benefits of This Approach

1. **Clear Identification**: Every asset uniquely identified by asset_contract
2. **Native Asset Support**: 'native' as contract value for native assets
3. **Token Support**: Full contract addresses for all tokens
4. **No Ambiguity**: Can distinguish between different tokens with same symbol
5. **Efficient Queries**: Single JOIN condition works for all asset types
6. **Future Proof**: Ready for subnet tokens, wrapped assets, bridges, etc.

## Migration Notes

1. Rename existing `asset` columns to `asset_symbol` for clarity
2. Add `asset_contract` column to base tables (default 'native')
3. Populate `asset_contract` as 'native' for all existing native asset records
4. Update all views to use new column names and JOIN logic