# Asset Contract Implementation Checklist (Version 2)

## Phase 1: Assets Dictionary Table ⏳

### Create Assets Infrastructure
- [ ] Create new directory `packages/indexers/substrate/assets/`
- [ ] Create `schema.sql` with assets table definition
- [ ] Add indexes for network and asset_verified columns
- [ ] Create initial data script for native assets (TOR, TAO, DOT)
- [ ] Test table creation and constraints

### Assets Table Schema
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
    PRIMARY KEY (network, asset)
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (network, asset);
```

## Phase 2: Asset Manager Service ⏳

### Create Asset Management Components
- [ ] Create `asset_manager.py` in `packages/indexers/substrate/assets/`
- [ ] Implement `AssetManager` class with:
  - [ ] `__init__` method with connection setup
  - [ ] `_init_native_assets` method
  - [ ] `ensure_asset_exists` method
  - [ ] `get_asset_info` method
  - [ ] `update_asset_verification` method
- [ ] Add logging for new asset discovery
- [ ] Add error handling for database operations
- [ ] Create unit tests for AssetManager

## Phase 3: Update Indexers ⏳

### Balance Series Indexers
- [x] Import AssetManager in `balance_series_indexer_base.py`
- [x] Add AssetManager instance to indexer initialization
- [x] Call `ensure_asset_exists` in `record_balance_series` method
- [x] Add 'native' as asset_contract value when inserting data
- [x] Update column_names list to include 'asset_contract'
- [x] Update queries to filter by asset_contract = 'native'
- [ ] Test with each network-specific indexer:
  - [ ] Torus indexer
  - [ ] Bittensor indexer
  - [ ] Polkadot indexer

### Balance Transfers Indexers
- [x] Import AssetManager in `balance_transfers_indexer_base.py`
- [x] Add AssetManager instance to indexer initialization
- [x] Call `ensure_asset_exists` when processing transfers
- [x] Add 'native' as asset_contract value in transfer records
- [x] Update column_names list to include 'asset_contract'
- [x] Update docstring for network-specific events to include asset_contract
- [ ] Test with each network-specific indexer

## Phase 4: Update Database Views ⏳

### Balance Series Views
- [ ] Update `balance_series_latest_view`:
  ```sql
  LEFT JOIN assets a ON bs.asset = a.asset AND a.network = '{network}'
  ```
- [ ] Update `balance_series_daily_view` with JOIN
- [ ] Update `balance_series_weekly_view` with JOIN
- [ ] Update `balance_series_monthly_view` with JOIN
- [ ] Test all views return asset_contract and asset_verified

### Balance Transfers Views
- [ ] Update `balance_transfers_volume_series_view` with JOIN
- [ ] Update `balance_transfers_network_daily_view` with JOIN
- [ ] Update `balance_transfers_network_weekly_view` with JOIN
- [ ] Update `balance_transfers_network_monthly_view` with JOIN
- [ ] Update `balance_transfers_volume_daily_view` with JOIN
- [ ] Update `balance_transfers_volume_weekly_view` with JOIN
- [ ] Update `balance_transfers_volume_monthly_view` with JOIN
- [ ] Update `balance_transfers_address_analytics_view` with JOIN
- [ ] Update `balance_transfers_volume_trends_view` with JOIN
- [ ] Update address time series views with JOIN

## Phase 5: Update Service Layer ✅

### Balance Series Service
- [x] Add network parameter to service methods
- [x] Update all queries to JOIN with assets table
- [x] Add asset_contract and asset_verified to column lists
- [x] Test each method returns new fields:
  - [x] `get_address_balance_series`
  - [x] `get_current_balances`
  - [x] `get_balance_changes`
  - [x] `get_balance_aggregations`
  - [ ] `get_balance_volume_series`

### Balance Transfers Service
- [x] Add network parameter to service methods
- [x] Update all queries to JOIN with assets table
- [x] Add asset_contract and asset_verified to column lists
- [x] Test each method returns new fields

## Phase 6: Create Asset Management API ⏳

### Asset Management Service
- [ ] Create `asset_management_service.py` in `packages/api/services/`
- [ ] Implement methods:
  - [ ] `get_assets` with pagination and filtering
  - [ ] `get_asset_by_symbol`
  - [ ] `update_asset_verification`
  - [ ] `get_asset_statistics`
  - [ ] `bulk_update_assets`

### Asset Management Router
- [ ] Create `asset_management.py` in `packages/api/routers/`
- [ ] Implement endpoints:
  - [ ] `GET /{network}/assets` - List assets
  - [ ] `GET /{network}/assets/{asset}` - Get specific asset
  - [ ] `PUT /{network}/assets/{asset}/verification` - Update verification
  - [ ] `GET /{network}/assets/stats` - Asset statistics
  - [ ] `POST /{network}/assets/bulk-update` - Bulk updates
- [ ] Add authentication/authorization for update endpoints
- [ ] Add to main router registration

## Phase 7: Update MCP Tools ⏳

### Balance Series Tool
- [ ] Update schema descriptions to mention JOIN with assets
- [ ] Add asset_contract and asset_verified field descriptions
- [ ] Update example queries in documentation

### Balance Transfers Tool
- [ ] Update schema descriptions to mention JOIN with assets
- [ ] Add asset_contract and asset_verified field descriptions
- [ ] Update example queries in documentation

## Phase 8: Testing & Validation ⏳

### Integration Tests
- [ ] Test asset discovery during indexing
- [ ] Test native assets are pre-verified
- [ ] Test unknown assets get 'unknown' status
- [ ] Test asset verification updates
- [ ] Test views return correct verification status
- [ ] Test API endpoints include new fields

### Performance Tests
- [ ] Measure JOIN performance impact
- [ ] Test with large number of assets
- [ ] Verify indexing speed not affected
- [ ] Check query response times

### Manual Testing Scenarios
- [ ] Index new blockchain data and verify assets created
- [ ] Update asset verification via API
- [ ] Query balance data and verify new fields present
- [ ] Test filtering by verification status
- [ ] Verify malicious assets can be identified

## Phase 9: Migration & Deployment ⏳

### Pre-deployment
- [ ] Extract unique assets from existing data:
  ```sql
  INSERT INTO assets (network, asset, first_seen_block)
  SELECT DISTINCT 'torus', asset, min(block_height)
  FROM balance_series
  GROUP BY asset;
  ```
- [ ] Review and categorize existing assets
- [ ] Prepare rollback plan

### Deployment Steps
1. [ ] Deploy assets table schema
2. [ ] Insert native assets data
3. [ ] Deploy AssetManager code
4. [ ] Deploy updated indexers
5. [ ] Update all database views
6. [ ] Deploy updated API services
7. [ ] Deploy asset management API
8. [ ] Run migration to populate assets from existing data
9. [ ] Start reindexing from block 0

### Post-deployment
- [ ] Monitor asset discovery rate
- [ ] Verify all views working correctly
- [ ] Check API responses include new fields
- [ ] Test asset verification updates
- [ ] Document any issues found

## Phase 10: Documentation ⏳

### Technical Documentation
- [ ] Document assets table schema
- [ ] Document AssetManager API
- [ ] Update service method documentation
- [ ] Create asset verification workflow diagram

### API Documentation
- [ ] Document new asset management endpoints
- [ ] Update existing endpoint docs with new fields
- [ ] Add example responses showing asset info
- [ ] Create asset verification guide

### Operational Documentation
- [ ] Create guide for verifying assets
- [ ] Document malicious asset identification process
- [ ] Create runbook for asset updates
- [ ] Document monitoring procedures

## Future Enhancements 🔮

### Asset Scanner Service
- [ ] Design automated scanning logic
- [ ] Implement pattern detection for scams
- [ ] Create alerting system
- [ ] Add machine learning model for detection

### Enhanced Asset Metadata
- [ ] Add logo URLs
- [ ] Add social media links
- [ ] Add total supply tracking
- [ ] Add holder count tracking

### Cross-Network Asset Tracking
- [ ] Design bridge detection
- [ ] Track asset relationships
- [ ] Implement cross-chain verification

## Success Criteria ✅

1. **Assets table populated** with all discovered assets
2. **All views return** asset_contract and asset_verified
3. **API responses include** new fields without breaking changes
4. **Asset updates** immediately reflected in queries
5. **Performance impact** < 5% on query times
6. **Zero data loss** during migration
7. **Documentation complete** for all changes

## Risk Mitigation 🛡️

- **Backup Strategy**: Full backup before deployment
- **Rollback Plan**: Scripts to revert view changes
- **Monitoring**: Alert on high JOIN times
- **Gradual Rollout**: Test on one network first
- **Communication**: Notify API users of new fields