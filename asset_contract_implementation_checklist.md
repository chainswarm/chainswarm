# Asset Contract Implementation Checklist

## Phase 1: Database Schema Updates ⏳

### Balance Series Schema
- [ ] Add `asset_contract String DEFAULT ''` to `balance_series` table
- [ ] Add `asset_verified String DEFAULT 'unknown'` to `balance_series` table
- [ ] Add CHECK constraint for `asset_verified` values
- [ ] Update `balance_series_latest_view` to include new fields
- [ ] Update `balance_series_daily_view` to include new fields
- [ ] Update `balance_series_weekly_mv_internal` materialized view
- [ ] Update `balance_series_monthly_mv_internal` materialized view
- [ ] Update `balance_series_weekly_view` wrapper view
- [ ] Update `balance_series_monthly_view` wrapper view

### Balance Transfers Schema
- [ ] Add `asset_contract String DEFAULT ''` to `balance_transfers` table
- [ ] Add `asset_verified String DEFAULT 'unknown'` to `balance_transfers` table
- [ ] Add CHECK constraint for `asset_verified` values
- [ ] Update `balance_transfers_volume_series_mv_internal` materialized view
- [ ] Update `balance_transfers_volume_series_view`
- [ ] Update `balance_transfers_network_daily_view`
- [ ] Update `balance_transfers_network_weekly_view`
- [ ] Update `balance_transfers_network_monthly_view`
- [ ] Update `balance_transfers_volume_daily_view`
- [ ] Update `balance_transfers_volume_weekly_view`
- [ ] Update `balance_transfers_volume_monthly_view`
- [ ] Update `balance_transfers_address_analytics_view`
- [ ] Update `balance_transfers_volume_trends_view`
- [ ] Update `balance_transfers_address_daily_internal` materialized view
- [ ] Update `balance_transfers_address_weekly_internal` materialized view
- [ ] Update `balance_transfers_address_monthly_internal` materialized view
- [ ] Update corresponding wrapper views for address time series

## Phase 2: Indexer Updates ⏳

### Balance Series Indexers
- [ ] Create asset verification logic function in base indexer
- [ ] Update `balance_series_indexer_base.py`:
  - [ ] Modify `record_balance_series` method to include new fields
  - [ ] Update column_names in insert statement
  - [ ] Add asset_contract and asset_verified to balance_data tuple
- [ ] Update `balance_series_indexer_torus.py` if needed
- [ ] Update `balance_series_indexer_bittensor.py` if needed
- [ ] Update `balance_series_indexer_polkadot.py` if needed

### Balance Transfers Indexers
- [ ] Update `balance_transfers_indexer_base.py`:
  - [ ] Add asset verification logic
  - [ ] Update transfer recording methods
  - [ ] Include new fields in insert statements
- [ ] Update network-specific transfer indexers if needed

### Common Components
- [ ] Create constants file for native assets mapping
- [ ] Add helper function for asset verification determination

## Phase 3: Service Layer Updates ⏳

### Balance Series Service (`balance_series_service.py`)
- [ ] Update `get_address_balance_series`:
  - [ ] Add new fields to SELECT statement
  - [ ] Add new fields to columns list
- [ ] Update `get_current_balances`:
  - [ ] Add new fields to SELECT statement
  - [ ] Add new fields to columns list
- [ ] Update `get_balance_changes`:
  - [ ] Add new fields to SELECT statement
  - [ ] Add new fields to columns list
- [ ] Update `get_balance_aggregations`:
  - [ ] Update daily query
  - [ ] Update weekly query
  - [ ] Update monthly query
- [ ] Update `get_balance_volume_series`:
  - [ ] Include new fields in aggregation

### Balance Transfers Service (`balance_transfers_service.py`)
- [ ] Update `get_address_transactions`:
  - [ ] Add new fields to SELECT statement
  - [ ] Add new fields to columns list
- [ ] Update `get_balance_volume_series`:
  - [ ] Update 4-hour period query
  - [ ] Update daily period query
  - [ ] Update weekly period query
  - [ ] Update monthly period query
- [ ] Update `get_network_analytics`
- [ ] Update `get_address_analytics`
- [ ] Update `get_volume_aggregations`
- [ ] Update `get_volume_trends`
- [ ] Update `get_addresses_analytics`

## Phase 4: MCP Tool Updates ⏳

### Balance Series Tool (`balance_series.py`)
- [ ] Update `schema` method:
  - [ ] Add description for `asset_contract` field
  - [ ] Add description for `asset_verified` field
  - [ ] Update table descriptions to mention new fields

### Balance Transfers Tool (`balance_transfers.py`)
- [ ] Update `schema` method:
  - [ ] Add description for `asset_contract` field
  - [ ] Add description for `asset_verified` field
  - [ ] Update table descriptions to mention new fields

## Phase 5: Testing & Validation ⏳

### Unit Tests
- [ ] Test asset verification logic with native assets
- [ ] Test asset verification logic with unknown assets
- [ ] Test database insertions with new fields
- [ ] Test service layer queries return new fields

### Integration Tests
- [ ] Test full indexing flow with new fields
- [ ] Test API endpoints return new fields
- [ ] Test MCP tools can query new fields
- [ ] Test views and materialized views work correctly

### Manual Testing
- [ ] Verify native assets show as verified
- [ ] Verify unknown assets show as unknown
- [ ] Test filtering by asset_verified status
- [ ] Verify performance is not impacted

## Phase 6: Documentation ⏳

### Code Documentation
- [ ] Update docstrings for modified methods
- [ ] Add comments explaining asset verification logic
- [ ] Document new field purposes in schema files

### API Documentation
- [ ] Update OpenAPI spec if applicable
- [ ] Update endpoint descriptions
- [ ] Add examples showing new fields

### User Documentation
- [ ] Create guide on asset verification statuses
- [ ] Document how to identify potentially malicious assets
- [ ] Add FAQ section about asset contracts

## Phase 7: Deployment ⏳

### Pre-deployment
- [ ] Backup existing data (if any)
- [ ] Review all schema changes
- [ ] Prepare rollback plan

### Deployment Steps
- [ ] Drop existing tables and views
- [ ] Execute updated schema files
- [ ] Deploy updated indexer code
- [ ] Deploy updated API services
- [ ] Start reindexing from block 0

### Post-deployment
- [ ] Monitor indexing progress
- [ ] Verify data integrity
- [ ] Check API responses include new fields
- [ ] Test MCP tools functionality

## Future Enhancements (Post-MVP) 🔮

- [ ] Design asset registry table schema
- [ ] Implement asset registry CRUD operations
- [ ] Add admin interface for asset verification
- [ ] Implement automated verification rules
- [ ] Add community reporting system
- [ ] Create asset verification API endpoints
- [ ] Add asset filtering options to all endpoints
- [ ] Implement verification history tracking
- [ ] Add webhook notifications for malicious assets

## Notes

- 🟢 Completed
- 🟡 In Progress
- 🔴 Blocked
- ⏳ Not Started
- 🔮 Future Enhancement

**Priority Order:**
1. Schema updates (must be done first)
2. Indexer updates (required for data population)
3. Service layer updates (required for API functionality)
4. MCP tool updates (enhances usability)
5. Testing & Documentation (ensures quality)
6. Deployment (final step)

**Estimated Timeline:**
- Phase 1-2: 2-3 days
- Phase 3-4: 2 days
- Phase 5-6: 1-2 days
- Phase 7: 1 day
- Total: ~1 week

**Risk Mitigation:**
- Test all schema changes in development environment first
- Have rollback scripts ready
- Monitor system performance during reindexing
- Communicate changes to API consumers