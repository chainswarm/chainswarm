-- Balance Tracking Schema
-- Tables for tracking balance changes, deltas, and transfers on Substrate networks

-- Balance Changes Table
-- Stores the current balance state for addresses at specific block heights
CREATE TABLE IF NOT EXISTS balance_changes (
    -- Block information
    block_height UInt32,
    block_timestamp UInt64,
    
    -- Address and balance information
    address String,
    asset String,
    free_balance Decimal128(18),
    reserved_balance Decimal128(18),
    staked_balance Decimal128(18),
    total_balance Decimal128(18),
    
    -- Versioning for updates
    _version UInt64,
    
    -- Constraints to ensure data integrity
    CONSTRAINT positive_free_balance CHECK free_balance >= 0,
    CONSTRAINT positive_reserved_balance CHECK reserved_balance >= 0,
    CONSTRAINT positive_staked_balance CHECK staked_balance >= 0,
    CONSTRAINT positive_total_balance CHECK total_balance >= 0
    
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, {PARTITION_SIZE})
ORDER BY (block_height, asset, address)
SETTINGS index_granularity = 8192
COMMENT 'Stores balance snapshots for addresses at specific block heights';

-- Balance Delta Changes Table
-- Stores the changes in balances between consecutive blocks
CREATE TABLE IF NOT EXISTS balance_delta_changes (
    -- Block information
    block_height UInt32,
    block_timestamp UInt64,
    
    -- Address and delta information
    address String,
    asset String,
    free_balance_delta Decimal128(18),
    reserved_balance_delta Decimal128(18),
    staked_balance_delta Decimal128(18),
    total_balance_delta Decimal128(18),
    
    -- Reference to previous state
    previous_block_height UInt32,
    
    -- Versioning for updates
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, {PARTITION_SIZE})
ORDER BY (block_height, asset, address)
SETTINGS index_granularity = 8192
COMMENT 'Stores balance changes (deltas) between consecutive blocks for addresses';

-- Balance Transfers Table
-- Stores individual transfer transactions between addresses
CREATE TABLE IF NOT EXISTS balance_transfers (
    -- Transaction identification
    extrinsic_id String,
    event_idx String,
    
    -- Block information
    block_height UInt32,
    block_timestamp UInt64,
    
    -- Transfer details
    from_address String,
    to_address String,
    asset String,
    amount Decimal128(18),
    fee Decimal128(18),
    
    -- Versioning for updates
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, {PARTITION_SIZE})
ORDER BY (extrinsic_id, event_idx, asset)
SETTINGS index_granularity = 8192
COMMENT 'Stores individual balance transfer transactions with associated fees';

-- Views for easier querying

-- View for current balances (latest balance for each address and asset)
CREATE VIEW IF NOT EXISTS balances_current_view AS
SELECT
    address,
    asset,
    argMax(block_height, block_height) as latest_block_height,
    argMax(block_timestamp, block_height) as latest_block_timestamp,
    argMax(free_balance, block_height) as free_balance,
    argMax(reserved_balance, block_height) as reserved_balance,
    argMax(staked_balance, block_height) as staked_balance,
    argMax(total_balance, block_height) as total_balance
FROM balance_changes
GROUP BY address, asset;

-- View for significant balance changes
CREATE VIEW IF NOT EXISTS balance_significant_changes_view AS
SELECT
    block_height,
    block_timestamp,
    address,
    asset,
    free_balance_delta,
    reserved_balance_delta,
    staked_balance_delta,
    total_balance_delta,
    previous_block_height
FROM balance_delta_changes
WHERE abs(total_balance_delta) > 100  -- Adjusted threshold for human-readable format
ORDER BY block_height DESC, asset, abs(total_balance_delta) DESC;

-- View for transfer statistics by address and asset
CREATE VIEW IF NOT EXISTS balance_transfers_statistics_view AS
SELECT
    address,
    asset,
    countIf(address = from_address) as outgoing_transfer_count,
    countIf(address = to_address) as incoming_transfer_count,
    sumIf(amount, address = from_address) as total_sent,
    sumIf(amount, address = to_address) as total_received,
    sumIf(fee, address = from_address) as total_fees_paid,
    min(block_height) as first_activity_block,
    max(block_height) as last_activity_block
FROM (
    SELECT from_address as address, from_address, to_address, asset, amount, fee, block_height FROM balance_transfers
    UNION ALL
    SELECT to_address as address, from_address, to_address, asset, amount, 0 as fee, block_height FROM balance_transfers
)
GROUP BY address, asset;

-- Materialized view for daily balance statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_daily_statistics_mv
ENGINE = AggregatingMergeTree()
ORDER BY (date, asset, address)
AS
SELECT
    toDate(intDiv(block_timestamp, 1000)) as date,
    address,
    asset,
    argMax(free_balance, block_height) as closing_free_balance,
    argMax(reserved_balance, block_height) as closing_reserved_balance,
    argMax(staked_balance, block_height) as closing_staked_balance,
    argMax(total_balance, block_height) as closing_total_balance,
    max(block_height) as last_block_of_day
FROM balance_changes
GROUP BY date, address, asset;

-- Materialized view for daily transfer volume
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_daily_volume_mv
ENGINE = AggregatingMergeTree()
ORDER BY (date, asset)
AS
SELECT
    toDate(intDiv(block_timestamp, 1000)) as date,
    asset,
    count() as transfer_count,
    uniqExact(from_address) as unique_senders,
    uniqExact(to_address) as unique_receivers,
    sum(amount) as total_volume,
    sum(fee) as total_fees,
    avg(amount) as avg_transfer_amount,
    max(amount) as max_transfer_amount
FROM balance_transfers
GROUP BY date, asset;

-- Index for efficient address lookups
ALTER TABLE balance_changes ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_delta_changes ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_from_address from_address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_to_address to_address TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for efficient asset lookups
ALTER TABLE balance_changes ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_delta_changes ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for block height range queries
ALTER TABLE balance_changes ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;
ALTER TABLE balance_delta_changes ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;

-- Composite indexes for efficient asset-address queries
ALTER TABLE balance_changes ADD INDEX IF NOT EXISTS idx_asset_address (asset, address) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_delta_changes ADD INDEX IF NOT EXISTS idx_asset_address (asset, address) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset_from_address (asset, from_address) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset_to_address (asset, to_address) TYPE bloom_filter(0.01) GRANULARITY 4;

-- Materialized view for available assets
CREATE MATERIALIZED VIEW IF NOT EXISTS available_assets_mv
ENGINE = AggregatingMergeTree()
ORDER BY (asset, total_balance_sum)
AS
SELECT
    asset,
    count(DISTINCT address) as address_count,
    sum(total_balance) as total_balance_sum,
    max(block_height) as last_seen_block
FROM balance_changes
WHERE asset != ''
GROUP BY asset;