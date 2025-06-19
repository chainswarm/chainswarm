-- Balance Tracking Schema
-- Tables for tracking balance changes and deltas on Substrate networks

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


-- Index for efficient address lookups
ALTER TABLE balance_changes ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_delta_changes ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for efficient asset lookups
ALTER TABLE balance_changes ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_delta_changes ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for block height range queries
ALTER TABLE balance_changes ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;
ALTER TABLE balance_delta_changes ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;

-- Composite indexes for efficient asset-address queries
ALTER TABLE balance_changes ADD INDEX IF NOT EXISTS idx_asset_address (asset, address) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_delta_changes ADD INDEX IF NOT EXISTS idx_asset_address (asset, address) TYPE bloom_filter(0.01) GRANULARITY 4;

-- Simple view for available assets list
CREATE VIEW IF NOT EXISTS available_assets_view AS
SELECT DISTINCT asset
FROM balance_changes
WHERE asset != ''
ORDER BY asset;