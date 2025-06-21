-- Balance Series Schema
-- Tables for tracking balance changes in time-series format with fixed 4-hour intervals

-- Balance Series Table
-- Stores balance snapshots at fixed 4-hour intervals
CREATE TABLE IF NOT EXISTS balance_series (
    -- Time period information
    -- Start of the 4-hour period - Unix timestamp in milliseconds
    period_start_timestamp UInt64,
    -- End of the 4-hour period - Unix timestamp in milliseconds
    period_end_timestamp UInt64,
    
    -- Block information for the snapshot
    -- Block height at the end of the period
    block_height UInt32,
    -- Block hash at the end of the period
    block_hash String,
    
    -- Address and balance information
    address String,
    asset String,
    free_balance Decimal128(18),
    reserved_balance Decimal128(18),
    staked_balance Decimal128(18),
    total_balance Decimal128(18),
    
    -- Change since last period
    free_balance_change Decimal128(18),
    reserved_balance_change Decimal128(18),
    staked_balance_change Decimal128(18),
    total_balance_change Decimal128(18),
    
    -- Percentage change since last period
    total_balance_percent_change Decimal64(6),
    
    -- Versioning for updates
    _version UInt64,
    
    -- Constraints to ensure data integrity
    CONSTRAINT positive_free_balance CHECK free_balance >= 0,
    CONSTRAINT positive_reserved_balance CHECK reserved_balance >= 0,
    CONSTRAINT positive_staked_balance CHECK staked_balance >= 0,
    CONSTRAINT positive_total_balance CHECK total_balance >= 0
    
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(period_start_timestamp))
ORDER BY (period_start_timestamp, asset, address)
SETTINGS index_granularity = 8192
COMMENT 'Stores balance snapshots at fixed 4-hour intervals';

-- Balance Series Processing State
-- Tracks the processing state of the balance series indexer
CREATE TABLE IF NOT EXISTS balance_series_state (
    -- Network and asset information
    network String,
    asset String,
    
    -- Last processed period
    -- End timestamp of the last processed period
    last_processed_timestamp UInt64,
    -- Block height at the end of the last processed period
    last_processed_block_height UInt32,
    
    -- Next period to process
    -- Start timestamp of the next period to process
    next_period_timestamp UInt64,
    
    -- Versioning for updates
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
SETTINGS index_granularity = 8192
COMMENT 'Tracks the processing state of the balance series indexer';

-- Views for easier querying

-- View for latest balance for each address and asset
CREATE VIEW IF NOT EXISTS balance_series_latest_view AS
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
GROUP BY address, asset;

-- View for daily aggregation
CREATE VIEW IF NOT EXISTS balance_series_daily_view AS
SELECT
    toDate(fromUnixTimestamp64Milli(period_start_timestamp)) as date,
    address,
    asset,
    -- Take the last period of each day
    argMax(free_balance, period_start_timestamp) as end_of_day_free_balance,
    argMax(reserved_balance, period_start_timestamp) as end_of_day_reserved_balance,
    argMax(staked_balance, period_start_timestamp) as end_of_day_staked_balance,
    argMax(total_balance, period_start_timestamp) as end_of_day_total_balance,
    -- Calculate daily change
    sum(free_balance_change) as daily_free_balance_change,
    sum(reserved_balance_change) as daily_reserved_balance_change,
    sum(staked_balance_change) as daily_staked_balance_change,
    sum(total_balance_change) as daily_total_balance_change
FROM balance_series
GROUP BY date, address, asset;

-- View for significant balance changes (>1% or >100 tokens)
CREATE VIEW IF NOT EXISTS balance_series_significant_changes_view AS
SELECT
    period_start_timestamp,
    period_end_timestamp,
    block_height,
    address,
    asset,
    free_balance,
    reserved_balance,
    staked_balance,
    total_balance,
    free_balance_change,
    reserved_balance_change,
    staked_balance_change,
    total_balance_change,
    total_balance_percent_change
FROM balance_series
WHERE abs(total_balance_percent_change) > 0.01 OR abs(total_balance_change) > 100
ORDER BY period_start_timestamp DESC, abs(total_balance_percent_change) DESC;

-- Materialized view for weekly balance statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_series_weekly_mv
ENGINE = AggregatingMergeTree()
ORDER BY (week_start, asset, address)
AS
SELECT
    toStartOfWeek(fromUnixTimestamp64Milli(period_start_timestamp)) as week_start,
    address,
    asset,
    argMax(free_balance, period_start_timestamp) as end_of_week_free_balance,
    argMax(reserved_balance, period_start_timestamp) as end_of_week_reserved_balance,
    argMax(staked_balance, period_start_timestamp) as end_of_week_staked_balance,
    argMax(total_balance, period_start_timestamp) as end_of_week_total_balance,
    sum(free_balance_change) as weekly_free_balance_change,
    sum(reserved_balance_change) as weekly_reserved_balance_change,
    sum(staked_balance_change) as weekly_staked_balance_change,
    sum(total_balance_change) as weekly_total_balance_change,
    max(block_height) as last_block_of_week
FROM balance_series
GROUP BY week_start, address, asset;

-- Materialized view for monthly balance statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_series_monthly_mv
ENGINE = AggregatingMergeTree()
ORDER BY (month_start, asset, address)
AS
SELECT
    toStartOfMonth(fromUnixTimestamp64Milli(period_start_timestamp)) as month_start,
    address,
    asset,
    argMax(free_balance, period_start_timestamp) as end_of_month_free_balance,
    argMax(reserved_balance, period_start_timestamp) as end_of_month_reserved_balance,
    argMax(staked_balance, period_start_timestamp) as end_of_month_staked_balance,
    argMax(total_balance, period_start_timestamp) as end_of_month_total_balance,
    sum(free_balance_change) as monthly_free_balance_change,
    sum(reserved_balance_change) as monthly_reserved_balance_change,
    sum(staked_balance_change) as monthly_staked_balance_change,
    sum(total_balance_change) as monthly_total_balance_change,
    max(block_height) as last_block_of_month
FROM balance_series
GROUP BY month_start, address, asset;

-- Indexes for efficient querying

-- Index for efficient address lookups
ALTER TABLE balance_series ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for efficient asset lookups
ALTER TABLE balance_series ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for period timestamp range queries
ALTER TABLE balance_series ADD INDEX IF NOT EXISTS idx_period_start period_start_timestamp TYPE minmax GRANULARITY 4;
ALTER TABLE balance_series ADD INDEX IF NOT EXISTS idx_period_end period_end_timestamp TYPE minmax GRANULARITY 4;

-- Index for block height range queries
ALTER TABLE balance_series ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;

-- Composite indexes for efficient asset-address queries
ALTER TABLE balance_series ADD INDEX IF NOT EXISTS idx_asset_address (asset, address) TYPE bloom_filter(0.01) GRANULARITY 4;

-- Simple view for available assets list
CREATE VIEW IF NOT EXISTS balance_series_available_assets_view AS
SELECT DISTINCT asset
FROM balance_series
WHERE asset != ''
ORDER BY asset;