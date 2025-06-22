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
SETTINGS index_granularity = 8192,
    compress_on_disk = 1,
    min_bytes_for_wide_part = 10485760,
    storage_policy = 'tiered'
COMMENT 'Stores balance snapshots at fixed 4-hour intervals';

-- Note: We track the processing state by querying the last record from the balance_series table
-- instead of maintaining a separate state table. This simplifies the schema and code.

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