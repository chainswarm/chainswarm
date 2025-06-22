-- Balance Volume Series Schema
-- Tables for tracking transfer volumes in time-series format with fixed 4-hour intervals

-- Balance Volume Series Table
-- Stores volume snapshots at fixed 4-hour intervals
CREATE TABLE IF NOT EXISTS balance_volume_series (
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
    
    -- Network identifier and asset information
    network String,
    asset String,
    
    -- Volume information
    total_volume Decimal128(18),
    transaction_count UInt32,
    unique_senders UInt32,
    unique_receivers UInt32,
    
    -- Transaction size categorization
    micro_tx_count UInt32,       -- < 0.1 tokens
    micro_tx_volume Decimal128(18),
    small_tx_count UInt32,       -- 0.1 - 10 tokens
    small_tx_volume Decimal128(18),
    medium_tx_count UInt32,      -- 10 - 100 tokens
    medium_tx_volume Decimal128(18),
    large_tx_count UInt32,       -- 100 - 1000 tokens
    large_tx_volume Decimal128(18),
    whale_tx_count UInt32,       -- > 1000 tokens
    whale_tx_volume Decimal128(18),
    
    -- Fee information
    total_fees Decimal128(18),
    avg_fee Decimal128(18),
    
    -- Network density metrics
    active_addresses UInt32,     -- Total unique addresses active in this period
    network_density Decimal64(6), -- Ratio of active connections to possible connections
    
    -- Changes since last period
    volume_change Decimal128(18),
    volume_percent_change Decimal64(6),
    tx_count_change Int32,
    tx_count_percent_change Decimal64(6),
    
    -- Versioning for updates
    _version UInt64,
    
    -- Constraints to ensure data integrity
    CONSTRAINT positive_volume CHECK total_volume >= 0,
    CONSTRAINT positive_fees CHECK total_fees >= 0
    
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(period_start_timestamp))
ORDER BY (period_start_timestamp, network, asset)
SETTINGS index_granularity = 8192
COMMENT 'Stores transfer volume snapshots at fixed 4-hour intervals';

-- Views for easier querying

-- View for latest volume data for each network and asset
CREATE VIEW IF NOT EXISTS balance_volume_series_latest_view AS
SELECT
    network,
    asset,
    argMax(period_start_timestamp, period_start_timestamp) as latest_period_start,
    argMax(period_end_timestamp, period_start_timestamp) as latest_period_end,
    argMax(block_height, period_start_timestamp) as latest_block_height,
    argMax(total_volume, period_start_timestamp) as total_volume,
    argMax(transaction_count, period_start_timestamp) as transaction_count,
    argMax(unique_senders, period_start_timestamp) as unique_senders,
    argMax(unique_receivers, period_start_timestamp) as unique_receivers,
    argMax(active_addresses, period_start_timestamp) as active_addresses,
    argMax(network_density, period_start_timestamp) as network_density
FROM balance_volume_series
GROUP BY network, asset;

-- View for daily aggregation
CREATE VIEW IF NOT EXISTS balance_volume_series_daily_view AS
SELECT
    toDate(fromUnixTimestamp64Milli(period_start_timestamp)) as date,
    network,
    asset,
    -- Daily aggregations
    sum(total_volume) as daily_volume,
    sum(transaction_count) as daily_tx_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    -- Transaction categories
    sum(micro_tx_count) as daily_micro_tx_count,
    sum(micro_tx_volume) as daily_micro_tx_volume,
    sum(small_tx_count) as daily_small_tx_count,
    sum(small_tx_volume) as daily_small_tx_volume,
    sum(medium_tx_count) as daily_medium_tx_count,
    sum(medium_tx_volume) as daily_medium_tx_volume,
    sum(large_tx_count) as daily_large_tx_count,
    sum(large_tx_volume) as daily_large_tx_volume,
    sum(whale_tx_count) as daily_whale_tx_count,
    sum(whale_tx_volume) as daily_whale_tx_volume,
    -- Fees
    sum(total_fees) as daily_total_fees,
    sum(total_fees)/sum(transaction_count) as daily_avg_fee,
    -- Network metrics
    max(active_addresses) as max_active_addresses,
    avg(network_density) as avg_network_density
FROM balance_volume_series
GROUP BY date, network, asset;

-- View for high-volume periods
CREATE VIEW IF NOT EXISTS balance_volume_series_high_volume_view AS
SELECT
    period_start_timestamp,
    period_end_timestamp,
    block_height,
    network,
    asset,
    total_volume,
    transaction_count,
    unique_senders,
    unique_receivers,
    whale_tx_count,
    whale_tx_volume,
    network_density
FROM balance_volume_series
WHERE total_volume > 1000 OR whale_tx_count > 0
ORDER BY period_start_timestamp DESC, total_volume DESC;

-- Materialized view for weekly volume statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_volume_series_weekly_mv
ENGINE = AggregatingMergeTree()
ORDER BY (week_start, network, asset)
AS
SELECT
    toStartOfWeek(fromUnixTimestamp64Milli(period_start_timestamp)) as week_start,
    network,
    asset,
    sum(total_volume) as weekly_volume,
    sum(transaction_count) as weekly_tx_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    sum(total_fees) as weekly_total_fees,
    max(active_addresses) as max_active_addresses,
    avg(network_density) as avg_network_density,
    -- Transaction categories
    sum(micro_tx_count) as weekly_micro_tx_count,
    sum(micro_tx_volume) as weekly_micro_tx_volume,
    sum(small_tx_count) as weekly_small_tx_count, 
    sum(small_tx_volume) as weekly_small_tx_volume,
    sum(medium_tx_count) as weekly_medium_tx_count,
    sum(medium_tx_volume) as weekly_medium_tx_volume,
    sum(large_tx_count) as weekly_large_tx_count,
    sum(large_tx_volume) as weekly_large_tx_volume,
    sum(whale_tx_count) as weekly_whale_tx_count,
    sum(whale_tx_volume) as weekly_whale_tx_volume,
    max(block_height) as last_block_of_week
FROM balance_volume_series
GROUP BY week_start, network, asset;

-- Materialized view for monthly volume statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_volume_series_monthly_mv
ENGINE = AggregatingMergeTree()
ORDER BY (month_start, network, asset)
AS
SELECT
    toStartOfMonth(fromUnixTimestamp64Milli(period_start_timestamp)) as month_start,
    network,
    asset,
    sum(total_volume) as monthly_volume,
    sum(transaction_count) as monthly_tx_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    sum(total_fees) as monthly_total_fees,
    max(active_addresses) as max_active_addresses,
    avg(network_density) as avg_network_density,
    -- Transaction categories
    sum(micro_tx_count) as monthly_micro_tx_count,
    sum(micro_tx_volume) as monthly_micro_tx_volume,
    sum(small_tx_count) as monthly_small_tx_count, 
    sum(small_tx_volume) as monthly_small_tx_volume,
    sum(medium_tx_count) as monthly_medium_tx_count,
    sum(medium_tx_volume) as monthly_medium_tx_volume,
    sum(large_tx_count) as monthly_large_tx_count,
    sum(large_tx_volume) as monthly_large_tx_volume,
    sum(whale_tx_count) as monthly_whale_tx_count,
    sum(whale_tx_volume) as monthly_whale_tx_volume,
    max(block_height) as last_block_of_month
FROM balance_volume_series
GROUP BY month_start, network, asset;

-- Indexes for efficient querying

-- Index for efficient network lookups
ALTER TABLE balance_volume_series ADD INDEX IF NOT EXISTS idx_network network TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for efficient asset lookups
ALTER TABLE balance_volume_series ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for period timestamp range queries
ALTER TABLE balance_volume_series ADD INDEX IF NOT EXISTS idx_period_start period_start_timestamp TYPE minmax GRANULARITY 4;
ALTER TABLE balance_volume_series ADD INDEX IF NOT EXISTS idx_period_end period_end_timestamp TYPE minmax GRANULARITY 4;

-- Index for block height range queries
ALTER TABLE balance_volume_series ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;

-- Composite indexes for efficient network-asset queries
ALTER TABLE balance_volume_series ADD INDEX IF NOT EXISTS idx_network_asset (network, asset) TYPE bloom_filter(0.01) GRANULARITY 4;

-- View for volume trend analysis
CREATE VIEW IF NOT EXISTS balance_volume_series_trends_view AS
SELECT
    toStartOfDay(fromUnixTimestamp64Milli(period_start_timestamp)) as day,
    network,
    asset,
    sum(total_volume) as daily_volume,
    sum(transaction_count) as daily_tx_count,
    sum(micro_tx_volume) / sum(total_volume) * 100 as micro_tx_percent,
    sum(small_tx_volume) / sum(total_volume) * 100 as small_tx_percent,
    sum(medium_tx_volume) / sum(total_volume) * 100 as medium_tx_percent,
    sum(large_tx_volume) / sum(total_volume) * 100 as large_tx_percent,
    sum(whale_tx_volume) / sum(total_volume) * 100 as whale_tx_percent
FROM balance_volume_series
GROUP BY day, network, asset
ORDER BY day;

-- Simple view for available networks and assets
CREATE VIEW IF NOT EXISTS balance_volume_series_available_assets_view AS
SELECT DISTINCT network, asset
FROM balance_volume_series
WHERE asset != ''
ORDER BY network, asset;