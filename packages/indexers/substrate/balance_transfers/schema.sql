-- Balance Transfers Schema
-- Tables and views for tracking individual transfer transactions on Substrate networks
-- Simplified version with asset-specific hardcoded thresholds

-- Drop potentially invalid objects first to ensure clean recreation
DROP VIEW IF EXISTS balance_transfers_address_relationships_mv;
DROP VIEW IF EXISTS balance_transfers_pairs_analysis_view;
DROP VIEW IF EXISTS balance_transfers_address_clusters_view;
DROP VIEW IF EXISTS balance_transfers_suspicious_activity_view;
DROP VIEW IF EXISTS balance_transfers_address_activity_patterns_view;
DROP VIEW IF EXISTS balance_transfers_address_classification_view;
DROP VIEW IF EXISTS balance_transfers_address_behavior_profiles_view;

-- =============================================================================
-- CORE TABLES
-- =============================================================================

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
SETTINGS
    index_granularity = 8192,
    compress_on_disk = 1,
    min_bytes_for_wide_part = 10485760,
    storage_policy = 'tiered'
COMMENT 'Stores individual balance transfer transactions with associated fees';

-- =============================================================================
-- INDEXES FOR EFFICIENT QUERIES
-- =============================================================================

-- Index for efficient address lookups
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_from_address from_address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_to_address to_address TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for efficient asset lookups
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for block height range queries
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;

-- Composite indexes for efficient asset-address queries
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset_from_address (asset, from_address) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset_to_address (asset, to_address) TYPE bloom_filter(0.01) GRANULARITY 4;

-- Indexes for address behavior profiles
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_address_timestamp (from_address, block_timestamp) TYPE minmax GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_to_address_timestamp (to_address, block_timestamp) TYPE minmax GRANULARITY 4;

-- Indexes for amount-based queries
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_amount_range amount TYPE minmax GRANULARITY 4;

-- Indexes for time-based analysis
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_hour_of_day toHour(toDateTime(intDiv(block_timestamp, 1000))) TYPE set(24) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_date toDate(toDateTime(intDiv(block_timestamp, 1000))) TYPE minmax GRANULARITY 4;

-- Composite indexes for relationship analysis
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_from_to_asset (from_address, to_address, asset) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset_amount_timestamp (asset, amount, block_timestamp) TYPE minmax GRANULARITY 4;

-- =============================================================================
-- INDEXES FOR VOLUME SERIES MATERIALIZED VIEW
-- =============================================================================

-- Index for efficient period start lookups
ALTER TABLE balance_transfers_volume_series_mv ADD INDEX IF NOT EXISTS idx_period_start period_start TYPE minmax GRANULARITY 4;

-- Index for efficient asset lookups
ALTER TABLE balance_transfers_volume_series_mv ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;

-- Composite index for efficient period-asset queries
ALTER TABLE balance_transfers_volume_series_mv ADD INDEX IF NOT EXISTS idx_period_asset (period_start, asset) TYPE bloom_filter(0.01) GRANULARITY 4;

-- Index for volume range queries
ALTER TABLE balance_transfers_volume_series_mv ADD INDEX IF NOT EXISTS idx_total_volume total_volume TYPE minmax GRANULARITY 4;

-- Index for transaction count range queries
ALTER TABLE balance_transfers_volume_series_mv ADD INDEX IF NOT EXISTS idx_transaction_count transaction_count TYPE minmax GRANULARITY 4;

-- =============================================================================
-- BASIC VIEWS
-- =============================================================================

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

-- =============================================================================
-- VOLUME SERIES MATERIALIZED VIEWS (4-Hour Intervals)
-- Asset-agnostic design with client-defined categorization
-- =============================================================================

-- Base 4-hour interval materialized view for volume series
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_volume_series_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(period_start)
ORDER BY (period_start, asset)
SETTINGS
    index_granularity = 8192,
    compress_on_disk = 1,
    min_bytes_for_wide_part = 10485760,
    storage_policy = 'tiered'
AS
SELECT
    -- =============================================================================
    -- TIME PERIOD INFORMATION (UTC-based 4-hour intervals aligned to midnight)
    -- =============================================================================
    toStartOfDay(toDateTime(intDiv(block_timestamp, 1000))) +
    INTERVAL (intDiv(toHour(toDateTime(intDiv(block_timestamp, 1000))), 4) * 4) HOUR as period_start,
    toStartOfDay(toDateTime(intDiv(block_timestamp, 1000))) +
    INTERVAL ((intDiv(toHour(toDateTime(intDiv(block_timestamp, 1000))), 4) + 1) * 4) HOUR as period_end,
    
    -- =============================================================================
    -- ASSET INFORMATION
    -- =============================================================================
    asset,
    
    -- =============================================================================
    -- BASIC VOLUME METRICS
    -- =============================================================================
    count() as transaction_count,
    uniqExact(from_address) as unique_senders,
    uniqExact(to_address) as unique_receivers,
    sum(amount) as total_volume,
    sum(fee) as total_fees,
    avg(amount) as avg_transfer_amount,
    max(amount) as max_transfer_amount,
    min(amount) as min_transfer_amount,
    median(amount) as median_transfer_amount,
    
    -- =============================================================================
    -- AMOUNT DISTRIBUTION QUANTILES (Client can define thresholds)
    -- =============================================================================
    quantile(0.10)(amount) as amount_p10,
    quantile(0.25)(amount) as amount_p25,
    quantile(0.50)(amount) as amount_p50,
    quantile(0.75)(amount) as amount_p75,
    quantile(0.90)(amount) as amount_p90,
    quantile(0.95)(amount) as amount_p95,
    quantile(0.99)(amount) as amount_p99,
    
    -- =============================================================================
    -- NETWORK ACTIVITY METRICS
    -- =============================================================================
    uniq(from_address, to_address) as unique_address_pairs,
    uniqExact(from_address) + uniqExact(to_address) as active_addresses,
    
    -- Network density calculation (ratio of actual connections to possible connections)
    CASE
        WHEN (uniqExact(from_address) + uniqExact(to_address)) > 1 THEN
            toFloat64(uniq(from_address, to_address)) /
            (toFloat64(uniqExact(from_address) + uniqExact(to_address)) *
             toFloat64(uniqExact(from_address) + uniqExact(to_address) - 1) / 2.0)
        ELSE 0.0
    END as network_density,
    
    -- =============================================================================
    -- TEMPORAL ACTIVITY PATTERNS
    -- =============================================================================
    
    -- Activity by time of day (4-hour period start hour)
    toHour(period_start) as period_hour,
    
    -- Activity distribution within the 4-hour window
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(period_start)) as hour_0_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(period_start) + 1) as hour_1_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(period_start) + 2) as hour_2_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(period_start) + 3) as hour_3_tx_count,
    
    -- =============================================================================
    -- STATISTICAL MEASURES
    -- =============================================================================
    stddevPop(amount) as amount_std_dev,
    varPop(amount) as amount_variance,
    skewPop(amount) as amount_skewness,
    kurtPop(amount) as amount_kurtosis,
    
    -- Fee statistics
    avg(fee) as avg_fee,
    max(fee) as max_fee,
    min(fee) as min_fee,
    stddevPop(fee) as fee_std_dev,
    median(fee) as median_fee,
    
    -- =============================================================================
    -- BLOCK INFORMATION
    -- =============================================================================
    min(block_height) as period_start_block,
    max(block_height) as period_end_block,
    max(block_height) - min(block_height) + 1 as blocks_in_period,
    
    -- =============================================================================
    -- HISTOGRAM BINS FOR CLIENT-SIDE CATEGORIZATION
    -- =============================================================================
    
    -- Amount histogram bins (optimized ranges)
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    
    -- Volume histogram bins (corresponding volumes)
    sumIf(amount, amount < 0.1) as volume_lt_01,
    sumIf(amount, amount >= 0.1 AND amount < 1) as volume_01_to_1,
    sumIf(amount, amount >= 1 AND amount < 10) as volume_1_to_10,
    sumIf(amount, amount >= 10 AND amount < 100) as volume_10_to_100,
    sumIf(amount, amount >= 100 AND amount < 1000) as volume_100_to_1k,
    sumIf(amount, amount >= 1000 AND amount < 10000) as volume_1k_to_10k,
    sumIf(amount, amount >= 10000) as volume_gte_10k

FROM balance_transfers
GROUP BY
    period_start,
    period_end,
    asset
COMMENT 'Base 4-hour interval materialized view for transfer volume analysis - asset agnostic with client-defined categorization';

-- =============================================================================
-- AGGREGATION VIEWS (Daily, Weekly, Monthly)
-- =============================================================================

-- Daily aggregation view
CREATE VIEW IF NOT EXISTS balance_transfers_volume_daily_view AS
SELECT
    toDate(period_start) as date,
    asset,
    
    -- Basic aggregations
    sum(transaction_count) as daily_transaction_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    sum(total_volume) as daily_total_volume,
    sum(total_fees) as daily_total_fees,
    sum(total_volume) / sum(transaction_count) as daily_avg_transfer_amount,
    max(max_transfer_amount) as daily_max_transfer_amount,
    min(min_transfer_amount) as daily_min_transfer_amount,
    
    -- Network metrics
    max(active_addresses) as max_daily_active_addresses,
    avg(network_density) as avg_daily_network_density,
    
    -- Statistical aggregations
    avg(amount_std_dev) as avg_daily_amount_std_dev,
    avg(median_transfer_amount) as avg_daily_median_amount,
    
    -- Histogram aggregations
    sum(tx_count_lt_01) as daily_tx_count_lt_01,
    sum(tx_count_01_to_1) as daily_tx_count_01_to_1,
    sum(tx_count_1_to_10) as daily_tx_count_1_to_10,
    sum(tx_count_10_to_100) as daily_tx_count_10_to_100,
    sum(tx_count_100_to_1k) as daily_tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as daily_tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as daily_tx_count_gte_10k,
    
    sum(volume_lt_01) as daily_volume_lt_01,
    sum(volume_01_to_1) as daily_volume_01_to_1,
    sum(volume_1_to_10) as daily_volume_1_to_10,
    sum(volume_10_to_100) as daily_volume_10_to_100,
    sum(volume_100_to_1k) as daily_volume_100_to_1k,
    sum(volume_1k_to_10k) as daily_volume_1k_to_10k,
    sum(volume_gte_10k) as daily_volume_gte_10k,
    
    -- Block information
    min(period_start_block) as daily_start_block,
    max(period_end_block) as daily_end_block
    
FROM balance_transfers_volume_series_mv
GROUP BY date, asset
ORDER BY date DESC, asset;

-- Weekly aggregation view
CREATE VIEW IF NOT EXISTS balance_transfers_volume_weekly_view AS
SELECT
    toStartOfWeek(period_start) as week_start,
    asset,
    
    -- Basic aggregations
    sum(transaction_count) as weekly_transaction_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    sum(total_volume) as weekly_total_volume,
    sum(total_fees) as weekly_total_fees,
    sum(total_volume) / sum(transaction_count) as weekly_avg_transfer_amount,
    max(max_transfer_amount) as weekly_max_transfer_amount,
    
    -- Network metrics
    max(active_addresses) as max_weekly_active_addresses,
    avg(network_density) as avg_weekly_network_density,
    
    -- Histogram aggregations
    sum(tx_count_lt_01) as weekly_tx_count_lt_01,
    sum(tx_count_01_to_1) as weekly_tx_count_01_to_1,
    sum(tx_count_1_to_10) as weekly_tx_count_1_to_10,
    sum(tx_count_10_to_100) as weekly_tx_count_10_to_100,
    sum(tx_count_100_to_1k) as weekly_tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as weekly_tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as weekly_tx_count_gte_10k,
    
    sum(volume_lt_01) as weekly_volume_lt_01,
    sum(volume_01_to_1) as weekly_volume_01_to_1,
    sum(volume_1_to_10) as weekly_volume_1_to_10,
    sum(volume_10_to_100) as weekly_volume_10_to_100,
    sum(volume_100_to_1k) as weekly_volume_100_to_1k,
    sum(volume_1k_to_10k) as weekly_volume_1k_to_10k,
    sum(volume_gte_10k) as weekly_volume_gte_10k,
    
    -- Block information
    min(period_start_block) as weekly_start_block,
    max(period_end_block) as weekly_end_block
    
FROM balance_transfers_volume_series_mv
GROUP BY week_start, asset
ORDER BY week_start DESC, asset;

-- Monthly aggregation view
CREATE VIEW IF NOT EXISTS balance_transfers_volume_monthly_view AS
SELECT
    toStartOfMonth(period_start) as month_start,
    asset,
    
    -- Basic aggregations
    sum(transaction_count) as monthly_transaction_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    sum(total_volume) as monthly_total_volume,
    sum(total_fees) as monthly_total_fees,
    sum(total_volume) / sum(transaction_count) as monthly_avg_transfer_amount,
    max(max_transfer_amount) as monthly_max_transfer_amount,
    
    -- Network metrics
    max(active_addresses) as max_monthly_active_addresses,
    avg(network_density) as avg_monthly_network_density,
    
    -- Histogram aggregations
    sum(tx_count_lt_01) as monthly_tx_count_lt_01,
    sum(tx_count_01_to_1) as monthly_tx_count_01_to_1,
    sum(tx_count_1_to_10) as monthly_tx_count_1_to_10,
    sum(tx_count_10_to_100) as monthly_tx_count_10_to_100,
    sum(tx_count_100_to_1k) as monthly_tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as monthly_tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as monthly_tx_count_gte_10k,
    
    sum(volume_lt_01) as monthly_volume_lt_01,
    sum(volume_01_to_1) as monthly_volume_01_to_1,
    sum(volume_1_to_10) as monthly_volume_1_to_10,
    sum(volume_10_to_100) as monthly_volume_10_to_100,
    sum(volume_100_to_1k) as monthly_volume_100_to_1k,
    sum(volume_1k_to_10k) as monthly_volume_1k_to_10k,
    sum(volume_gte_10k) as monthly_volume_gte_10k,
    
    -- Block information
    min(period_start_block) as monthly_start_block,
    max(period_end_block) as monthly_end_block
    
FROM balance_transfers_volume_series_mv
GROUP BY month_start, asset
ORDER BY month_start DESC, asset;

-- =============================================================================
-- ANALYSIS VIEWS
-- =============================================================================

-- Volume trends view with rolling averages
CREATE VIEW IF NOT EXISTS balance_transfers_volume_trends_view AS
SELECT
    period_start,
    asset,
    total_volume,
    transaction_count,
    
    -- Rolling averages (7 periods = ~28 hours)
    avg(total_volume) OVER (
        PARTITION BY asset
        ORDER BY period_start
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7_period_avg_volume,
    
    avg(transaction_count) OVER (
        PARTITION BY asset
        ORDER BY period_start
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7_period_avg_tx_count,
    
    -- Rolling averages (30 periods = ~5 days)
    avg(total_volume) OVER (
        PARTITION BY asset
        ORDER BY period_start
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_30_period_avg_volume,
    
    -- Volume change from previous period
    total_volume - lag(total_volume, 1) OVER (
        PARTITION BY asset
        ORDER BY period_start
    ) as volume_change,
    
    -- Percentage change from previous period
    CASE
        WHEN lag(total_volume, 1) OVER (PARTITION BY asset ORDER BY period_start) > 0 THEN
            ((total_volume - lag(total_volume, 1) OVER (PARTITION BY asset ORDER BY period_start)) /
             lag(total_volume, 1) OVER (PARTITION BY asset ORDER BY period_start)) * 100
        ELSE 0
    END as volume_change_percent
    
FROM balance_transfers_volume_series_mv
ORDER BY period_start DESC, asset;

-- Volume quantiles view for distribution analysis
CREATE VIEW IF NOT EXISTS balance_transfers_volume_quantiles_view AS
SELECT
    toDate(period_start) as date,
    asset,
    
    -- Volume quantiles
    quantile(0.10)(total_volume) as q10_volume,
    quantile(0.25)(total_volume) as q25_volume,
    quantile(0.50)(total_volume) as median_volume,
    quantile(0.75)(total_volume) as q75_volume,
    quantile(0.90)(total_volume) as q90_volume,
    quantile(0.99)(total_volume) as q99_volume,
    
    -- Transaction count quantiles
    quantile(0.10)(transaction_count) as q10_tx_count,
    quantile(0.50)(transaction_count) as median_tx_count,
    quantile(0.90)(transaction_count) as q90_tx_count,
    
    -- Average transaction size (derived measure)
    avg(total_volume / transaction_count) as avg_tx_size,
    
    -- Fee quantiles
    quantile(0.10)(avg_fee) as q10_fee,
    quantile(0.50)(avg_fee) as median_fee,
    quantile(0.90)(avg_fee) as q90_fee
    
FROM balance_transfers_volume_series_mv
GROUP BY date, asset
ORDER BY date DESC, asset;

-- Simple view for available assets list
CREATE VIEW IF NOT EXISTS available_transfer_assets_view AS
SELECT DISTINCT asset
FROM balance_transfers
WHERE asset != ''
ORDER BY asset;

-- =============================================================================
-- ADDRESS BEHAVIOR PROFILES VIEW
-- =============================================================================

-- Address Behavior Profiles View (Simplified for debugging)
-- Comprehensive behavioral analysis for each address with asset-specific thresholds
CREATE VIEW IF NOT EXISTS balance_transfers_address_behavior_profiles_view AS
SELECT
    address,
    asset,
    
    -- Basic Activity Metrics
    count() as total_transactions,
    countIf(address = from_address) as outgoing_count,
    countIf(address = to_address) as incoming_count,
    
    -- Volume Metrics
    sumIf(amount, address = from_address) as total_sent,
    sumIf(amount, address = to_address) as total_received,
    sum(amount) as total_volume,
    
    -- Statistical Metrics
    avgIf(amount, address = from_address) as avg_sent_amount,
    avgIf(amount, address = to_address) as avg_received_amount,
    
    -- Temporal Patterns
    min(block_timestamp) as first_activity,
    max(block_timestamp) as last_activity,
    max(block_timestamp) - min(block_timestamp) as activity_span_seconds,
    
    -- Unique Counterparties
    uniqIf(to_address, address = from_address) as unique_recipients,
    uniqIf(from_address, address = to_address) as unique_senders,
    
    -- Fee Analysis
    sumIf(fee, address = from_address) as total_fees_paid,
    avgIf(fee, address = from_address) as avg_fee_paid,
    maxIf(fee, address = from_address) as max_fee_paid,
    
    -- Activity Distribution by Time of Day
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 0 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) <= 5) as night_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 6 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) <= 11) as morning_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 12 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) <= 17) as afternoon_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 18 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) <= 23) as evening_transactions,
    
    -- Asset-agnostic transaction size histogram
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    
    -- Behavioral Indicators
    stddevPopIf(amount, address = from_address) as sent_amount_variance,
    stddevPopIf(amount, address = to_address) as received_amount_variance,
    uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as active_days

FROM (
    SELECT from_address as address, from_address, to_address, asset, amount, fee, block_height, block_timestamp FROM balance_transfers
    UNION ALL
    SELECT to_address as address, from_address, to_address, asset, amount, 0 as fee, block_height, block_timestamp FROM balance_transfers
)
GROUP BY address, asset
HAVING total_transactions >= 1;

-- =============================================================================
-- ADDRESS CLASSIFICATION VIEW
-- =============================================================================

-- Address Classification View (Independent)
-- Classify addresses into behavioral categories with asset-specific thresholds
CREATE VIEW IF NOT EXISTS balance_transfers_address_classification_view AS
SELECT
    address,
    asset,
    total_volume,
    total_transactions,
    unique_recipients,
    unique_senders,
    
    -- Asset-agnostic classification logic based on behavioral patterns
    CASE
        WHEN total_volume >= 100000 AND unique_recipients >= 100 THEN 'Exchange'
        WHEN total_volume >= 100000 AND unique_recipients < 10 THEN 'Whale'
        WHEN total_volume >= 10000 AND total_transactions >= 1000 THEN 'High_Volume_Trader'
        WHEN unique_recipients >= 50 AND unique_senders >= 50 THEN 'Hub_Address'
        WHEN total_transactions >= 100 AND total_volume < 1000 THEN 'Retail_Active'
        WHEN total_transactions < 10 AND total_volume >= 10000 THEN 'Whale_Inactive'
        WHEN total_transactions < 10 AND total_volume < 100 THEN 'Retail_Inactive'
        ELSE 'Regular_User'
    END as address_type,
    
    -- Asset-agnostic volume classification
    CASE
        WHEN total_volume >= 100000 THEN 'Ultra_High'
        WHEN total_volume >= 10000 THEN 'High'
        WHEN total_volume >= 1000 THEN 'Medium'
        WHEN total_volume >= 100 THEN 'Low'
        ELSE 'Micro'
    END as volume_tier
    
FROM balance_transfers_address_behavior_profiles_view;

-- =============================================================================
-- SUSPICIOUS ACTIVITY DETECTION VIEW
-- =============================================================================

-- Suspicious Activity Detection View (Corrected)
-- Identify potentially suspicious or anomalous activity patterns
CREATE VIEW IF NOT EXISTS balance_transfers_suspicious_activity_view AS
SELECT
    abp.address,
    abp.asset,
    abp.total_transactions,
    abp.total_volume,
    
    -- Suspicious Pattern Indicators
    CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END as unusual_time_pattern,
    CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END as fixed_amount_pattern,
    CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END as single_recipient_pattern,
    CASE WHEN abp.tx_count_gte_10k > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END as large_infrequent_pattern,
    
    -- Composite Suspicion Score (0-4)
    (CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
     CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
     CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
     CASE WHEN abp.tx_count_gte_10k > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) as suspicion_score,
    
    -- Risk Level
    CASE
        WHEN (CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.tx_count_gte_10k > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 3 THEN 'High'
        WHEN (CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.tx_count_gte_10k > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 2 THEN 'Medium'
        WHEN (CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.tx_count_gte_10k > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 1 THEN 'Low'
        ELSE 'Normal'
    END as risk_level
    
FROM balance_transfers_address_behavior_profiles_view abp
WHERE abp.total_transactions >= 5  -- Minimum transactions for analysis
ORDER BY suspicion_score DESC, abp.total_volume DESC;

-- =============================================================================
-- RELATIONSHIP ANALYSIS VIEWS
-- =============================================================================

-- Address Relationships View
-- Track and analyze relationships between addresses based on transfer patterns
CREATE VIEW IF NOT EXISTS balance_transfers_address_relationships_view AS
SELECT
    from_address,
    to_address,
    asset,
    count() as transfer_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    
    -- Relationship Strength Score
    least((transfer_count * 0.4 + log10(total_amount + 1) * 0.6) * 2.0, 10.0) as relationship_strength,
    
    -- Asset-agnostic relationship type classification
    CASE
        WHEN total_amount >= 10000 AND transfer_count >= 10 THEN 'High_Value'
        WHEN transfer_count >= 100 THEN 'High_Frequency'
        WHEN transfer_count >= 5 THEN 'Regular'
        ELSE 'Casual'
    END as relationship_type
    
FROM balance_transfers
WHERE from_address != to_address
GROUP BY from_address, to_address, asset
HAVING count() >= 2  -- Filter for significant relationships
ORDER BY relationship_strength DESC;

-- =============================================================================
-- ACTIVITY PATTERNS VIEW
-- =============================================================================

-- Address Activity Patterns View
-- Analyze temporal and behavioral patterns in address activity
CREATE VIEW IF NOT EXISTS balance_transfers_address_activity_patterns_view AS
SELECT
    address,
    asset,
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as activity_date,
    
    -- Daily Activity Metrics
    count() as daily_transactions,
    sum(amount) as daily_volume,
    countIf(address = from_address) as daily_outgoing,
    countIf(address = to_address) as daily_incoming,
    
    -- Hourly Distribution Analysis
    groupArray(toHour(toDateTime(intDiv(block_timestamp, 1000)))) as hourly_activity,
    uniq(toHour(toDateTime(intDiv(block_timestamp, 1000)))) as active_hours,
    toUInt8(avg(toHour(toDateTime(intDiv(block_timestamp, 1000))))) as peak_hour,
    
    -- Asset-agnostic transaction size histogram
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    
    -- Statistical Measures
    avg(amount) as daily_avg_amount,
    median(amount) as daily_median_amount,
    stddevPop(amount) as daily_amount_variance,
    
    -- Counterparty Analysis
    uniqIf(to_address, address = from_address) as daily_unique_recipients,
    uniqIf(from_address, address = to_address) as daily_unique_senders,
    uniq(if(address = from_address, to_address, from_address)) as daily_unique_counterparties,
    
    -- Fee Patterns
    sumIf(fee, address = from_address) as daily_fees_paid,
    avgIf(fee, address = from_address) as avg_daily_fee
    
FROM (
    SELECT from_address as address, from_address, to_address, asset, amount, fee, block_height, block_timestamp FROM balance_transfers
    UNION ALL
    SELECT to_address as address, from_address, to_address, asset, amount, 0 as fee, block_height, block_timestamp FROM balance_transfers
)
GROUP BY address, asset, activity_date;
