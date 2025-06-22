-- Balance Transfers Schema - Part 2: Basic Views
-- Basic views for transfer statistics and daily volume

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
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(period_start)
ORDER BY (period_start, asset)
SETTINGS index_granularity = 8192
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
    
    -- Volume change from previous period (using ClickHouse-compatible functions)
    total_volume - any(total_volume) OVER (
        PARTITION BY asset
        ORDER BY period_start
        ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
    ) as volume_change,
    
    -- Percentage change from previous period
    CASE
        WHEN any(total_volume) OVER (PARTITION BY asset ORDER BY period_start ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) > 0 THEN
            ((total_volume - any(total_volume) OVER (PARTITION BY asset ORDER BY period_start ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)) /
             any(total_volume) OVER (PARTITION BY asset ORDER BY period_start ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)) * 100
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

-- Materialized view for daily transfer volume (legacy compatibility)
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_daily_volume_mv
ENGINE = AggregatingMergeTree()
ORDER BY (date, asset)
AS
SELECT
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as date,
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

-- Simple view for available assets list
CREATE VIEW IF NOT EXISTS available_transfer_assets_view AS
SELECT DISTINCT asset
FROM balance_transfers
WHERE asset != ''
ORDER BY asset;