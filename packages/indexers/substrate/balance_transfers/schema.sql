
-- Balance Transfers Table (unchanged)
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
PARTITION BY intDiv(block_height, 100000)
ORDER BY (extrinsic_id, event_idx, asset)
SETTINGS
    index_granularity = 8192
COMMENT 'Stores individual balance transfer transactions with associated fees';


-- Base 4-hour interval materialized view for volume series
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_volume_series_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(period_start)
ORDER BY (period_start, asset)
SETTINGS
    index_granularity = 8192
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
    asset, amount
COMMENT 'Base 4-hour interval materialized view for transfer volume analysis - asset agnostic with client-defined categorization';