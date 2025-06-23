-- Balance Transfers Schema
-- Tables and views for tracking individual transfer transactions on Substrate networks
-- Asset-agnostic design with universal histogram bins and optimized materialized views
-- Consolidated version with merged views and reduced redundancy

-- Drop potentially invalid objects first to ensure clean recreation
DROP VIEW IF EXISTS balance_transfers_network_enhanced_view;
DROP VIEW IF EXISTS balance_transfers_network_flow_view;
DROP VIEW IF EXISTS balance_transfers_transaction_analytics_view;
DROP VIEW IF EXISTS balance_transfers_address_relationships_mv;
DROP VIEW IF EXISTS balance_transfers_pairs_analysis_view;
DROP VIEW IF EXISTS balance_transfers_address_clusters_view;
DROP VIEW IF EXISTS balance_transfers_suspicious_activity_view;
DROP VIEW IF EXISTS balance_transfers_address_activity_patterns_view;
DROP VIEW IF EXISTS balance_transfers_address_classification_view;
DROP VIEW IF EXISTS balance_transfers_address_behavior_profiles_view;
DROP VIEW IF EXISTS balance_transfers_daily_volume_mv;

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
    index_granularity = 8192
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
-- VOLUME SERIES MATERIALIZED VIEWS (4-Hour Intervals)
-- =============================================================================

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
    asset
COMMENT 'Base 4-hour interval materialized view for transfer volume analysis - asset agnostic with client-defined categorization';

-- Daily Network Analytics View (Merged with flow view)
CREATE VIEW IF NOT EXISTS balance_transfers_network_daily_view AS
WITH daily_aggregates AS (
    SELECT
        toDate(period_start) as period,
        asset,
        
        -- Basic volume metrics (aggregated from 4-hour periods)
        sum(transaction_count) as transaction_count,
        sum(total_volume) as total_volume,
        max(unique_senders) as max_unique_senders,
        max(unique_receivers) as max_unique_receivers,
        max(active_addresses) as unique_addresses,
        
        -- Asset-agnostic transaction size histogram (aggregated from 4-hour periods)
        sum(tx_count_lt_01) as tx_count_lt_01,
        sum(tx_count_01_to_1) as tx_count_01_to_1,
        sum(tx_count_1_to_10) as tx_count_1_to_10,
        sum(tx_count_10_to_100) as tx_count_10_to_100,
        sum(tx_count_100_to_1k) as tx_count_100_to_1k,
        sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
        sum(tx_count_gte_10k) as tx_count_gte_10k,
        
        -- Network density (average across 4-hour periods)
        avg(network_density) as avg_network_density,
        
        -- Fee metrics (aggregated from 4-hour periods)
        sum(total_fees) as total_fees,
        max(max_fee) as max_fee,
        min(min_fee) as min_fee,
        
        -- Statistical measures (averaged across 4-hour periods)
        avg(median_transfer_amount) as median_transaction_size,
        avg(amount_std_dev) as avg_amount_std_dev,
        
        -- Min/max for transaction sizes
        max(max_transfer_amount) as max_transaction_size,
        min(min_transfer_amount) as min_transaction_size,
        
        -- Block information
        min(period_start_block) as period_start_block,
        max(period_end_block) as period_end_block
    
    FROM balance_transfers_volume_series_mv
    GROUP BY toDate(period_start), asset
)
SELECT
    'daily' as period_type,
    period,
    asset,
    transaction_count,
    total_volume,
    max_unique_senders,
    max_unique_receivers,
    unique_addresses,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    avg_network_density,
    total_fees,
    
    -- Additional statistics from flow view
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_size,
    max_transaction_size,
    min_transaction_size,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    max_fee,
    min_fee,
    median_transaction_size,
    avg_amount_std_dev,
    
    -- Block information
    period_start_block,
    period_end_block,
    period_end_block - period_start_block + 1 as blocks_in_period

FROM daily_aggregates
ORDER BY period DESC, asset;

-- Weekly Network Analytics View (Direct aggregation from base table to avoid nested aggregates)
CREATE VIEW IF NOT EXISTS balance_transfers_network_weekly_view AS
WITH weekly_base AS (
    SELECT
        toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as period,
        asset,
        count() as transaction_count,
        uniqExact(from_address) as unique_senders,
        uniqExact(to_address) as unique_receivers,
        uniqExact(from_address) + uniqExact(to_address) as active_addresses,
        sum(amount) as total_volume,
        sum(fee) as total_fees,
        avg(amount) as avg_transfer_amount,
        max(amount) as max_transfer_amount,
        min(amount) as min_transfer_amount,
        median(amount) as median_transfer_amount,
        stddevPop(amount) as amount_std_dev,
        max(fee) as max_fee,
        min(fee) as min_fee,
        avg(fee) as avg_fee,
        min(block_height) as period_start_block,
        max(block_height) as period_end_block,
        
        -- Network density calculation (complex, simplified for weekly view)
        uniq(from_address, to_address) / greatest(uniqExact(from_address) + uniqExact(to_address), 1) as network_density,
        
        -- Amount histogram bins
        countIf(amount < 0.1) as tx_count_lt_01,
        countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
        countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
        countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
        countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
        countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
        countIf(amount >= 10000) as tx_count_gte_10k
    FROM balance_transfers
    GROUP BY period, asset
)
SELECT
    'weekly' as period_type,
    period,
    asset,
    transaction_count,
    total_volume,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    active_addresses as unique_addresses,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    network_density as avg_network_density,
    total_fees,
    
    -- Additional statistics
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_size,
    max_transfer_amount as max_transaction_size,
    min_transfer_amount as min_transaction_size,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    max_fee,
    min_fee,
    median_transfer_amount as median_transaction_size,
    amount_std_dev as avg_amount_std_dev,
    
    -- Block information
    period_start_block,
    period_end_block,
    period_end_block - period_start_block + 1 as blocks_in_period
    
FROM weekly_base
ORDER BY period DESC, asset;

-- Monthly Network Analytics View (Direct aggregation from base table to avoid nested aggregates)
CREATE VIEW IF NOT EXISTS balance_transfers_network_monthly_view AS
WITH monthly_base AS (
    SELECT
        toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000))) as period,
        asset,
        count() as transaction_count,
        uniqExact(from_address) as unique_senders,
        uniqExact(to_address) as unique_receivers,
        uniqExact(from_address) + uniqExact(to_address) as active_addresses,
        sum(amount) as total_volume,
        sum(fee) as total_fees,
        avg(amount) as avg_transfer_amount,
        max(amount) as max_transfer_amount,
        min(amount) as min_transfer_amount,
        median(amount) as median_transfer_amount,
        stddevPop(amount) as amount_std_dev,
        max(fee) as max_fee,
        min(fee) as min_fee,
        avg(fee) as avg_fee,
        min(block_height) as period_start_block,
        max(block_height) as period_end_block,
        
        -- Network density calculation (complex, simplified for monthly view)
        uniq(from_address, to_address) / greatest(uniqExact(from_address) + uniqExact(to_address), 1) as network_density,
        
        -- Amount histogram bins
        countIf(amount < 0.1) as tx_count_lt_01,
        countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
        countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
        countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
        countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
        countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
        countIf(amount >= 10000) as tx_count_gte_10k
    FROM balance_transfers
    GROUP BY period, asset
)
SELECT
    'monthly' as period_type,
    period,
    asset,
    transaction_count,
    total_volume,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    active_addresses as unique_addresses,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    network_density as avg_network_density,
    total_fees,
    
    -- Additional statistics
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_size,
    max_transfer_amount as max_transaction_size,
    min_transfer_amount as min_transaction_size,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    max_fee,
    min_fee,
    median_transfer_amount as median_transaction_size,
    amount_std_dev as avg_amount_std_dev,
    
    -- Block information
    period_start_block,
    period_end_block,
    period_end_block - period_start_block + 1 as blocks_in_period
    
FROM monthly_base
ORDER BY period DESC, asset;

-- =============================================================================
-- CONSOLIDATED ADDRESS ANALYTICS VIEW 
-- =============================================================================

-- Comprehensive Address Analytics View (combining functionality from all address views)
CREATE VIEW IF NOT EXISTS balance_transfers_address_analytics_view AS
WITH address_base_metrics AS (
    SELECT
        from_address as address,
        asset,
        
        -- Basic transaction counts
        count() as total_transactions,
        count() as outgoing_count,
        sum(amount) as total_sent,
        sum(fee) as total_fees_paid,
        avg(fee) as avg_fee_paid,
        uniq(to_address) as unique_recipients,
        min(block_timestamp) as first_activity_ts,
        max(block_timestamp) as last_activity_ts,
        
        -- Time-based patterns (using hour extraction)
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 0 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 6) as night_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 6 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 12) as morning_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 12 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 18) as afternoon_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 18 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 24) as evening_transactions,
        
        -- Universal histogram bins (asset-agnostic)
        countIf(amount < 0.1) as tx_count_lt_01,
        countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
        countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
        countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
        countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
        countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
        countIf(amount >= 10000) as tx_count_gte_10k,
        
        -- Statistical measures
        varPop(amount) as sent_amount_variance,
        avg(amount) as avg_sent_amount,
        uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as active_days
        
    FROM balance_transfers
    GROUP BY from_address, asset
),
address_incoming_metrics AS (
    SELECT
        to_address as address,
        asset,
        count() as incoming_count,
        sum(amount) as total_received,
        uniq(from_address) as unique_senders,
        varPop(amount) as received_amount_variance
    FROM balance_transfers
    GROUP BY to_address, asset
),
address_combined_metrics AS (
    SELECT
        COALESCE(out.address, inc.address) as address,
        COALESCE(out.asset, inc.asset) as asset,
        
        -- Basic metrics
        COALESCE(out.total_transactions, 0) as total_transactions,
        COALESCE(out.outgoing_count, 0) as outgoing_count,
        COALESCE(inc.incoming_count, 0) as incoming_count,
        COALESCE(out.total_sent, 0) as total_sent,
        COALESCE(inc.total_received, 0) as total_received,
        COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) as total_volume,
        COALESCE(out.unique_recipients, 0) as unique_recipients,
        COALESCE(inc.unique_senders, 0) as unique_senders,
        
        -- Time metrics
        COALESCE(out.first_activity_ts, 0) as first_activity,
        COALESCE(out.last_activity_ts, 0) as last_activity,
        COALESCE(out.last_activity_ts, 0) - COALESCE(out.first_activity_ts, 0) as activity_span_seconds,
        
        -- Fees
        COALESCE(out.total_fees_paid, 0) as total_fees_paid,
        COALESCE(out.avg_fee_paid, 0) as avg_fee_paid,
        
        -- Time distribution
        COALESCE(out.night_transactions, 0) as night_transactions,
        COALESCE(out.morning_transactions, 0) as morning_transactions,
        COALESCE(out.afternoon_transactions, 0) as afternoon_transactions,
        COALESCE(out.evening_transactions, 0) as evening_transactions,
        
        -- Histogram bins
        COALESCE(out.tx_count_lt_01, 0) as tx_count_lt_01,
        COALESCE(out.tx_count_01_to_1, 0) as tx_count_01_to_1,
        COALESCE(out.tx_count_1_to_10, 0) as tx_count_1_to_10,
        COALESCE(out.tx_count_10_to_100, 0) as tx_count_10_to_100,
        COALESCE(out.tx_count_100_to_1k, 0) as tx_count_100_to_1k,
        COALESCE(out.tx_count_1k_to_10k, 0) as tx_count_1k_to_10k,
        COALESCE(out.tx_count_gte_10k, 0) as tx_count_gte_10k,
        
        -- Statistical measures
        COALESCE(out.sent_amount_variance, 0) as sent_amount_variance,
        COALESCE(inc.received_amount_variance, 0) as received_amount_variance,
        COALESCE(out.avg_sent_amount, 0) as avg_sent_amount,
        COALESCE(out.active_days, 0) as active_days
        
    FROM address_base_metrics out
    FULL OUTER JOIN address_incoming_metrics inc 
        ON out.address = inc.address AND out.asset = inc.asset
)
SELECT
    address,
    asset,
    
    -- Basic Metrics
    total_transactions,
    outgoing_count,
    incoming_count,
    total_sent,
    total_received,
    total_volume,
    unique_recipients,
    unique_senders,
    first_activity,
    last_activity,
    activity_span_seconds,
    total_fees_paid,
    avg_fee_paid,
    
    -- Time Distribution
    night_transactions,
    morning_transactions,
    afternoon_transactions,
    evening_transactions,
    
    -- Asset-agnostic Histogram Bins
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    
    -- Statistical Measures
    sent_amount_variance,
    received_amount_variance,
    active_days,
    
    -- COMPUTED SUSPICION INDICATORS
    CASE 
        WHEN total_transactions > 0 AND toFloat64(night_transactions) >= toFloat64(total_transactions) * 0.8 
        THEN 1 ELSE 0 
    END as unusual_time_pattern,
    
    CASE 
        WHEN avg_sent_amount > 0 AND sent_amount_variance < avg_sent_amount * 0.05 AND total_transactions >= 20 
        THEN 1 ELSE 0 
    END as fixed_amount_pattern,
    
    CASE 
        WHEN unique_recipients = 1 AND total_transactions >= 50 
        THEN 1 ELSE 0 
    END as single_recipient_pattern,
    
    CASE 
        WHEN tx_count_gte_10k > 0 AND total_transactions <= 5 
        THEN 1 ELSE 0 
    END as large_infrequent_pattern,
    
    -- COMPUTED RISK SCORE (sum of all indicators)
    (CASE 
        WHEN total_transactions > 0 AND toFloat64(night_transactions) >= toFloat64(total_transactions) * 0.8 
        THEN 1 ELSE 0 
    END +
    CASE 
        WHEN avg_sent_amount > 0 AND sent_amount_variance < avg_sent_amount * 0.05 AND total_transactions >= 20 
        THEN 1 ELSE 0 
    END +
    CASE 
        WHEN unique_recipients = 1 AND total_transactions >= 50 
        THEN 1 ELSE 0 
    END +
    CASE 
        WHEN tx_count_gte_10k > 0 AND total_transactions <= 5 
        THEN 1 ELSE 0 
    END) as risk_score,
    
    -- RISK LEVEL
    CASE
        WHEN (CASE 
                WHEN total_transactions > 0 AND toFloat64(night_transactions) >= toFloat64(total_transactions) * 0.8 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN avg_sent_amount > 0 AND sent_amount_variance < avg_sent_amount * 0.05 AND total_transactions >= 20 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN unique_recipients = 1 AND total_transactions >= 50 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN tx_count_gte_10k > 0 AND total_transactions <= 5 
                THEN 1 ELSE 0 
             END) >= 3 THEN 'High'
        WHEN (CASE 
                WHEN total_transactions > 0 AND toFloat64(night_transactions) >= toFloat64(total_transactions) * 0.8 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN avg_sent_amount > 0 AND sent_amount_variance < avg_sent_amount * 0.05 AND total_transactions >= 20 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN unique_recipients = 1 AND total_transactions >= 50 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN tx_count_gte_10k > 0 AND total_transactions <= 5 
                THEN 1 ELSE 0 
             END) >= 2 THEN 'Medium'
        WHEN (CASE 
                WHEN total_transactions > 0 AND toFloat64(night_transactions) >= toFloat64(total_transactions) * 0.8 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN avg_sent_amount > 0 AND sent_amount_variance < avg_sent_amount * 0.05 AND total_transactions >= 20 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN unique_recipients = 1 AND total_transactions >= 50 
                THEN 1 ELSE 0 
             END +
             CASE 
                WHEN tx_count_gte_10k > 0 AND total_transactions <= 5 
                THEN 1 ELSE 0 
             END) >= 1 THEN 'Low'
        ELSE 'Normal'
    END as risk_level,
    
    -- ADDRESS CLASSIFICATION
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
    
    -- VOLUME CLASSIFICATION
    CASE
        WHEN total_volume >= 100000 THEN 'Ultra_High'
        WHEN total_volume >= 10000 THEN 'High'
        WHEN total_volume >= 1000 THEN 'Medium'
        WHEN total_volume >= 100 THEN 'Low'
        ELSE 'Micro'
    END as volume_tier
    
FROM address_combined_metrics
WHERE total_transactions > 0;

-- =============================================================================
-- DAILY PATTERNS VIEW 
-- =============================================================================

-- Daily Activity Patterns View (enhanced from both patterns views)
CREATE VIEW IF NOT EXISTS balance_transfers_daily_patterns_view AS
SELECT
    from_address,
    to_address,
    asset,
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as activity_date,
    count() as daily_transactions,
    sum(amount) as daily_volume,
    avg(amount) as daily_avg_amount,
    min(amount) as daily_min_amount,
    max(amount) as daily_max_amount,
    uniq(toHour(toDateTime(intDiv(block_timestamp, 1000)))) as active_hours_count,
    
    -- Daily histogram
    countIf(amount < 0.1) as daily_tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as daily_tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as daily_tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as daily_tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as daily_tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as daily_tx_count_1k_to_10k,
    countIf(amount >= 10000) as daily_tx_count_gte_10k,
    
    -- Statistical Measures
    median(amount) as daily_median_amount,
    stddevPop(amount) as daily_amount_variance,
    
    -- Daily fee analysis
    sum(fee) as daily_fees,
    avg(fee) as daily_avg_fee,
    
    -- Hourly Distribution Analysis
    groupArray(toHour(toDateTime(intDiv(block_timestamp, 1000)))) as hourly_activity,
    toUInt8(avg(toHour(toDateTime(intDiv(block_timestamp, 1000))))) as peak_hour,
    
    -- Time distribution
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 0 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 6) as night_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 6 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 12) as morning_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 12 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 18) as afternoon_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 18 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 24) as evening_transactions

FROM balance_transfers
WHERE from_address != to_address
GROUP BY from_address, to_address, asset, activity_date
ORDER BY activity_date DESC, daily_volume DESC;

-- =============================================================================
-- VOLUME AGGREGATION VIEWS
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

-- Weekly aggregation view - Direct from base table to avoid nested aggregation
CREATE VIEW IF NOT EXISTS balance_transfers_volume_weekly_view AS
WITH weekly_aggregates AS (
    SELECT
        toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as week_start,
        asset,
        count() as weekly_transaction_count,
        uniqExact(from_address) as unique_senders,
        uniqExact(to_address) as unique_receivers,
        uniqExact(from_address) + uniqExact(to_address) as active_addresses,
        sum(amount) as weekly_total_volume,
        sum(fee) as weekly_total_fees,
        max(amount) as weekly_max_transfer_amount,
        min(block_height) as weekly_start_block,
        max(block_height) as weekly_end_block,
        
        -- Histogram aggregations
        countIf(amount < 0.1) as tx_count_lt_01,
        countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
        countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
        countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
        countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
        countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
        countIf(amount >= 10000) as tx_count_gte_10k,
        
        sumIf(amount, amount < 0.1) as volume_lt_01,
        sumIf(amount, amount >= 0.1 AND amount < 1) as volume_01_to_1,
        sumIf(amount, amount >= 1 AND amount < 10) as volume_1_to_10,
        sumIf(amount, amount >= 10 AND amount < 100) as volume_10_to_100,
        sumIf(amount, amount >= 100 AND amount < 1000) as volume_100_to_1k,
        sumIf(amount, amount >= 1000 AND amount < 10000) as volume_1k_to_10k,
        sumIf(amount, amount >= 10000) as volume_gte_10k
    FROM balance_transfers
    GROUP BY week_start, asset
)
SELECT
    week_start,
    asset,
    weekly_transaction_count,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    weekly_total_volume,
    weekly_total_fees,
    weekly_total_volume / weekly_transaction_count as weekly_avg_transfer_amount,
    weekly_max_transfer_amount,
    
    -- Network metrics
    active_addresses as max_weekly_active_addresses,
    
    -- Histogram aggregations
    tx_count_lt_01 as weekly_tx_count_lt_01,
    tx_count_01_to_1 as weekly_tx_count_01_to_1,
    tx_count_1_to_10 as weekly_tx_count_1_to_10,
    tx_count_10_to_100 as weekly_tx_count_10_to_100,
    tx_count_100_to_1k as weekly_tx_count_100_to_1k,
    tx_count_1k_to_10k as weekly_tx_count_1k_to_10k,
    tx_count_gte_10k as weekly_tx_count_gte_10k,
    
    volume_lt_01 as weekly_volume_lt_01,
    volume_01_to_1 as weekly_volume_01_to_1,
    volume_1_to_10 as weekly_volume_1_to_10,
    volume_10_to_100 as weekly_volume_10_to_100,
    volume_100_to_1k as weekly_volume_100_to_1k,
    volume_1k_to_10k as weekly_volume_1k_to_10k,
    volume_gte_10k as weekly_volume_gte_10k,
    
    -- Block information
    weekly_start_block,
    weekly_end_block
    
FROM weekly_aggregates
ORDER BY week_start DESC, asset;

-- Monthly aggregation view - Direct from base table to avoid nested aggregation
CREATE VIEW IF NOT EXISTS balance_transfers_volume_monthly_view AS
WITH monthly_aggregates AS (
    SELECT
        toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000))) as month_start,
        asset,
        count() as monthly_transaction_count,
        uniqExact(from_address) as unique_senders,
        uniqExact(to_address) as unique_receivers,
        uniqExact(from_address) + uniqExact(to_address) as active_addresses,
        sum(amount) as monthly_total_volume,
        sum(fee) as monthly_total_fees,
        max(amount) as monthly_max_transfer_amount,
        min(block_height) as monthly_start_block,
        max(block_height) as monthly_end_block,
        
        -- Histogram aggregations
        countIf(amount < 0.1) as tx_count_lt_01,
        countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
        countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
        countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
        countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
        countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
        countIf(amount >= 10000) as tx_count_gte_10k,
        
        sumIf(amount, amount < 0.1) as volume_lt_01,
        sumIf(amount, amount >= 0.1 AND amount < 1) as volume_01_to_1,
        sumIf(amount, amount >= 1 AND amount < 10) as volume_1_to_10,
        sumIf(amount, amount >= 10 AND amount < 100) as volume_10_to_100,
        sumIf(amount, amount >= 100 AND amount < 1000) as volume_100_to_1k,
        sumIf(amount, amount >= 1000 AND amount < 10000) as volume_1k_to_10k,
        sumIf(amount, amount >= 10000) as volume_gte_10k
    FROM balance_transfers
    GROUP BY month_start, asset
)
SELECT
    month_start,
    asset,
    monthly_transaction_count,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    monthly_total_volume,
    monthly_total_fees,
    monthly_total_volume / monthly_transaction_count as monthly_avg_transfer_amount,
    monthly_max_transfer_amount,
    
    -- Network metrics
    active_addresses as max_monthly_active_addresses,
    
    -- Histogram aggregations
    tx_count_lt_01 as monthly_tx_count_lt_01,
    tx_count_01_to_1 as monthly_tx_count_01_to_1,
    tx_count_1_to_10 as monthly_tx_count_1_to_10,
    tx_count_10_to_100 as monthly_tx_count_10_to_100,
    tx_count_100_to_1k as monthly_tx_count_100_to_1k,
    tx_count_1k_to_10k as monthly_tx_count_1k_to_10k,
    tx_count_gte_10k as monthly_tx_count_gte_10k,
    
    volume_lt_01 as monthly_volume_lt_01,
    volume_01_to_1 as monthly_volume_01_to_1,
    volume_1_to_10 as monthly_volume_1_to_10,
    volume_10_to_100 as monthly_volume_10_to_100,
    volume_100_to_1k as monthly_volume_100_to_1k,
    volume_1k_to_10k as monthly_volume_1k_to_10k,
    volume_gte_10k as monthly_volume_gte_10k,
    
    -- Block information
    monthly_start_block,
    monthly_end_block
    
FROM monthly_aggregates
ORDER BY month_start DESC, asset;

-- =============================================================================
-- ANALYSIS VIEWS
-- =============================================================================

-- Volume trends view with rolling averages only (simplified for compatibility)
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
    ) as rolling_30_period_avg_volume
    
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