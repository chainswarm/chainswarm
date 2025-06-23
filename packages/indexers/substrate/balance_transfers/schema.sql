-- Balance Transfers Schema (Refactored with Consistent Naming)
-- Fixed all naming inconsistencies: using unique_senders instead of max_unique_senders
-- Standardized all field names across views for consistency
-- Eliminated duplication with reusable functions and layered architecture

-- =============================================================================
-- CLEANUP OLD OBJECTS
-- =============================================================================

-- Drop old views that will be replaced
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

-- Drop existing views to recreate with new architecture
DROP VIEW IF EXISTS balance_transfers_network_daily_view;
DROP VIEW IF EXISTS balance_transfers_network_weekly_view;
DROP VIEW IF EXISTS balance_transfers_network_monthly_view;
DROP VIEW IF EXISTS balance_transfers_volume_daily_view;
DROP VIEW IF EXISTS balance_transfers_volume_weekly_view;
DROP VIEW IF EXISTS balance_transfers_volume_monthly_view;
DROP VIEW IF EXISTS balance_transfers_volume_trends_view;
DROP VIEW IF EXISTS balance_transfers_volume_quantiles_view;
DROP VIEW IF EXISTS balance_transfers_address_analytics_view;
DROP VIEW IF EXISTS balance_transfers_daily_patterns_view;

-- Drop existing materialized views
DROP VIEW IF EXISTS balance_transfers_daily_mv;

-- =============================================================================
-- CORE TABLES
-- =============================================================================

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
PARTITION BY intDiv(block_height, {PARTITION_SIZE})
ORDER BY (extrinsic_id, event_idx, asset)
SETTINGS
    index_granularity = 8192
COMMENT 'Stores individual balance transfer transactions with associated fees';

-- =============================================================================
-- INDEXES (unchanged)
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
-- REUSABLE FUNCTIONS
-- =============================================================================

-- Function to classify amounts into histogram bins
CREATE FUNCTION IF NOT EXISTS getAmountBin(amount Decimal128(18)) 
RETURNS String
LANGUAGE SQL
DETERMINISTIC
AS $$
    CASE
        WHEN amount < 0.1 THEN 'lt_01'
        WHEN amount >= 0.1 AND amount < 1 THEN '01_to_1'
        WHEN amount >= 1 AND amount < 10 THEN '1_to_10'
        WHEN amount >= 10 AND amount < 100 THEN '10_to_100'
        WHEN amount >= 100 AND amount < 1000 THEN '100_to_1k'
        WHEN amount >= 1000 AND amount < 10000 THEN '1k_to_10k'
        ELSE 'gte_10k'
    END
$$;

-- Function to calculate risk score based on transaction patterns
CREATE FUNCTION IF NOT EXISTS calculateRiskScore(
    total_transactions UInt64,
    night_transactions UInt64,
    avg_sent_amount Decimal128(18),
    sent_amount_variance Decimal128(18),
    unique_recipients UInt64,
    tx_count_gte_10k UInt64
) RETURNS UInt8
LANGUAGE SQL
DETERMINISTIC
AS $$
    toUInt8(
        -- Unusual time pattern
        CASE 
            WHEN total_transactions > 0 AND toFloat64(night_transactions) >= toFloat64(total_transactions) * 0.8 
            THEN 1 ELSE 0 
        END +
        -- Fixed amount pattern
        CASE 
            WHEN avg_sent_amount > 0 AND sent_amount_variance < avg_sent_amount * 0.05 AND total_transactions >= 20 
            THEN 1 ELSE 0 
        END +
        -- Single recipient pattern
        CASE 
            WHEN unique_recipients = 1 AND total_transactions >= 50 
            THEN 1 ELSE 0 
        END +
        -- Large infrequent pattern
        CASE 
            WHEN tx_count_gte_10k > 0 AND total_transactions <= 5 
            THEN 1 ELSE 0 
        END
    )
$$;

-- Function to calculate network density (standardized)
CREATE FUNCTION IF NOT EXISTS calculateNetworkDensity(
    unique_pairs UInt64,
    total_addresses UInt64
) RETURNS Float64
LANGUAGE SQL
DETERMINISTIC
AS $$
    CASE
        WHEN total_addresses > 1 THEN
            toFloat64(unique_pairs) / 
            (toFloat64(total_addresses) * toFloat64(total_addresses - 1) / 2.0)
        ELSE 0.0
    END
$$;

-- =============================================================================
-- BASE 4-HOUR MATERIALIZED VIEW (with consistent naming)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_volume_series_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(period_start)
ORDER BY (period_start, asset)
SETTINGS
    index_granularity = 8192
AS
SELECT
    -- Time period information
    toStartOfInterval(toDateTime(intDiv(block_timestamp, 1000)), INTERVAL 4 HOUR) as period_start,
    toStartOfInterval(toDateTime(intDiv(block_timestamp, 1000)), INTERVAL 4 HOUR) + INTERVAL 4 HOUR as period_end,
    
    -- Asset information
    asset,
    
    -- Basic volume metrics (consistent naming)
    count() as transaction_count,
    uniqExact(from_address) as unique_senders,
    uniqExact(to_address) as unique_receivers,
    sum(amount) as total_volume,
    sum(fee) as total_fees,
    avg(amount) as avg_transaction_amount,  -- Consistent: transaction not transfer
    max(amount) as max_transaction_amount,
    min(amount) as min_transaction_amount,
    median(amount) as median_transaction_amount,
    
    -- Amount distribution quantiles
    quantile(0.10)(amount) as amount_p10,
    quantile(0.25)(amount) as amount_p25,
    quantile(0.50)(amount) as amount_p50,
    quantile(0.75)(amount) as amount_p75,
    quantile(0.90)(amount) as amount_p90,
    quantile(0.95)(amount) as amount_p95,
    quantile(0.99)(amount) as amount_p99,
    
    -- Network activity metrics
    uniq(from_address, to_address) as unique_address_pairs,
    uniqExact(from_address) + uniqExact(to_address) as active_addresses,
    
    -- Network density calculation (using function)
    calculateNetworkDensity(
        uniq(from_address, to_address),
        uniqExact(from_address) + uniqExact(to_address)
    ) as network_density,
    
    -- Temporal activity patterns
    toHour(period_start) as period_hour,
    
    -- Statistical measures
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
    
    -- Block information
    min(block_height) as period_start_block,
    max(block_height) as period_end_block,
    max(block_height) - min(block_height) + 1 as blocks_in_period,
    
    -- Histogram bins using function
    countIf(getAmountBin(amount) = 'lt_01') as tx_count_lt_01,
    countIf(getAmountBin(amount) = '01_to_1') as tx_count_01_to_1,
    countIf(getAmountBin(amount) = '1_to_10') as tx_count_1_to_10,
    countIf(getAmountBin(amount) = '10_to_100') as tx_count_10_to_100,
    countIf(getAmountBin(amount) = '100_to_1k') as tx_count_100_to_1k,
    countIf(getAmountBin(amount) = '1k_to_10k') as tx_count_1k_to_10k,
    countIf(getAmountBin(amount) = 'gte_10k') as tx_count_gte_10k,
    
    -- Volume histogram bins
    sumIf(amount, getAmountBin(amount) = 'lt_01') as volume_lt_01,
    sumIf(amount, getAmountBin(amount) = '01_to_1') as volume_01_to_1,
    sumIf(amount, getAmountBin(amount) = '1_to_10') as volume_1_to_10,
    sumIf(amount, getAmountBin(amount) = '10_to_100') as volume_10_to_100,
    sumIf(amount, getAmountBin(amount) = '100_to_1k') as volume_100_to_1k,
    sumIf(amount, getAmountBin(amount) = '1k_to_10k') as volume_1k_to_10k,
    sumIf(amount, getAmountBin(amount) = 'gte_10k') as volume_gte_10k

FROM balance_transfers
GROUP BY
    period_start,
    period_end,
    asset
COMMENT 'Base 4-hour interval materialized view for transfer volume analysis';

-- =============================================================================
-- DAILY MATERIALIZED VIEW (with consistent naming)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_daily_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, asset)
SETTINGS
    index_granularity = 8192
AS
SELECT
    toDate(period_start) as date,
    asset,
    
    -- Aggregate metrics from 4-hour periods
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    sum(total_fees) as total_fees,
    
    -- For unique counts, take the max from 4-hour periods (consistent naming)
    max(unique_senders) as unique_senders,
    max(unique_receivers) as unique_receivers,
    max(active_addresses) as active_addresses,
    
    -- Network metrics
    avg(network_density) as avg_network_density,
    
    -- Statistical aggregations (consistent naming)
    avg(median_transaction_amount) as median_transaction_amount,
    max(max_transaction_amount) as max_transaction_amount,
    min(min_transaction_amount) as min_transaction_amount,
    avg(amount_std_dev) as avg_amount_std_dev,
    
    -- Fee metrics
    avg(avg_fee) as avg_fee,
    max(max_fee) as max_fee,
    min(min_fee) as min_fee,
    
    -- Histogram aggregations (sum from 4-hour periods)
    sum(tx_count_lt_01) as tx_count_lt_01,
    sum(tx_count_01_to_1) as tx_count_01_to_1,
    sum(tx_count_1_to_10) as tx_count_1_to_10,
    sum(tx_count_10_to_100) as tx_count_10_to_100,
    sum(tx_count_100_to_1k) as tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as tx_count_gte_10k,
    
    sum(volume_lt_01) as volume_lt_01,
    sum(volume_01_to_1) as volume_01_to_1,
    sum(volume_1_to_10) as volume_1_to_10,
    sum(volume_10_to_100) as volume_10_to_100,
    sum(volume_100_to_1k) as volume_100_to_1k,
    sum(volume_1k_to_10k) as volume_1k_to_10k,
    sum(volume_gte_10k) as volume_gte_10k,
    
    -- Block information
    min(period_start_block) as start_block,
    max(period_end_block) as end_block
    
FROM balance_transfers_volume_series_mv
GROUP BY date, asset
COMMENT 'Daily aggregation materialized view for efficient weekly/monthly rollups';

-- =============================================================================
-- NETWORK ANALYTICS VIEWS (with consistent naming)
-- =============================================================================

-- Daily Network Analytics View (uses daily MV)
CREATE VIEW IF NOT EXISTS balance_transfers_network_daily_view AS
SELECT
    'daily' as period_type,
    date as period,
    asset,
    transaction_count,
    total_volume,
    unique_senders,         -- Consistent: no max_ prefix
    unique_receivers,       -- Consistent: no max_ prefix
    active_addresses as unique_addresses,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    avg_network_density,
    total_fees,
    
    -- Additional statistics (consistent naming)
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_amount,
    max_transaction_amount,
    min_transaction_amount,
    avg_fee,
    max_fee,
    min_fee,
    median_transaction_amount,
    avg_amount_std_dev,
    
    -- Block information
    start_block as period_start_block,
    end_block as period_end_block,
    end_block - start_block + 1 as blocks_in_period

FROM balance_transfers_daily_mv
ORDER BY period DESC, asset;

-- Weekly Network Analytics View (uses daily MV with consistent naming)
CREATE VIEW IF NOT EXISTS balance_transfers_network_weekly_view AS
SELECT
    'weekly' as period_type,
    toStartOfWeek(date) as period,
    asset,
    
    -- Aggregate from daily data
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    max(unique_senders) as unique_senders,       -- Consistent: no max_ prefix
    max(unique_receivers) as unique_receivers,   -- Consistent: no max_ prefix
    max(active_addresses) as unique_addresses,
    
    -- Histogram aggregations
    sum(tx_count_lt_01) as tx_count_lt_01,
    sum(tx_count_01_to_1) as tx_count_01_to_1,
    sum(tx_count_1_to_10) as tx_count_1_to_10,
    sum(tx_count_10_to_100) as tx_count_10_to_100,
    sum(tx_count_100_to_1k) as tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as tx_count_gte_10k,
    
    -- Network and fee metrics
    avg(avg_network_density) as avg_network_density,
    sum(total_fees) as total_fees,
    
    -- Calculated fields (consistent naming)
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_volume) / sum(transaction_count) 
        ELSE 0 
    END as avg_transaction_amount,
    
    max(max_transaction_amount) as max_transaction_amount,
    min(min_transaction_amount) as min_transaction_amount,
    
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_fees) / sum(transaction_count) 
        ELSE 0 
    END as avg_fee,
    
    max(max_fee) as max_fee,
    min(min_fee) as min_fee,
    avg(median_transaction_amount) as median_transaction_amount,
    avg(avg_amount_std_dev) as avg_amount_std_dev,
    
    -- Block information
    min(start_block) as period_start_block,
    max(end_block) as period_end_block,
    max(end_block) - min(start_block) + 1 as blocks_in_period
    
FROM balance_transfers_daily_mv
GROUP BY toStartOfWeek(date), asset
ORDER BY period DESC, asset;

-- Monthly Network Analytics View (uses daily MV with consistent naming)
CREATE VIEW IF NOT EXISTS balance_transfers_network_monthly_view AS
SELECT
    'monthly' as period_type,
    toStartOfMonth(date) as period,
    asset,
    
    -- Same aggregation pattern as weekly
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    max(unique_senders) as unique_senders,       -- Consistent: no max_ prefix
    max(unique_receivers) as unique_receivers,   -- Consistent: no max_ prefix
    max(active_addresses) as unique_addresses,
    
    -- Histogram aggregations
    sum(tx_count_lt_01) as tx_count_lt_01,
    sum(tx_count_01_to_1) as tx_count_01_to_1,
    sum(tx_count_1_to_10) as tx_count_1_to_10,
    sum(tx_count_10_to_100) as tx_count_10_to_100,
    sum(tx_count_100_to_1k) as tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as tx_count_gte_10k,
    
    -- Network and fee metrics
    avg(avg_network_density) as avg_network_density,
    sum(total_fees) as total_fees,
    
    -- Calculated fields (consistent naming)
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_volume) / sum(transaction_count) 
        ELSE 0 
    END as avg_transaction_amount,
    
    max(max_transaction_amount) as max_transaction_amount,
    min(min_transaction_amount) as min_transaction_amount,
    
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_fees) / sum(transaction_count) 
        ELSE 0 
    END as avg_fee,
    
    max(max_fee) as max_fee,
    min(min_fee) as min_fee,
    avg(median_transaction_amount) as median_transaction_amount,
    avg(avg_amount_std_dev) as avg_amount_std_dev,
    
    -- Block information
    min(start_block) as period_start_block,
    max(end_block) as period_end_block,
    max(end_block) - min(start_block) + 1 as blocks_in_period
    
FROM balance_transfers_daily_mv
GROUP BY toStartOfMonth(date), asset
ORDER BY period DESC, asset;

-- =============================================================================
-- ADDRESS ANALYTICS VIEW (with consistent naming)
-- =============================================================================

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
        
        -- Time-based patterns
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 0 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 6) as night_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 6 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 12) as morning_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 12 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 18) as afternoon_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 18 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 24) as evening_transactions,
        
        -- Histogram bins using function
        countIf(getAmountBin(amount) = 'lt_01') as tx_count_lt_01,
        countIf(getAmountBin(amount) = '01_to_1') as tx_count_01_to_1,
        countIf(getAmountBin(amount) = '1_to_10') as tx_count_1_to_10,
        countIf(getAmountBin(amount) = '10_to_100') as tx_count_10_to_100,
        countIf(getAmountBin(amount) = '100_to_1k') as tx_count_100_to_1k,
        countIf(getAmountBin(amount) = '1k_to_10k') as tx_count_1k_to_10k,
        countIf(getAmountBin(amount) = 'gte_10k') as tx_count_gte_10k,
        
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
    
    -- Risk score using function
    calculateRiskScore(
        total_transactions,
        night_transactions,
        avg_sent_amount,
        sent_amount_variance,
        unique_recipients,
        tx_count_gte_10k
    ) as risk_score,
    
    -- Risk level based on score
    CASE
        WHEN calculateRiskScore(total_transactions, night_transactions, avg_sent_amount, 
                               sent_amount_variance, unique_recipients, tx_count_gte_10k) >= 3 THEN 'High'
        WHEN calculateRiskScore(total_transactions, night_transactions, avg_sent_amount, 
                               sent_amount_variance, unique_recipients, tx_count_gte_10k) >= 2 THEN 'Medium'
        WHEN calculateRiskScore(total_transactions, night_transactions, avg_sent_amount, 
                               sent_amount_variance, unique_recipients, tx_count_gte_10k) >= 1 THEN 'Low'
        ELSE 'Normal'
    END as risk_level,
    
    -- Suspicion indicators (for transparency)
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
    
    -- Address classification
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
    
    -- Volume classification
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
-- DAILY PATTERNS VIEW (with consistent naming)
-- =============================================================================

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
    
    -- Daily histogram using function
    countIf(getAmountBin(amount) = 'lt_01') as daily_tx_count_lt_01,
    countIf(getAmountBin(amount) = '01_to_1') as daily_tx_count_01_to_1,
    countIf(getAmountBin(amount) = '1_to_10') as daily_tx_count_1_to_10,
    countIf(getAmountBin(amount) = '10_to_100') as daily_tx_count_10_to_100,
    countIf(getAmountBin(amount) = '100_to_1k') as daily_tx_count_100_to_1k,
    countIf(getAmountBin(amount) = '1k_to_10k') as daily_tx_count_1k_to_10k,
    countIf(getAmountBin(amount) = 'gte_10k') as daily_tx_count_gte_10k,
    
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
-- VOLUME AGGREGATION VIEWS (with consistent naming)
-- =============================================================================

-- Daily volume view (direct from daily MV with consistent naming)
CREATE VIEW IF NOT EXISTS balance_transfers_volume_daily_view AS
SELECT
    date,
    asset,
    
    -- Basic aggregations (consistent naming)
    transaction_count as daily_transaction_count,
    unique_senders,           -- No prefix needed
    unique_receivers,         -- No prefix needed
    total_volume as daily_total_volume,
    total_fees as daily_total_fees,
    CASE
        WHEN transaction_count > 0
        THEN total_volume / transaction_count
        ELSE 0
    END as daily_avg_transaction_amount,
    max_transaction_amount as daily_max_transaction_amount,
    min_transaction_amount as daily_min_transaction_amount,
    
    -- Network metrics
    active_addresses as daily_active_addresses,
    avg_network_density as avg_daily_network_density,
    
    -- Statistical aggregations
    avg_amount_std_dev as avg_daily_amount_std_dev,
    median_transaction_amount as avg_daily_median_amount,
    
    -- Histogram aggregations
    tx_count_lt_01 as daily_tx_count_lt_01,
    tx_count_01_to_1 as daily_tx_count_01_to_1,
    tx_count_1_to_10 as daily_tx_count_1_to_10,
    tx_count_10_to_100 as daily_tx_count_10_to_100,
    tx_count_100_to_1k as daily_tx_count_100_to_1k,
    tx_count_1k_to_10k as daily_tx_count_1k_to_10k,
    tx_count_gte_10k as daily_tx_count_gte_10k,
    
    volume_lt_01 as daily_volume_lt_01,
    volume_01_to_1 as daily_volume_01_to_1,
    volume_1_to_10 as daily_volume_1_to_10,
    volume_10_to_100 as daily_volume_10_to_100,
    volume_100_to_1k as daily_volume_100_to_1k,
    volume_1k_to_10k as daily_volume_1k_to_10k,
    volume_gte_10k as daily_volume_gte_10k,
    
    -- Block information
    start_block as daily_start_block,
    end_block as daily_end_block
    
FROM balance_transfers_daily_mv
ORDER BY date DESC, asset;

-- Weekly volume view (aggregates from daily MV with consistent naming)
CREATE VIEW IF NOT EXISTS balance_transfers_volume_weekly_view AS
SELECT
    toStartOfWeek(date) as week_start,
    asset,
    
    -- Aggregations from daily (consistent naming)
    sum(transaction_count) as weekly_transaction_count,
    max(unique_senders) as unique_senders,       -- Consistent
    max(unique_receivers) as unique_receivers,   -- Consistent
    sum(total_volume) as weekly_total_volume,
    sum(total_fees) as weekly_total_fees,
    CASE
        WHEN sum(transaction_count) > 0
        THEN sum(total_volume) / sum(transaction_count)
        ELSE 0
    END as weekly_avg_transaction_amount,
    max(max_transaction_amount) as weekly_max_transaction_amount,
    
    -- Network metrics
    max(active_addresses) as weekly_active_addresses,
    
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
    min(start_block) as weekly_start_block,
    max(end_block) as weekly_end_block
    
FROM balance_transfers_daily_mv
GROUP BY week_start, asset
ORDER BY week_start DESC, asset;

-- Monthly volume view (aggregates from daily MV with consistent naming)
CREATE VIEW IF NOT EXISTS balance_transfers_volume_monthly_view AS
SELECT
    toStartOfMonth(date) as month_start,
    asset,
    
    -- Aggregations from daily (consistent naming)
    sum(transaction_count) as monthly_transaction_count,
    max(unique_senders) as unique_senders,       -- Consistent
    max(unique_receivers) as unique_receivers,   -- Consistent
    sum(total_volume) as monthly_total_volume,
    sum(total_fees) as monthly_total_fees,
    CASE
        WHEN sum(transaction_count) > 0
        THEN sum(total_volume) / sum(transaction_count)
        ELSE 0
    END as monthly_avg_transaction_amount,
    max(max_transaction_amount) as monthly_max_transaction_amount,
    
    -- Network metrics
    max(active_addresses) as monthly_active_addresses,
    
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
    min(start_block) as monthly_start_block,
    max(end_block) as monthly_end_block
    
FROM balance_transfers_daily_mv
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
    
    -- Trend indicators
    CASE
        WHEN total_volume > avg(total_volume) OVER (
            PARTITION BY asset
            ORDER BY period_start
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) * 1.5 THEN 'Spike'
        WHEN total_volume < avg(total_volume) OVER (
            PARTITION BY asset
            ORDER BY period_start
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) * 0.5 THEN 'Drop'
        ELSE 'Normal'
    END as volume_trend
    
FROM balance_transfers_volume_series_mv
ORDER BY period_start DESC, asset;

-- =============================================================================
-- COMPLETE VOLUME QUANTILES VIEW (previously incomplete)
-- =============================================================================

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
    quantile(0.25)(transaction_count) as q25_tx_count,
    quantile(0.50)(transaction_count) as median_tx_count,
    quantile(0.75)(transaction_count) as q75_tx_count,
    quantile(0.90)(transaction_count) as q90_tx_count,
    quantile(0.99)(transaction_count) as q99_tx_count,
    
    -- Distribution metrics
    max(total_volume) - min(total_volume) as volume_range,
    stddevPop(total_volume) as volume_std_dev,
    varPop(total_volume) as volume_variance,
    
    -- Outlier detection
    count() as periods_in_day,
    countIf(total_volume > quantile(0.95)(total_volume)) as high_volume_periods,
    countIf(total_volume < quantile(0.05)(total_volume)) as low_volume_periods,
    
    -- Concentration metrics
    sum(total_volume) as daily_total_volume,
    max(total_volume) / sum(total_volume) as max_volume_concentration,
    
    -- Period statistics
    avg(transaction_count) as avg_period_tx_count,
    stddevPop(transaction_count) as tx_count_std_dev,
    
    -- Network density distribution
    min(network_density) as min_network_density,
    max(network_density) as max_network_density,
    avg(network_density) as avg_network_density,
    
    -- Fee distribution
    quantile(0.50)(avg_fee) as median_avg_fee,
    quantile(0.90)(max_fee) as q90_max_fee
    
FROM balance_transfers_volume_series_mv
GROUP BY date, asset
ORDER BY date DESC, asset;

-- =============================================================================
-- NAMING CONSISTENCY SUMMARY
-- =============================================================================
-- All views now use consistent naming:
-- - unique_senders (not max_unique_senders)
-- - unique_receivers (not max_unique_receivers)
-- - avg_transaction_amount (not avg_transfer_amount)
-- - median_transaction_amount (not median_transfer_amount)
-- - Network density calculation standardized via calculateNetworkDensity() function
-- - All histogram calculations use getAmountBin() function
-- - Risk scoring uses calculateRiskScore() function