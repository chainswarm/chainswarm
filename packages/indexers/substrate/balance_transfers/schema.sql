-- Balance Transfers Schema
-- Tables and views for tracking individual transfer transactions on Substrate networks

-- Drop potentially invalid objects first to ensure clean recreation
DROP VIEW IF EXISTS balance_transfers_address_relationships_mv;
DROP VIEW IF EXISTS balance_transfers_pairs_analysis_view;
DROP VIEW IF EXISTS balance_transfers_address_clusters_view;
DROP VIEW IF EXISTS balance_transfers_suspicious_activity_view;
DROP VIEW IF EXISTS balance_transfers_address_activity_patterns_view;
DROP VIEW IF EXISTS balance_transfers_address_classification_view;
DROP VIEW IF EXISTS balance_transfers_address_behavior_profiles_view;

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

-- Materialized view for daily transfer volume
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

-- Indexes for efficient queries

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

-- Simple view for available assets list
CREATE VIEW IF NOT EXISTS available_transfer_assets_view AS
SELECT DISTINCT asset
FROM balance_transfers
WHERE asset != ''
ORDER BY asset;

-- Enhanced Address Analytics Views and Materialized Views

-- Address Behavior Profiles View
-- Comprehensive behavioral analysis for each address
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
    quantileIf(0.5)(amount, address = from_address) as median_sent_amount,
    quantileIf(0.5)(amount, address = to_address) as median_received_amount,
    quantileIf(0.9)(amount, address = from_address) as p90_sent_amount,
    quantileIf(0.9)(amount, address = to_address) as p90_received_amount,
    
    -- Temporal Patterns
    min(block_timestamp) as first_activity,
    max(block_timestamp) as last_activity,
    max(block_timestamp) - min(block_timestamp) as activity_span_seconds,
    count() / ((max(block_timestamp) - min(block_timestamp)) / 86400 + 1) as avg_transactions_per_day,
    
    -- Unique Counterparties
    uniqIf(to_address, address = from_address) as unique_recipients,
    uniqIf(from_address, address = to_address) as unique_senders,
    uniq(multiIf(address = from_address, to_address, from_address)) as total_unique_counterparties,
    
    -- Fee Analysis
    sumIf(fee, address = from_address) as total_fees_paid,
    avgIf(fee, address = from_address) as avg_fee_paid,
    maxIf(fee, address = from_address) as max_fee_paid,
    
    -- Activity Distribution by Time of Day
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 0 AND 5) as night_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 6 AND 11) as morning_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 12 AND 17) as afternoon_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 18 AND 23) as evening_transactions,
    
    -- Transaction Size Distribution
    countIf(amount < 100) as micro_transactions,
    countIf(amount >= 100 AND amount < 1000) as small_transactions,
    countIf(amount >= 1000 AND amount < 10000) as medium_transactions,
    countIf(amount >= 10000 AND amount < 100000) as large_transactions,
    countIf(amount >= 100000) as whale_transactions,
    
    -- Behavioral Indicators
    stddevPopIf(amount, address = from_address) as sent_amount_variance,
    stddevPopIf(amount, address = to_address) as received_amount_variance,
    uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as active_days,
    
    -- Network Effect Metrics (simplified to avoid subquery issues)
    0 as circular_transactions
    
FROM (
    SELECT from_address as address, from_address, to_address, asset, amount, fee, block_height, block_timestamp FROM balance_transfers
    UNION ALL
    SELECT to_address as address, from_address, to_address, asset, amount, 0 as fee, block_height, block_timestamp FROM balance_transfers
)
GROUP BY address, asset
HAVING total_transactions >= 1;

-- Address Classification View
-- Classify addresses into behavioral categories with risk assessment
CREATE VIEW IF NOT EXISTS balance_transfers_address_classification_view AS
SELECT
    address,
    asset,
    total_volume,
    total_transactions,
    unique_recipients,
    unique_senders,
    total_unique_counterparties,
    avg_transactions_per_day,
    activity_span_seconds,
    
    -- Primary Classification Logic
    CASE
        WHEN total_volume >= 1000000 AND unique_recipients >= 100 THEN 'Exchange'
        WHEN total_volume >= 1000000 AND unique_recipients < 10 THEN 'Whale'
        WHEN total_volume >= 100000 AND total_transactions >= 1000 THEN 'High_Volume_Trader'
        WHEN unique_recipients >= 50 AND unique_senders >= 50 THEN 'Hub_Address'
        WHEN total_transactions >= 100 AND total_volume < 10000 THEN 'Retail_Active'
        WHEN total_transactions < 10 AND total_volume >= 50000 THEN 'Whale_Inactive'
        WHEN total_transactions < 10 AND total_volume < 1000 THEN 'Retail_Inactive'
        WHEN circular_transactions >= total_transactions * 0.3 THEN 'Circular_Trader'
        ELSE 'Regular_User'
    END as address_type,
    
    -- Risk Assessment
    CASE
        WHEN unique_recipients = 1 AND total_transactions > 100 THEN 'Potential_Mixer'
        WHEN abs(avg_sent_amount - avg_received_amount) < avg_sent_amount * 0.05 AND total_transactions > 50 THEN 'Potential_Tumbler'
        WHEN night_transactions / total_transactions > 0.8 THEN 'Unusual_Hours'
        WHEN whale_transactions > 0 AND total_transactions < 20 THEN 'Large_Infrequent'
        WHEN sent_amount_variance < avg_sent_amount * 0.1 AND total_transactions > 20 THEN 'Fixed_Amount_Pattern'
        WHEN circular_transactions >= 10 THEN 'High_Circular_Activity'
        ELSE 'Normal'
    END as risk_flag,
    
    -- Activity Level Classification
    CASE
        WHEN activity_span_seconds < 86400 THEN 'Single_Day'
        WHEN activity_span_seconds < 604800 THEN 'Weekly'
        WHEN activity_span_seconds < 2592000 THEN 'Monthly'
        WHEN activity_span_seconds < 31536000 THEN 'Yearly'
        ELSE 'Long_Term'
    END as activity_duration,
    
    -- Volume Classification
    CASE
        WHEN total_volume >= 1000000 THEN 'Ultra_High'
        WHEN total_volume >= 100000 THEN 'High'
        WHEN total_volume >= 10000 THEN 'Medium'
        WHEN total_volume >= 1000 THEN 'Low'
        ELSE 'Micro'
    END as volume_tier,
    
    -- Activity Pattern
    CASE
        WHEN avg_transactions_per_day >= 10 THEN 'Very_Active'
        WHEN avg_transactions_per_day >= 1 THEN 'Active'
        WHEN avg_transactions_per_day >= 0.1 THEN 'Moderate'
        ELSE 'Inactive'
    END as activity_level,
    
    -- Diversification Score (0-1, higher = more diversified)
    least(total_unique_counterparties / 100.0, 1.0) as diversification_score,
    
    -- Behavioral Consistency Score (0-1, higher = more consistent)
    1.0 - least(greatest(sent_amount_variance / nullif(avg_sent_amount, 0), 0), 1.0) as consistency_score
    
FROM balance_transfers_address_behavior_profiles_view;

-- Address Relationships View (simplified for debugging)
-- Track and analyze relationships between addresses based on transfer patterns
CREATE VIEW IF NOT EXISTS balance_transfers_address_relationships_view AS
SELECT
    from_address,
    to_address,
    asset,
    count() as transfer_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
FROM balance_transfers
WHERE from_address != to_address
GROUP BY from_address, to_address, asset
HAVING count() >= 2;

-- Transfer Pairs Analysis View (simplified to match available columns)
-- Identify significant address pairs and classify their interaction patterns
CREATE VIEW IF NOT EXISTS balance_transfers_pairs_analysis_view AS
SELECT
    from_address,
    to_address,
    asset,
    transfer_count,
    total_amount,
    avg_amount,
    
    -- Simplified Relationship Strength Score (0-10 scale)
    least(
        (transfer_count * 0.4 + log10(total_amount + 1) * 0.6) * 2,
        10.0
    ) as relationship_strength,
    
    -- Simplified Relationship Type Classification
    CASE
        WHEN total_amount >= 100000 AND transfer_count >= 10 THEN 'High_Value'
        WHEN transfer_count >= 100 THEN 'High_Frequency'
        WHEN transfer_count >= 5 THEN 'Regular'
        ELSE 'Casual'
    END as relationship_type
    
FROM balance_transfers_address_relationships_view ar
WHERE transfer_count >= 2  -- Filter for significant relationships
ORDER BY relationship_strength DESC;

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
    
    -- Transaction Size Distribution
    countIf(amount < 100) as micro_transactions,
    countIf(amount >= 100 AND amount < 1000) as small_transactions,
    countIf(amount >= 1000 AND amount < 10000) as medium_transactions,
    countIf(amount >= 10000 AND amount < 100000) as large_transactions,
    countIf(amount >= 100000) as whale_transactions,
    
    -- Statistical Measures
    avg(amount) as daily_avg_amount,
    median(amount) as daily_median_amount,
    stddevPop(amount) as daily_amount_variance,
    
    -- Counterparty Analysis
    uniqIf(to_address, address = from_address) as daily_unique_recipients,
    uniqIf(from_address, address = to_address) as daily_unique_senders,
    uniq(multiIf(address = from_address, to_address, from_address)) as daily_unique_counterparties,
    
    -- Fee Patterns
    sumIf(fee, address = from_address) as daily_fees_paid,
    avgIf(fee, address = from_address) as avg_daily_fee,
    
    -- Behavioral Indicators (simplified to avoid subquery issues)
    0 as daily_circular_transactions
    
FROM (
    SELECT from_address as address, from_address, to_address, asset, amount, fee, block_height, block_timestamp FROM balance_transfers
    UNION ALL
    SELECT to_address as address, from_address, to_address, asset, amount, 0 as fee, block_height, block_timestamp FROM balance_transfers
)
GROUP BY address, asset, activity_date;


-- Suspicious Activity Detection View
-- Identify potentially suspicious or anomalous activity patterns
CREATE VIEW IF NOT EXISTS balance_transfers_suspicious_activity_view AS
SELECT
    ac.address,
    ac.asset,
    ac.address_type,
    ac.risk_flag,
    abp.total_transactions,
    abp.total_volume,
    abp.avg_transactions_per_day,
    
    -- Suspicious Pattern Indicators
    CASE
        WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0
    END as has_risk_flag,
    
    CASE
        WHEN abp.circular_transactions >= toFloat64(abp.total_transactions) * 0.5 THEN 1 ELSE 0
    END as high_circular_activity,
    
    CASE
        WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0
    END as unusual_time_pattern,
    
    CASE
        WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0
    END as fixed_amount_pattern,
    
    CASE
        WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0
    END as single_recipient_pattern,
    
    CASE
        WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0
    END as large_infrequent_pattern,
    
    -- Composite Suspicion Score (0-6)
    (CASE WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0 END +
     CASE WHEN abp.circular_transactions >= toFloat64(abp.total_transactions) * 0.5 THEN 1 ELSE 0 END +
     CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
     CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
     CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
     CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) as suspicion_score,
    
    -- Risk Level
    CASE
        WHEN (CASE WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0 END +
              CASE WHEN abp.circular_transactions >= toFloat64(abp.total_transactions) * 0.5 THEN 1 ELSE 0 END +
              CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 4 THEN 'High'
        WHEN (CASE WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0 END +
              CASE WHEN abp.circular_transactions >= toFloat64(abp.total_transactions) * 0.5 THEN 1 ELSE 0 END +
              CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 2 THEN 'Medium'
        WHEN (CASE WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0 END +
              CASE WHEN abp.circular_transactions >= toFloat64(abp.total_transactions) * 0.5 THEN 1 ELSE 0 END +
              CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 1 THEN 'Low'
        ELSE 'Normal'
    END as risk_level
    
FROM balance_transfers_address_classification_view ac
JOIN balance_transfers_address_behavior_profiles_view abp ON ac.address = abp.address AND ac.asset = abp.asset
WHERE abp.total_transactions >= 5  -- Minimum transactions for analysis
ORDER BY suspicion_score DESC, abp.total_volume DESC;

-- Enhanced Indexes for Address Analytics

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
