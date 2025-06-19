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
SETTINGS index_granularity = 8192
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
    
    -- Simplified transaction size counts
    countIf(amount < 10) as micro_transactions,
    countIf(amount >= 10 AND amount < 100) as small_transactions,
    countIf(amount >= 100 AND amount < 1000) as medium_transactions,
    countIf(amount >= 1000 AND amount < 10000) as large_transactions,
    countIf(amount >= 10000) as whale_transactions,
    
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
    
    -- Primary Classification Logic with USD-Equivalent Thresholds
    if(asset = 'TOR',
        if(total_volume >= 1000000 AND unique_recipients >= 100, 'Exchange',
        if(total_volume >= 1000000 AND unique_recipients < 10, 'Whale',
        if(total_volume >= 100000 AND total_transactions >= 1000, 'High_Volume_Trader',
        if(unique_recipients >= 50 AND unique_senders >= 50, 'Hub_Address',
        if(total_transactions >= 100 AND total_volume < 10000, 'Retail_Active',
        if(total_transactions < 10 AND total_volume >= 50000, 'Whale_Inactive',
        if(total_transactions < 10 AND total_volume < 1000, 'Retail_Inactive', 'Regular_User'))))))),
    if(asset = 'TAO',
        if(total_volume >= 3000 AND unique_recipients >= 100, 'Exchange',
        if(total_volume >= 3000 AND unique_recipients < 10, 'Whale',
        if(total_volume >= 300 AND total_transactions >= 1000, 'High_Volume_Trader',
        if(unique_recipients >= 50 AND unique_senders >= 50, 'Hub_Address',
        if(total_transactions >= 100 AND total_volume < 30, 'Retail_Active',
        if(total_transactions < 10 AND total_volume >= 150, 'Whale_Inactive',
        if(total_transactions < 10 AND total_volume < 3, 'Retail_Inactive', 'Regular_User'))))))),
    if(asset = 'DOT',
        if(total_volume >= 250000 AND unique_recipients >= 100, 'Exchange',
        if(total_volume >= 250000 AND unique_recipients < 10, 'Whale',
        if(total_volume >= 25000 AND total_transactions >= 1000, 'High_Volume_Trader',
        if(unique_recipients >= 50 AND unique_senders >= 50, 'Hub_Address',
        if(total_transactions >= 100 AND total_volume < 2500, 'Retail_Active',
        if(total_transactions < 10 AND total_volume >= 12500, 'Whale_Inactive',
        if(total_transactions < 10 AND total_volume < 250, 'Retail_Inactive', 'Regular_User'))))))),
        'Regular_User'))) as address_type,
    
    -- Volume Classification with USD-Equivalent Thresholds
    if(asset = 'TOR',
        if(total_volume >= 100000, 'Ultra_High',
        if(total_volume >= 10000, 'High',
        if(total_volume >= 1000, 'Medium',
        if(total_volume >= 100, 'Low', 'Micro')))),
    if(asset = 'TAO',
        if(total_volume >= 300, 'Ultra_High',
        if(total_volume >= 30, 'High',
        if(total_volume >= 3, 'Medium',
        if(total_volume >= 0.5, 'Low', 'Micro')))),
    if(asset = 'DOT',
        if(total_volume >= 25000, 'Ultra_High',
        if(total_volume >= 2500, 'High',
        if(total_volume >= 250, 'Medium',
        if(total_volume >= 25, 'Low', 'Micro')))),
        if(total_volume >= 100000, 'Ultra_High',
        if(total_volume >= 10000, 'High',
        if(total_volume >= 1000, 'Medium',
        if(total_volume >= 100, 'Low', 'Micro'))))))) as volume_tier
    
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
    CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END as large_infrequent_pattern,
    
    -- Composite Suspicion Score (0-4)
    (CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
     CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
     CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
     CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) as suspicion_score,
    
    -- Risk Level
    CASE
        WHEN (CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 3 THEN 'High'
        WHEN (CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 2 THEN 'Medium'
        WHEN (CASE WHEN toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 1 THEN 'Low'
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
    
    -- Relationship Type Classification with USD-Equivalent Thresholds
    CASE asset
        WHEN 'TOR' THEN
            CASE
                WHEN total_amount >= 10000 AND transfer_count >= 10 THEN 'High_Value'  -- >= $10K USD
                WHEN transfer_count >= 100 THEN 'High_Frequency'
                WHEN transfer_count >= 5 THEN 'Regular'
                ELSE 'Casual'
            END
        WHEN 'TAO' THEN
            CASE
                WHEN total_amount >= 30 AND transfer_count >= 10 THEN 'High_Value'     -- >= $10K USD (rounded)
                WHEN transfer_count >= 100 THEN 'High_Frequency'
                WHEN transfer_count >= 5 THEN 'Regular'
                ELSE 'Casual'
            END
        WHEN 'DOT' THEN
            CASE
                WHEN total_amount >= 2500 AND transfer_count >= 10 THEN 'High_Value'   -- >= $10K USD (10K/4)
                WHEN transfer_count >= 100 THEN 'High_Frequency'
                WHEN transfer_count >= 5 THEN 'Regular'
                ELSE 'Casual'
            END
        ELSE
            CASE
                WHEN total_amount >= 10000 AND transfer_count >= 10 THEN 'High_Value'
                WHEN transfer_count >= 100 THEN 'High_Frequency'
                WHEN transfer_count >= 5 THEN 'Regular'
                ELSE 'Casual'
            END
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
    
    -- Transaction Size Distribution with Simplified USD-Equivalent Thresholds
    CASE asset
        WHEN 'TOR' THEN countIf(amount < 100)          -- < $100 USD
        WHEN 'TAO' THEN countIf(amount < 0.5)          -- < $175 USD (rounded)
        WHEN 'DOT' THEN countIf(amount < 25)           -- < $100 USD
        ELSE countIf(amount < 100)
    END as micro_transactions,
    
    CASE asset
        WHEN 'TOR' THEN countIf(amount >= 100 AND amount < 1000)     -- $100-$1K USD
        WHEN 'TAO' THEN countIf(amount >= 0.5 AND amount < 3)        -- $175-$1K USD
        WHEN 'DOT' THEN countIf(amount >= 25 AND amount < 250)       -- $100-$1K USD
        ELSE countIf(amount >= 100 AND amount < 1000)
    END as small_transactions,
    
    CASE asset
        WHEN 'TOR' THEN countIf(amount >= 1000 AND amount < 10000)   -- $1K-$10K USD
        WHEN 'TAO' THEN countIf(amount >= 3 AND amount < 30)         -- $1K-$10K USD
        WHEN 'DOT' THEN countIf(amount >= 250 AND amount < 2500)     -- $1K-$10K USD
        ELSE countIf(amount >= 1000 AND amount < 10000)
    END as medium_transactions,
    
    CASE asset
        WHEN 'TOR' THEN countIf(amount >= 10000 AND amount < 100000) -- $10K-$100K USD
        WHEN 'TAO' THEN countIf(amount >= 30 AND amount < 300)       -- $10K-$100K USD
        WHEN 'DOT' THEN countIf(amount >= 2500 AND amount < 25000)   -- $10K-$100K USD
        ELSE countIf(amount >= 10000 AND amount < 100000)
    END as large_transactions,
    
    CASE asset
        WHEN 'TOR' THEN countIf(amount >= 100000)      -- >= $100K USD
        WHEN 'TAO' THEN countIf(amount >= 300)         -- >= $100K USD
        WHEN 'DOT' THEN countIf(amount >= 25000)       -- >= $100K USD
        ELSE countIf(amount >= 100000)
    END as whale_transactions,
    
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
