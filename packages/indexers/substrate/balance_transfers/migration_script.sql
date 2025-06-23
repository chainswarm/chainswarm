-- Balance Transfers Schema Migration Script
-- Migrates from the old duplicated schema to the new refactored architecture
-- Run this script to safely transition to the optimized schema

-- =============================================================================
-- STEP 1: CREATE NEW FUNCTIONS (Safe - doesn't affect existing schema)
-- =============================================================================

-- Create histogram bin classification function
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

-- Create risk score calculation function
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

-- =============================================================================
-- STEP 2: CREATE DAILY MATERIALIZED VIEW (New layer in architecture)
-- =============================================================================

-- This view aggregates from the existing 4-hour MV
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_daily_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, asset)
POPULATE
AS
SELECT
    toDate(period_start) as date,
    asset,
    
    -- Aggregate metrics from 4-hour periods
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    sum(total_fees) as total_fees,
    
    -- For unique counts, take the max from 4-hour periods
    max(unique_senders) as unique_senders,
    max(unique_receivers) as unique_receivers,
    max(active_addresses) as active_addresses,
    
    -- Network metrics
    avg(network_density) as avg_network_density,
    
    -- Statistical aggregations
    avg(median_transfer_amount) as median_amount,
    max(max_transfer_amount) as max_amount,
    min(min_transfer_amount) as min_amount,
    avg(amount_std_dev) as avg_std_dev,
    
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
GROUP BY date, asset;

-- =============================================================================
-- STEP 3: RENAME OLD VIEWS (Keep as backup during transition)
-- =============================================================================

-- Rename network views
RENAME TABLE balance_transfers_network_daily_view TO balance_transfers_network_daily_view_old;
RENAME TABLE balance_transfers_network_weekly_view TO balance_transfers_network_weekly_view_old;
RENAME TABLE balance_transfers_network_monthly_view TO balance_transfers_network_monthly_view_old;

-- Rename volume views
RENAME TABLE balance_transfers_volume_daily_view TO balance_transfers_volume_daily_view_old;
RENAME TABLE balance_transfers_volume_weekly_view TO balance_transfers_volume_weekly_view_old;
RENAME TABLE balance_transfers_volume_monthly_view TO balance_transfers_volume_monthly_view_old;

-- Rename address analytics view
RENAME TABLE balance_transfers_address_analytics_view TO balance_transfers_address_analytics_view_old;

-- Rename patterns view
RENAME TABLE balance_transfers_daily_patterns_view TO balance_transfers_daily_patterns_view_old;

-- =============================================================================
-- STEP 4: CREATE NEW OPTIMIZED VIEWS
-- =============================================================================

-- Create new network daily view (from daily MV)
CREATE VIEW balance_transfers_network_daily_view AS
SELECT
    'daily' as period_type,
    date as period,
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
    avg_network_density,
    total_fees,
    
    -- Additional statistics
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_size,
    max_amount as max_transaction_size,
    min_amount as min_transaction_size,
    avg_fee,
    max_fee,
    min_fee,
    median_amount as median_transaction_size,
    avg_std_dev as avg_amount_std_dev,
    
    -- Block information
    start_block as period_start_block,
    end_block as period_end_block,
    end_block - start_block + 1 as blocks_in_period

FROM balance_transfers_daily_mv
ORDER BY period DESC, asset;

-- Create new network weekly view (from daily MV)
CREATE VIEW balance_transfers_network_weekly_view AS
SELECT
    'weekly' as period_type,
    toStartOfWeek(date) as period,
    asset,
    
    -- Aggregate from daily data
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
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
    
    -- Calculated fields
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_volume) / sum(transaction_count) 
        ELSE 0 
    END as avg_transaction_size,
    
    max(max_amount) as max_transaction_size,
    min(min_amount) as min_transaction_size,
    
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_fees) / sum(transaction_count) 
        ELSE 0 
    END as avg_fee,
    
    max(max_fee) as max_fee,
    min(min_fee) as min_fee,
    avg(median_amount) as median_transaction_size,
    avg(avg_std_dev) as avg_amount_std_dev,
    
    -- Block information
    min(start_block) as period_start_block,
    max(end_block) as period_end_block,
    max(end_block) - min(start_block) + 1 as blocks_in_period
    
FROM balance_transfers_daily_mv
GROUP BY toStartOfWeek(date), asset
ORDER BY period DESC, asset;

-- Create new network monthly view (from daily MV)
CREATE VIEW balance_transfers_network_monthly_view AS
SELECT
    'monthly' as period_type,
    toStartOfMonth(date) as period,
    asset,
    
    -- Same pattern as weekly
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
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
    
    -- Calculated fields
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_volume) / sum(transaction_count) 
        ELSE 0 
    END as avg_transaction_size,
    
    max(max_amount) as max_transaction_size,
    min(min_amount) as min_transaction_size,
    
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_fees) / sum(transaction_count) 
        ELSE 0 
    END as avg_fee,
    
    max(max_fee) as max_fee,
    min(min_fee) as min_fee,
    avg(median_amount) as median_transaction_size,
    avg(avg_std_dev) as avg_amount_std_dev,
    
    -- Block information
    min(start_block) as period_start_block,
    max(end_block) as period_end_block,
    max(end_block) - min(start_block) + 1 as blocks_in_period
    
FROM balance_transfers_daily_mv
GROUP BY toStartOfMonth(date), asset
ORDER BY period DESC, asset;

-- =============================================================================
-- STEP 5: VALIDATION QUERIES
-- =============================================================================

-- Compare old vs new weekly aggregations
SELECT
    'Weekly Validation' as check_type,
    old.period,
    old.asset,
    old.transaction_count as old_count,
    new.transaction_count as new_count,
    abs(old.transaction_count - new.transaction_count) as diff,
    CASE 
        WHEN old.transaction_count > 0 
        THEN abs(old.transaction_count - new.transaction_count) / old.transaction_count * 100
        ELSE 0
    END as diff_pct
FROM balance_transfers_network_weekly_view_old old
JOIN balance_transfers_network_weekly_view new 
    ON old.period = new.period AND old.asset = new.asset
WHERE old.period >= now() - INTERVAL 1 MONTH
ORDER BY diff_pct DESC
LIMIT 10;

-- Compare old vs new monthly aggregations
SELECT
    'Monthly Validation' as check_type,
    old.period,
    old.asset,
    old.total_volume as old_volume,
    new.total_volume as new_volume,
    abs(old.total_volume - new.total_volume) as diff,
    CASE 
        WHEN old.total_volume > 0 
        THEN abs(old.total_volume - new.total_volume) / old.total_volume * 100
        ELSE 0
    END as diff_pct
FROM balance_transfers_network_monthly_view_old old
JOIN balance_transfers_network_monthly_view new 
    ON old.period = new.period AND old.asset = new.asset
WHERE old.period >= now() - INTERVAL 3 MONTH
ORDER BY diff_pct DESC
LIMIT 10;

-- =============================================================================
-- STEP 6: PERFORMANCE COMPARISON
-- =============================================================================

-- Test query performance on old weekly view
SELECT 
    'Old Weekly View' as test_name,
    now() as start_time,
    count(*) as row_count
FROM balance_transfers_network_weekly_view_old
WHERE period >= now() - INTERVAL 3 MONTH
SETTINGS max_execution_time = 60;

-- Test query performance on new weekly view
SELECT 
    'New Weekly View' as test_name,
    now() as start_time,
    count(*) as row_count
FROM balance_transfers_network_weekly_view
WHERE period >= now() - INTERVAL 3 MONTH
SETTINGS max_execution_time = 60;

-- =============================================================================
-- STEP 7: CLEANUP (Run only after validation)
-- =============================================================================

-- After confirming the new views work correctly, drop the old ones
-- IMPORTANT: Only run these after thorough testing!

-- DROP VIEW IF EXISTS balance_transfers_network_daily_view_old;
-- DROP VIEW IF EXISTS balance_transfers_network_weekly_view_old;
-- DROP VIEW IF EXISTS balance_transfers_network_monthly_view_old;
-- DROP VIEW IF EXISTS balance_transfers_volume_daily_view_old;
-- DROP VIEW IF EXISTS balance_transfers_volume_weekly_view_old;
-- DROP VIEW IF EXISTS balance_transfers_volume_monthly_view_old;
-- DROP VIEW IF EXISTS balance_transfers_address_analytics_view_old;
-- DROP VIEW IF EXISTS balance_transfers_daily_patterns_view_old;

-- =============================================================================
-- ROLLBACK SCRIPT (In case of issues)
-- =============================================================================

-- To rollback to the old schema:
-- 1. Drop the new views
-- DROP VIEW IF EXISTS balance_transfers_network_daily_view;
-- DROP VIEW IF EXISTS balance_transfers_network_weekly_view;
-- DROP VIEW IF EXISTS balance_transfers_network_monthly_view;

-- 2. Rename old views back
-- RENAME TABLE balance_transfers_network_daily_view_old TO balance_transfers_network_daily_view;
-- RENAME TABLE balance_transfers_network_weekly_view_old TO balance_transfers_network_weekly_view;
-- RENAME TABLE balance_transfers_network_monthly_view_old TO balance_transfers_network_monthly_view;

-- 3. Drop the daily MV if needed
-- DROP VIEW IF EXISTS balance_transfers_daily_mv;

-- 4. Drop the new functions
-- DROP FUNCTION IF EXISTS getAmountBin;
-- DROP FUNCTION IF EXISTS calculateRiskScore;