-- =====================================================================================
-- Transaction Analytics View - Consolidated Relationship and Anomaly Analysis
-- =====================================================================================
-- Replaces: schema_part6_relationships_activity.sql and schema_part10_anomaly_detection.sql
-- 
-- This view provides comprehensive transaction analysis including:
-- - Address relationship analysis with strength scoring
-- - Transaction pattern analysis with universal histogram bins
-- - Anomaly detection for unusual transactions
-- - Activity pattern analysis (temporal, frequency, size)
-- 
-- Simplified for ClickHouse 24.11.1 compatibility
-- Avoids nested aggregations and complex CTEs
-- =====================================================================================

-- Simple Relationship Analysis View
CREATE VIEW balance_transfers_relationships_view AS
SELECT
    from_address,
    to_address,
    asset,
    count() as transfer_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    min(amount) as min_amount,
    max(amount) as max_amount,
    min(block_timestamp) as first_interaction,
    max(block_timestamp) as last_interaction,
    sum(fee) as total_fees,
    
    -- Universal histogram bins
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    
    -- Temporal patterns
    uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as active_days,
    uniq(toHour(toDateTime(intDiv(block_timestamp, 1000)))) as unique_active_hours,
    
    -- Time distribution
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 0 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 6) as night_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 6 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 12) as morning_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 12 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 18) as afternoon_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 18 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 24) as evening_transactions,
    
    -- Statistical measures
    varPop(amount) as amount_variance,
    stddevPop(amount) as amount_stddev
    
FROM balance_transfers
WHERE from_address != to_address
GROUP BY from_address, to_address, asset
HAVING transfer_count >= 1
ORDER BY total_amount DESC;

-- Enhanced Transaction Analytics View with computed fields
CREATE VIEW balance_transfers_transaction_analytics_view AS
SELECT
    from_address,
    to_address,
    asset,
    transfer_count,
    total_amount,
    avg_amount,
    min_amount,
    max_amount,
    first_interaction,
    last_interaction,
    total_fees,
    
    -- Histogram bins
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    
    -- Temporal metrics
    active_days,
    unique_active_hours,
    night_transactions,
    morning_transactions,
    afternoon_transactions,
    evening_transactions,
    amount_variance,
    amount_stddev,
    
    -- Computed relationship strength (0-10 scale)
    least(
        (toFloat64(transfer_count) * 0.3 + 
         log10(total_amount + 1) * 0.4 + 
         toFloat64(active_days) * 0.3) * 1.5, 
        10.0
    ) as relationship_strength,
    
    -- Activity span in days
    intDiv(last_interaction - first_interaction, 86400000) as activity_span_days,
    
    
    -- Computed anomaly indicators
    CASE 
        WHEN max_amount > avg_amount * 10 AND max_amount > 10000
        THEN 1 ELSE 0 
    END as has_extreme_amount,
    
    CASE 
        WHEN amount_stddev < avg_amount * 0.05 AND transfer_count >= 10 
        THEN 1 ELSE 0 
    END as has_fixed_amount_pattern,
    
    CASE 
        WHEN transfer_count > 0 AND toFloat64(night_transactions) >= toFloat64(transfer_count) * 0.8 
        THEN 1 ELSE 0 
    END as has_unusual_timing_pattern,
    
    -- Computed anomaly score
    (CASE 
        WHEN max_amount > avg_amount * 10 AND max_amount > 10000
        THEN 1 ELSE 0 
    END +
    CASE 
        WHEN amount_stddev < avg_amount * 0.05 AND transfer_count >= 10 
        THEN 1 ELSE 0 
    END +
    CASE 
        WHEN transfer_count > 0 AND toFloat64(night_transactions) >= toFloat64(transfer_count) * 0.8 
        THEN 1 ELSE 0 
    END) as anomaly_score,
    
    -- Computed anomaly level
    CASE
        WHEN anomaly_score >= 2 THEN 'High'
        WHEN anomaly_score >= 1 THEN 'Medium'
        ELSE 'Normal'
    END as anomaly_level,
    
    -- Activity distribution percentages
    CASE 
        WHEN transfer_count > 0 THEN (night_transactions * 100.0) / transfer_count 
        ELSE 0 
    END as night_activity_pct,
    CASE 
        WHEN transfer_count > 0 THEN (morning_transactions * 100.0) / transfer_count 
        ELSE 0 
    END as morning_activity_pct,
    CASE 
        WHEN transfer_count > 0 THEN (afternoon_transactions * 100.0) / transfer_count 
        ELSE 0 
    END as afternoon_activity_pct,
    CASE 
        WHEN transfer_count > 0 THEN (evening_transactions * 100.0) / transfer_count 
        ELSE 0 
    END as evening_activity_pct

FROM balance_transfers_relationships_view
ORDER BY relationship_strength DESC, total_amount DESC;

-- Simple Daily Activity Patterns View
CREATE VIEW balance_transfers_daily_patterns_view AS
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
    countIf(amount >= 10000) as daily_tx_count_gte_10k

FROM balance_transfers
WHERE from_address != to_address
GROUP BY from_address, to_address, asset, activity_date
ORDER BY activity_date DESC, daily_volume DESC;