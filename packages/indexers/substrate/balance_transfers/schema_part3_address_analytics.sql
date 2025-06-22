-- =====================================================================================
-- Address Analytics View - Consolidated Analysis
-- =====================================================================================
-- Replaces: schema_part3_behavior_profiles.sql, schema_part4_classification.sql, 
--           schema_part5_suspicious_activity.sql
-- 
-- This view provides comprehensive address analysis including:
-- - Behavior profiles with universal histogram bins
-- - Address classification (computed)
-- - Suspicious activity detection (computed)
-- - Risk scoring and assessment
-- 
-- Uses CTE patterns for ClickHouse 24.11.1 compatibility
-- =====================================================================================

-- Address Analytics View with comprehensive behavior, classification, and risk analysis
CREATE VIEW balance_transfers_address_analytics_view AS
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
    END) as risk_score
    
FROM address_combined_metrics
WHERE total_transactions > 0;

-- Add computed risk level as a separate view for easier querying
CREATE VIEW balance_transfers_address_risk_levels_view AS
SELECT
    *,
    CASE
        WHEN risk_score >= 3 THEN 'High'
        WHEN risk_score >= 2 THEN 'Medium'
        WHEN risk_score >= 1 THEN 'Low'
        ELSE 'Normal'
    END as risk_level
FROM balance_transfers_address_analytics_view;