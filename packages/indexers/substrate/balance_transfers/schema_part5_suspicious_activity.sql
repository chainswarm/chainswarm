-- Balance Transfers Schema - Part 5: Suspicious Activity Detection View
-- Identify potentially suspicious or anomalous activity patterns

-- =============================================================================
-- SUSPICIOUS ACTIVITY DETECTION VIEW
-- =============================================================================

-- Suspicious Activity Detection View
-- Identify potentially suspicious or anomalous activity patterns using asset-agnostic thresholds
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