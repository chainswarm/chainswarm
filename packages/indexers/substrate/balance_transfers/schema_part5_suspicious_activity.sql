-- Balance Transfers Schema - Part 5: Suspicious Activity Detection View
-- Identify potentially suspicious or anomalous activity patterns

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
    
    -- Suspicious Pattern Indicators with Asset-Specific Thresholds
    if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) as unusual_time_pattern,
    
    -- Fixed Amount Pattern with Asset-Specific Thresholds
    if(asset = 'TOR',
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0),
    if(asset = 'TAO',
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.03 AND abp.total_transactions >= 15, 1, 0),
    if(asset = 'DOT',
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.04 AND abp.total_transactions >= 18, 1, 0),
        -- Default case for other assets
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0)))) as fixed_amount_pattern,
    
    -- Single Recipient Pattern with Asset-Specific Thresholds
    if(asset = 'TOR',
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0),
    if(asset = 'TAO',
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 30, 1, 0),
    if(asset = 'DOT',
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 40, 1, 0),
        -- Default case for other assets
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0)))) as single_recipient_pattern,
    
    -- Large Infrequent Pattern with Asset-Specific Thresholds
    if(asset = 'TOR',
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0),
    if(asset = 'TAO',
        if(abp.large_transactions > 0 AND abp.total_transactions <= 3, 1, 0),
    if(asset = 'DOT',
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 4, 1, 0),
        -- Default case for other assets
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0)))) as large_infrequent_pattern,
    
    -- Composite Suspicion Score (0-4)
    (if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
     if(asset = 'TOR',
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0),
     if(asset = 'TAO',
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.03 AND abp.total_transactions >= 15, 1, 0),
     if(asset = 'DOT',
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.04 AND abp.total_transactions >= 18, 1, 0),
        -- Default case for other assets
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0)))) +
     if(asset = 'TOR',
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0),
     if(asset = 'TAO',
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 30, 1, 0),
     if(asset = 'DOT',
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 40, 1, 0),
        -- Default case for other assets
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0)))) +
     if(asset = 'TOR',
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0),
     if(asset = 'TAO',
        if(abp.large_transactions > 0 AND abp.total_transactions <= 3, 1, 0),
     if(asset = 'DOT',
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 4, 1, 0),
        -- Default case for other assets
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0))))) as suspicion_score,
    
    -- Risk Level with Asset-Specific Thresholds
    if(asset = 'TOR',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0) +
            if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0)) >= 3, 'High',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0) +
            if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0)) >= 2, 'Medium',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0) +
            if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0)) >= 1, 'Low', 'Normal'))),
    if(asset = 'TAO',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.03 AND abp.total_transactions >= 15, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 30, 1, 0) +
            if(abp.large_transactions > 0 AND abp.total_transactions <= 3, 1, 0)) >= 3, 'High',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.03 AND abp.total_transactions >= 15, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 30, 1, 0) +
            if(abp.large_transactions > 0 AND abp.total_transactions <= 3, 1, 0)) >= 2, 'Medium',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.03 AND abp.total_transactions >= 15, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 30, 1, 0) +
            if(abp.large_transactions > 0 AND abp.total_transactions <= 3, 1, 0)) >= 1, 'Low', 'Normal'))),
    if(asset = 'DOT',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.04 AND abp.total_transactions >= 18, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 40, 1, 0) +
            if(abp.whale_transactions > 0 AND abp.total_transactions <= 4, 1, 0)) >= 3, 'High',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.04 AND abp.total_transactions >= 18, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 40, 1, 0) +
            if(abp.whale_transactions > 0 AND abp.total_transactions <= 4, 1, 0)) >= 2, 'Medium',
        if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
            if(abp.sent_amount_variance < abp.avg_sent_amount * 0.04 AND abp.total_transactions >= 18, 1, 0) +
            if(abp.unique_recipients = 1 AND abp.total_transactions >= 40, 1, 0) +
            if(abp.whale_transactions > 0 AND abp.total_transactions <= 4, 1, 0)) >= 1, 'Low', 'Normal'))),
    -- Default case for other assets
    if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0) +
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0) +
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0)) >= 3, 'High',
    if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0) +
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0) +
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0)) >= 2, 'Medium',
    if((if(toFloat64(abp.night_transactions) >= toFloat64(abp.total_transactions) * 0.8, 1, 0) +
        if(abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20, 1, 0) +
        if(abp.unique_recipients = 1 AND abp.total_transactions >= 50, 1, 0) +
        if(abp.whale_transactions > 0 AND abp.total_transactions <= 5, 1, 0)) >= 1, 'Low', 'Normal')))))) as risk_level
    
FROM balance_transfers_address_behavior_profiles_view abp
WHERE abp.total_transactions >= 5  -- Minimum transactions for analysis
ORDER BY suspicion_score DESC, abp.total_volume DESC;