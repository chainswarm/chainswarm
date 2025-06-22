-- Balance Transfers Schema - Part 4: Address Classification View
-- Classify addresses into behavioral categories with asset-agnostic thresholds

-- =============================================================================
-- ADDRESS CLASSIFICATION VIEW
-- =============================================================================

-- Address Classification View
-- Classify addresses into behavioral categories with asset-agnostic thresholds
CREATE VIEW IF NOT EXISTS balance_transfers_address_classification_view AS
SELECT
    address,
    asset,
    total_volume,
    total_transactions,
    unique_recipients,
    unique_senders,
    
    -- Asset-agnostic classification logic based on behavioral patterns
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
    
    -- Asset-agnostic volume classification
    CASE
        WHEN total_volume >= 100000 THEN 'Ultra_High'
        WHEN total_volume >= 10000 THEN 'High'
        WHEN total_volume >= 1000 THEN 'Medium'
        WHEN total_volume >= 100 THEN 'Low'
        ELSE 'Micro'
    END as volume_tier
    
FROM balance_transfers_address_behavior_profiles_view;