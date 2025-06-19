-- Balance Transfers Schema - Part 4: Address Classification View
-- Classify addresses into behavioral categories with asset-specific thresholds

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