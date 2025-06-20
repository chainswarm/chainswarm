-- Balance Transfers Schema - Part 3: Behavior Profiles View
-- Comprehensive behavioral analysis for each address with asset-specific thresholds

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
    
    -- Transaction Size Distribution with Asset-Specific USD-Equivalent Thresholds
    if(asset = 'TOR', countIf(amount < 100),
    if(asset = 'TAO', countIf(amount < 0.5),
    if(asset = 'DOT', countIf(amount < 25),
       countIf(amount < 10)))) as micro_transactions,
    
    if(asset = 'TOR', countIf(amount >= 100 AND amount < 1000),
    if(asset = 'TAO', countIf(amount >= 0.5 AND amount < 3),
    if(asset = 'DOT', countIf(amount >= 25 AND amount < 250),
       countIf(amount >= 10 AND amount < 100)))) as small_transactions,
    
    if(asset = 'TOR', countIf(amount >= 1000 AND amount < 10000),
    if(asset = 'TAO', countIf(amount >= 3 AND amount < 30),
    if(asset = 'DOT', countIf(amount >= 250 AND amount < 2500),
       countIf(amount >= 100 AND amount < 1000)))) as medium_transactions,
    
    if(asset = 'TOR', countIf(amount >= 10000 AND amount < 100000),
    if(asset = 'TAO', countIf(amount >= 30 AND amount < 300),
    if(asset = 'DOT', countIf(amount >= 2500 AND amount < 25000),
       countIf(amount >= 1000 AND amount < 10000)))) as large_transactions,
    
    if(asset = 'TOR', countIf(amount >= 100000),
    if(asset = 'TAO', countIf(amount >= 300),
    if(asset = 'DOT', countIf(amount >= 25000),
       countIf(amount >= 10000)))) as whale_transactions,
    
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