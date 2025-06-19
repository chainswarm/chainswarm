-- Balance Transfers Schema - Part 6: Relationship Analysis and Activity Patterns Views
-- Track relationships between addresses and analyze temporal patterns

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
    if(asset = 'TOR',
        if(total_amount >= 10000 AND transfer_count >= 10, 'High_Value',
        if(transfer_count >= 100, 'High_Frequency',
        if(transfer_count >= 5, 'Regular', 'Casual'))),
    if(asset = 'TAO',
        if(total_amount >= 30 AND transfer_count >= 10, 'High_Value',
        if(transfer_count >= 100, 'High_Frequency',
        if(transfer_count >= 5, 'Regular', 'Casual'))),
    if(asset = 'DOT',
        if(total_amount >= 2500 AND transfer_count >= 10, 'High_Value',
        if(transfer_count >= 100, 'High_Frequency',
        if(transfer_count >= 5, 'Regular', 'Casual'))),
        if(total_amount >= 10000 AND transfer_count >= 10, 'High_Value',
        if(transfer_count >= 100, 'High_Frequency',
        if(transfer_count >= 5, 'Regular', 'Casual')))))) as relationship_type
    
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
    if(asset = 'TOR', countIf(amount < 100),
    if(asset = 'TAO', countIf(amount < 0.5),
    if(asset = 'DOT', countIf(amount < 25),
       countIf(amount < 100)))) as micro_transactions,
    
    if(asset = 'TOR', countIf(amount >= 100 AND amount < 1000),
    if(asset = 'TAO', countIf(amount >= 0.5 AND amount < 3),
    if(asset = 'DOT', countIf(amount >= 25 AND amount < 250),
       countIf(amount >= 100 AND amount < 1000)))) as small_transactions,
    
    if(asset = 'TOR', countIf(amount >= 1000 AND amount < 10000),
    if(asset = 'TAO', countIf(amount >= 3 AND amount < 30),
    if(asset = 'DOT', countIf(amount >= 250 AND amount < 2500),
       countIf(amount >= 1000 AND amount < 10000)))) as medium_transactions,
    
    if(asset = 'TOR', countIf(amount >= 10000 AND amount < 100000),
    if(asset = 'TAO', countIf(amount >= 30 AND amount < 300),
    if(asset = 'DOT', countIf(amount >= 2500 AND amount < 25000),
       countIf(amount >= 10000 AND amount < 100000)))) as large_transactions,
    
    if(asset = 'TOR', countIf(amount >= 100000),
    if(asset = 'TAO', countIf(amount >= 300),
    if(asset = 'DOT', countIf(amount >= 25000),
       countIf(amount >= 100000)))) as whale_transactions,
    
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