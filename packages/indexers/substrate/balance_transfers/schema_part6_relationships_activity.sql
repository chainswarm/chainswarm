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
    
    -- Asset-agnostic relationship type classification
    CASE
        WHEN total_amount >= 10000 AND transfer_count >= 10 THEN 'High_Value'
        WHEN transfer_count >= 100 THEN 'High_Frequency'
        WHEN transfer_count >= 5 THEN 'Regular'
        ELSE 'Casual'
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
    
    -- Asset-agnostic transaction size histogram
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    
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