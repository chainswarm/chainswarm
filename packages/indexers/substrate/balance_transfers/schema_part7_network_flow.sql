-- Balance Transfers Schema - Part 7: Network Flow Analysis
-- Analyze transaction flows and network-level metrics

-- =============================================================================
-- NETWORK FLOW ANALYSIS VIEWS
-- =============================================================================

-- Transaction Flow Aggregation View
-- Provides a high-level overview of network activity by day and asset
CREATE VIEW IF NOT EXISTS balance_transfers_network_flow_view AS
SELECT
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as day,
    asset,
    count() as transaction_count,
    sum(amount) as total_volume,
    count(DISTINCT from_address) as unique_senders,
    count(DISTINCT to_address) as unique_receivers,
    -- Total unique addresses (union of senders and receivers)
    uniqExact(from_address) + uniqExact(to_address) - uniqExactIf(from_address, from_address = to_address) as unique_addresses,
    
    -- Transaction size distribution
    countIf(
        if(asset = 'TOR', amount < 100,
        if(asset = 'TAO', amount < 0.5,
        if(asset = 'DOT', amount < 25,
           amount < 100)))
    ) as micro_transactions,
    
    countIf(
        if(asset = 'TOR', amount >= 100 AND amount < 1000,
        if(asset = 'TAO', amount >= 0.5 AND amount < 3,
        if(asset = 'DOT', amount >= 25 AND amount < 250,
           amount >= 100 AND amount < 1000)))
    ) as small_transactions,
    
    countIf(
        if(asset = 'TOR', amount >= 1000 AND amount < 10000,
        if(asset = 'TAO', amount >= 3 AND amount < 30,
        if(asset = 'DOT', amount >= 250 AND amount < 2500,
           amount >= 1000 AND amount < 10000)))
    ) as medium_transactions,
    
    countIf(
        if(asset = 'TOR', amount >= 10000 AND amount < 100000,
        if(asset = 'TAO', amount >= 30 AND amount < 300,
        if(asset = 'DOT', amount >= 2500 AND amount < 25000,
           amount >= 10000 AND amount < 100000)))
    ) as large_transactions,
    
    countIf(
        if(asset = 'TOR', amount >= 100000,
        if(asset = 'TAO', amount >= 300,
        if(asset = 'DOT', amount >= 25000,
           amount >= 100000)))
    ) as whale_transactions,
    
    -- Network density (ratio of actual connections to possible connections)
    uniqExact(from_address, to_address) / (uniqExact(from_address) * uniqExact(to_address)) as network_density,
    
    -- Average transaction size
    avg(amount) as avg_transaction_size,
    
    -- Fee metrics
    sum(fee) as total_fees,
    avg(fee) as avg_fee
    
FROM balance_transfers
GROUP BY day, asset
ORDER BY day DESC, asset;