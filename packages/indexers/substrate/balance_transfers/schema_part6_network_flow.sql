-- Balance Transfers Schema - Part 7: Network Flow Analysis
-- Analyze transaction flows and network-level metrics

-- =============================================================================
-- NETWORK FLOW ANALYSIS VIEWS
-- =============================================================================

-- Transaction Flow Aggregation View (Optimized)
-- Provides a high-level overview of network activity by day and asset
-- Uses pre-aggregated timeseries materialized view for optimal performance
CREATE VIEW IF NOT EXISTS balance_transfers_network_flow_view AS
WITH daily_aggregates AS (
    SELECT
        toDate(period_start) as day,
        asset,
        
        -- Basic volume metrics (aggregated from 4-hour periods)
        sum(transaction_count) as transaction_count,
        sum(total_volume) as total_volume,
        max(unique_senders) as unique_senders,
        max(unique_receivers) as unique_receivers,
        max(active_addresses) as unique_addresses,
        
        -- Asset-agnostic transaction size histogram (aggregated from 4-hour periods)
        sum(tx_count_lt_01) as tx_count_lt_01,
        sum(tx_count_01_to_1) as tx_count_01_to_1,
        sum(tx_count_1_to_10) as tx_count_1_to_10,
        sum(tx_count_10_to_100) as tx_count_10_to_100,
        sum(tx_count_100_to_1k) as tx_count_100_to_1k,
        sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
        sum(tx_count_gte_10k) as tx_count_gte_10k,
        
        -- Network density (average across 4-hour periods)
        avg(network_density) as network_density,
        
        -- Fee metrics (aggregated from 4-hour periods)
        sum(total_fees) as total_fees,
        max(max_fee) as max_fee,
        min(min_fee) as min_fee,
        
        -- Statistical measures (averaged across 4-hour periods)
        avg(median_transfer_amount) as median_transaction_size,
        avg(amount_std_dev) as avg_amount_std_dev,
        
        -- Min/max for transaction sizes
        max(max_transfer_amount) as max_transaction_size,
        min(min_transfer_amount) as min_transaction_size,
        
        -- Block information
        min(period_start_block) as day_start_block,
        max(period_end_block) as day_end_block
        
    FROM balance_transfers_volume_series_mv
    GROUP BY toDate(period_start), asset
)
SELECT
    day,
    asset,
    transaction_count,
    total_volume,
    unique_senders,
    unique_receivers,
    unique_addresses,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    network_density,
    
    -- Derived metrics (calculated from aggregated values)
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_size,
    max_transaction_size,
    min_transaction_size,
    
    total_fees,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    max_fee,
    min_fee,
    
    median_transaction_size,
    avg_amount_std_dev,
    
    day_start_block,
    day_end_block,
    day_end_block - day_start_block + 1 as blocks_in_day
    
FROM daily_aggregates
ORDER BY day DESC, asset;