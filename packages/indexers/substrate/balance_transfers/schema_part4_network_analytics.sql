-- =====================================================================================
-- Network Analytics View - Consolidated Network and Temporal Analysis
-- =====================================================================================
-- Replaces: Enhanced version of schema_part7_network_flow.sql and 
--           schema_part8_temporal_analysis.sql
-- 
-- This view provides comprehensive network analysis including:
-- - Network flow metrics using optimized materialized view
-- - Temporal patterns (daily, weekly, hourly)
-- - Universal histogram bins for all assets
-- 
-- Simplified for ClickHouse 24.11.1 compatibility
-- Leverages balance_transfers_volume_series_mv for 10-100x performance improvement
-- =====================================================================================

-- Daily Network Analytics View
CREATE VIEW balance_transfers_network_daily_view AS
SELECT
    'daily' as period_type,
    toDate(period_start) as period,
    asset,
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    max(active_addresses) as unique_addresses,
    sum(tx_count_lt_01) as tx_count_lt_01,
    sum(tx_count_01_to_1) as tx_count_01_to_1,
    sum(tx_count_1_to_10) as tx_count_1_to_10,
    sum(tx_count_10_to_100) as tx_count_10_to_100,
    sum(tx_count_100_to_1k) as tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as tx_count_gte_10k,
    avg(network_density) as avg_network_density,
    sum(total_fees) as total_fees
FROM balance_transfers_volume_series_mv
GROUP BY toDate(period_start), asset
ORDER BY period DESC, asset;

-- Weekly Network Analytics View
CREATE VIEW balance_transfers_network_weekly_view AS
SELECT
    'weekly' as period_type,
    toStartOfWeek(period_start) as period,
    asset,
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    max(active_addresses) as unique_addresses,
    sum(tx_count_lt_01) as tx_count_lt_01,
    sum(tx_count_01_to_1) as tx_count_01_to_1,
    sum(tx_count_1_to_10) as tx_count_1_to_10,
    sum(tx_count_10_to_100) as tx_count_10_to_100,
    sum(tx_count_100_to_1k) as tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as tx_count_gte_10k,
    avg(network_density) as avg_network_density,
    sum(total_fees) as total_fees
FROM balance_transfers_volume_series_mv
GROUP BY toStartOfWeek(period_start), asset
ORDER BY period DESC, asset;

-- Monthly Network Analytics View
CREATE VIEW balance_transfers_network_monthly_view AS
SELECT
    'monthly' as period_type,
    toStartOfMonth(period_start) as period,
    asset,
    sum(transaction_count) as transaction_count,
    sum(total_volume) as total_volume,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    max(active_addresses) as unique_addresses,
    sum(tx_count_lt_01) as tx_count_lt_01,
    sum(tx_count_01_to_1) as tx_count_01_to_1,
    sum(tx_count_1_to_10) as tx_count_1_to_10,
    sum(tx_count_10_to_100) as tx_count_10_to_100,
    sum(tx_count_100_to_1k) as tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as tx_count_gte_10k,
    avg(network_density) as avg_network_density,
    sum(total_fees) as total_fees
FROM balance_transfers_volume_series_mv
GROUP BY toStartOfMonth(period_start), asset
ORDER BY period DESC, asset;

-- Combined Network Analytics View (Union of all periods)
CREATE VIEW balance_transfers_network_analytics_view AS
SELECT * FROM balance_transfers_network_daily_view
UNION ALL
SELECT * FROM balance_transfers_network_weekly_view
UNION ALL
SELECT * FROM balance_transfers_network_monthly_view
ORDER BY period_type, period DESC, asset;

-- Enhanced view with computed fields
CREATE VIEW balance_transfers_network_enhanced_view AS
SELECT
    period_type,
    period,
    asset,
    transaction_count,
    total_volume,
    max_unique_senders,
    max_unique_receivers,
    unique_addresses,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    avg_network_density,
    total_fees,
    
    -- Computed fields (safe calculations)
    CASE
        WHEN transaction_count > 0 THEN total_volume / transaction_count
        ELSE 0
    END as avg_transaction_size

FROM balance_transfers_network_analytics_view
ORDER BY period_type, period DESC, asset;