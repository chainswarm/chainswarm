-- =============================================================================
-- ADDRESS ANALYTICS VIEW (refactored with functions)
-- =============================================================================

CREATE VIEW IF NOT EXISTS balance_transfers_address_analytics_view AS
WITH address_base_metrics AS (
    SELECT
        from_address as address,
        asset,
        
        -- Basic transaction counts
        count() as total_transactions,
        count() as outgoing_count,
        sum(amount) as total_sent,
        sum(fee) as total_fees_paid,
        avg(fee) as avg_fee_paid,
        uniq(to_address) as unique_recipients,
        min(block_timestamp) as first_activity_ts,
        max(block_timestamp) as last_activity_ts,
        
        -- Time-based patterns
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 0 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 6) as night_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 6 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 12) as morning_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 12 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 18) as afternoon_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 18 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 24) as evening_transactions,
        
        -- Histogram bins using function
        countIf(getAmountBin(amount) = 'lt_01') as tx_count_lt_01,
        countIf(getAmountBin(amount) = '01_to_1') as tx_count_01_to_1,
        countIf(getAmountBin(amount) = '1_to_10') as tx_count_1_to_10,
        countIf(getAmountBin(amount) = '10_to_100') as tx_count_10_to_100,
        countIf(getAmountBin(amount) = '100_to_1k') as tx_count_100_to_1k,
        countIf(getAmountBin(amount) = '1k_to_10k') as tx_count_1k_to_10k,
        countIf(getAmountBin(amount) = 'gte_10k') as tx_count_gte_10k,
        
        -- Statistical measures
        varPop(amount) as sent_amount_variance,
        avg(amount) as avg_sent_amount,
        uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as active_days
        
    FROM balance_transfers
    GROUP BY from_address, asset
),
address_incoming_metrics AS (
    SELECT
        to_address as address,
        asset,
        count() as incoming_count,
        sum(amount) as total_received,
        uniq(from_address) as unique_senders,
        varPop(amount) as received_amount_variance
    FROM balance_transfers
    GROUP BY to_address, asset
),
address_combined_metrics AS (
    SELECT
        COALESCE(out.address, inc.address) as address,
        COALESCE(out.asset, inc.asset) as asset,
        
        -- Basic metrics
        COALESCE(out.total_transactions, 0) as total_transactions,
        COALESCE(out.outgoing_count, 0) as outgoing_count,
        COALESCE(inc.incoming_count, 0) as incoming_count,
        COALESCE(out.total_sent, 0) as total_sent,
        COALESCE(inc.total_received, 0) as total_received,
        COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) as total_volume,
        COALESCE(out.unique_recipients, 0) as unique_recipients,
        COALESCE(inc.unique_senders, 0) as unique_senders,
        
        -- Time metrics
        COALESCE(out.first_activity_ts, 0) as first_activity,
        COALESCE(out.last_activity_ts, 0) as last_activity,
        COALESCE(out.last_activity_ts, 0) - COALESCE(out.first_activity_ts, 0) as activity_span_seconds,
        
        -- Fees
        COALESCE(out.total_fees_paid, 0) as total_fees_paid,
        COALESCE(out.avg_fee_paid, 0) as avg_fee_paid,
        
        -- Time distribution
        COALESCE(out.night_transactions, 0) as night_transactions,
        COALESCE(out.morning_transactions, 0) as morning_transactions,
        COALESCE(out.afternoon_transactions, 0) as afternoon_transactions,
        COALESCE(out.evening_transactions, 0) as evening_transactions,
        
        -- Histogram bins
        COALESCE(out.tx_count_lt_01, 0) as tx_count_lt_01,
        COALESCE(out.tx_count_01_to_1, 0) as tx_count_01_to_1,
        COALESCE(out.tx_count_1_to_10, 0) as tx_count_1_to_10,
        COALESCE(out.tx_count_10_to_100, 0) as tx_count_10_to_100,
        COALESCE(out.tx_count_100_to_1k, 0) as tx_count_100_to_1k,
        COALESCE(out.tx_count_1k_to_10k, 0) as tx_count_1k_to_10k,
        COALESCE(out.tx_count_gte_10k, 0) as tx_count_gte_10k,
        
        -- Statistical measures
        COALESCE(out.sent_amount_variance, 0) as sent_amount_variance,
        COALESCE(inc.received_amount_variance, 0) as received_amount_variance,
        COALESCE(out.avg_sent_amount, 0) as avg_sent_amount,
        COALESCE(out.active_days, 0) as active_days
        
    FROM address_base_metrics out
    FULL OUTER JOIN address_incoming_metrics inc 
        ON out.address = inc.address AND out.asset = inc.asset
)
SELECT
    address,
    asset,
    
    -- Basic Metrics
    total_transactions,
    outgoing_count,
    incoming_count,
    total_sent,
    total_received,
    total_volume,
    unique_recipients,
    unique_senders,
    first_activity,
    last_activity,
    activity_span_seconds,
    total_fees_paid,
    avg_fee_paid,
    
    -- Time Distribution
    night_transactions,
    morning_transactions,
    afternoon_transactions,
    evening_transactions,
    
    -- Asset-agnostic Histogram Bins
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    
    -- Statistical Measures
    sent_amount_variance,
    received_amount_variance,
    active_days,
    
    -- Risk score using function
    calculateRiskScore(
        total_transactions,
        night_transactions,
        avg_sent_amount,
        sent_amount_variance,
        unique_recipients,
        tx_count_gte_10k
    ) as risk_score,
    
    -- Risk level based on score
    CASE
        WHEN calculateRiskScore(total_transactions, night_transactions, avg_sent_amount, 
                               sent_amount_variance, unique_recipients, tx_count_gte_10k) >= 3 THEN 'High'
        WHEN calculateRiskScore(total_transactions, night_transactions, avg_sent_amount, 
                               sent_amount_variance, unique_recipients, tx_count_gte_10k) >= 2 THEN 'Medium'
        WHEN calculateRiskScore(total_transactions, night_transactions, avg_sent_amount, 
                               sent_amount_variance, unique_recipients, tx_count_gte_10k) >= 1 THEN 'Low'
        ELSE 'Normal'
    END as risk_level,
    
    -- Suspicion indicators (for transparency)
    CASE 
        WHEN total_transactions > 0 AND toFloat64(night_transactions) >= toFloat64(total_transactions) * 0.8 
        THEN 1 ELSE 0 
    END as unusual_time_pattern,
    
    CASE 
        WHEN avg_sent_amount > 0 AND sent_amount_variance < avg_sent_amount * 0.05 AND total_transactions >= 20 
        THEN 1 ELSE 0 
    END as fixed_amount_pattern,
    
    CASE 
        WHEN unique_recipients = 1 AND total_transactions >= 50 
        THEN 1 ELSE 0 
    END as single_recipient_pattern,
    
    CASE 
        WHEN tx_count_gte_10k > 0 AND total_transactions <= 5 
        THEN 1 ELSE 0 
    END as large_infrequent_pattern,
    
    -- Address classification
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
    
    -- Volume classification
    CASE
        WHEN total_volume >= 100000 THEN 'Ultra_High'
        WHEN total_volume >= 10000 THEN 'High'
        WHEN total_volume >= 1000 THEN 'Medium'
        WHEN total_volume >= 100 THEN 'Low'
        ELSE 'Micro'
    END as volume_tier
    
FROM address_combined_metrics
WHERE total_transactions > 0;

-- =============================================================================
-- DAILY PATTERNS VIEW (refactored with histogram function)
-- =============================================================================

CREATE VIEW IF NOT EXISTS balance_transfers_daily_patterns_view AS
SELECT
    from_address,
    to_address,
    asset,
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as activity_date,
    count() as daily_transactions,
    sum(amount) as daily_volume,
    avg(amount) as daily_avg_amount,
    min(amount) as daily_min_amount,
    max(amount) as daily_max_amount,
    uniq(toHour(toDateTime(intDiv(block_timestamp, 1000)))) as active_hours_count,
    
    -- Daily histogram using function
    countIf(getAmountBin(amount) = 'lt_01') as daily_tx_count_lt_01,
    countIf(getAmountBin(amount) = '01_to_1') as daily_tx_count_01_to_1,
    countIf(getAmountBin(amount) = '1_to_10') as daily_tx_count_1_to_10,
    countIf(getAmountBin(amount) = '10_to_100') as daily_tx_count_10_to_100,
    countIf(getAmountBin(amount) = '100_to_1k') as daily_tx_count_100_to_1k,
    countIf(getAmountBin(amount) = '1k_to_10k') as daily_tx_count_1k_to_10k,
    countIf(getAmountBin(amount) = 'gte_10k') as daily_tx_count_gte_10k,
    
    -- Statistical Measures
    median(amount) as daily_median_amount,
    stddevPop(amount) as daily_amount_variance,
    
    -- Daily fee analysis
    sum(fee) as daily_fees,
    avg(fee) as daily_avg_fee,
    
    -- Hourly Distribution Analysis
    groupArray(toHour(toDateTime(intDiv(block_timestamp, 1000)))) as hourly_activity,
    toUInt8(avg(toHour(toDateTime(intDiv(block_timestamp, 1000))))) as peak_hour,
    
    -- Time distribution
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 0 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 6) as night_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 6 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 12) as morning_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 12 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 18) as afternoon_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 18 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 24) as evening_transactions

FROM balance_transfers
WHERE from_address != to_address
GROUP BY from_address, to_address, asset, activity_date
ORDER BY activity_date DESC, daily_volume DESC;

-- =============================================================================
-- VOLUME AGGREGATION VIEWS (using layered architecture)
-- =============================================================================

-- Daily volume view (direct from daily MV)
CREATE VIEW IF NOT EXISTS balance_transfers_volume_daily_view AS
SELECT
    date,
    asset,
    
    -- Basic aggregations
    transaction_count as daily_transaction_count,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    total_volume as daily_total_volume,
    total_fees as daily_total_fees,
    CASE 
        WHEN transaction_count > 0 
        THEN total_volume / transaction_count 
        ELSE 0 
    END as daily_avg_transfer_amount,
    max_amount as daily_max_transfer_amount,
    min_amount as daily_min_transfer_amount,
    
    -- Network metrics
    active_addresses as max_daily_active_addresses,
    avg_network_density as avg_daily_network_density,
    
    -- Statistical aggregations
    avg_std_dev as avg_daily_amount_std_dev,
    median_amount as avg_daily_median_amount,
    
    -- Histogram aggregations
    tx_count_lt_01 as daily_tx_count_lt_01,
    tx_count_01_to_1 as daily_tx_count_01_to_1,
    tx_count_1_to_10 as daily_tx_count_1_to_10,
    tx_count_10_to_100 as daily_tx_count_10_to_100,
    tx_count_100_to_1k as daily_tx_count_100_to_1k,
    tx_count_1k_to_10k as daily_tx_count_1k_to_10k,
    tx_count_gte_10k as daily_tx_count_gte_10k,
    
    volume_lt_01 as daily_volume_lt_01,
    volume_01_to_1 as daily_volume_01_to_1,
    volume_1_to_10 as daily_volume_1_to_10,
    volume_10_to_100 as daily_volume_10_to_100,
    volume_100_to_1k as daily_volume_100_to_1k,
    volume_1k_to_10k as daily_volume_1k_to_10k,
    volume_gte_10k as daily_volume_gte_10k,
    
    -- Block information
    start_block as daily_start_block,
    end_block as daily_end_block
    
FROM balance_transfers_daily_mv
ORDER BY date DESC, asset;

-- Weekly volume view (aggregates from daily MV)
CREATE VIEW IF NOT EXISTS balance_transfers_volume_weekly_view AS
SELECT
    toStartOfWeek(date) as week_start,
    asset,
    
    -- Aggregations from daily
    sum(transaction_count) as weekly_transaction_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    sum(total_volume) as weekly_total_volume,
    sum(total_fees) as weekly_total_fees,
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_volume) / sum(transaction_count) 
        ELSE 0 
    END as weekly_avg_transfer_amount,
    max(max_amount) as weekly_max_transfer_amount,
    
    -- Network metrics
    max(active_addresses) as max_weekly_active_addresses,
    
    -- Histogram aggregations
    sum(tx_count_lt_01) as weekly_tx_count_lt_01,
    sum(tx_count_01_to_1) as weekly_tx_count_01_to_1,
    sum(tx_count_1_to_10) as weekly_tx_count_1_to_10,
    sum(tx_count_10_to_100) as weekly_tx_count_10_to_100,
    sum(tx_count_100_to_1k) as weekly_tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as weekly_tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as weekly_tx_count_gte_10k,
    
    sum(volume_lt_01) as weekly_volume_lt_01,
    sum(volume_01_to_1) as weekly_volume_01_to_1,
    sum(volume_1_to_10) as weekly_volume_1_to_10,
    sum(volume_10_to_100) as weekly_volume_10_to_100,
    sum(volume_100_to_1k) as weekly_volume_100_to_1k,
    sum(volume_1k_to_10k) as weekly_volume_1k_to_10k,
    sum(volume_gte_10k) as weekly_volume_gte_10k,
    
    -- Block information
    min(start_block) as weekly_start_block,
    max(end_block) as weekly_end_block
    
FROM balance_transfers_daily_mv
GROUP BY week_start, asset
ORDER BY week_start DESC, asset;

-- Monthly volume view (aggregates from daily MV)
CREATE VIEW IF NOT EXISTS balance_transfers_volume_monthly_view AS
SELECT
    toStartOfMonth(date) as month_start,
    asset,
    
    -- Aggregations from daily
    sum(transaction_count) as monthly_transaction_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    sum(total_volume) as monthly_total_volume,
    sum(total_fees) as monthly_total_fees,
    CASE 
        WHEN sum(transaction_count) > 0 
        THEN sum(total_volume) / sum(transaction_count) 
        ELSE 0 
    END as monthly_avg_transfer_amount,
    max(max_amount) as monthly_max_transfer_amount,
    
    -- Network metrics
    max(active_addresses) as max_monthly_active_addresses,
    
    -- Histogram aggregations
    sum(tx_count_lt_01) as monthly_tx_count_lt_01,
    sum(tx_count_01_to_1) as monthly_tx_count_01_to_1,
    sum(tx_count_1_to_10) as monthly_tx_count_1_to_10,
    sum(tx_count_10_to_100) as monthly_tx_count_10_to_100,
    sum(tx_count_100_to_1k) as monthly_tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as monthly_tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as monthly_tx_count_gte_10k,
    
    sum(volume_lt_01) as monthly_volume_lt_01,
    sum(volume_01_to_1) as monthly_volume_01_to_1,
    sum(volume_1_to_10) as monthly_volume_1_to_10,
    sum(volume_10_to_100) as monthly_volume_10_to_100,
    sum(volume_100_to_1k) as monthly_volume_100_to_1k,
    sum(volume_1k_to_10k) as monthly_volume_1k_to_10k,
    sum(volume_gte_10k) as monthly_volume_gte_10k,
    
    -- Block information
    min(start_block) as monthly_start_block,
    max(end_block) as monthly_end_block
    
FROM balance_transfers_daily_mv
GROUP BY month_start, asset
ORDER BY month_start DESC, asset;

-- =============================================================================
-- ANALYSIS VIEWS
-- =============================================================================

-- Volume trends view with rolling averages
CREATE VIEW IF NOT EXISTS balance_transfers_volume_trends_view AS
SELECT
    period_start,
    asset,
    total_volume,
    transaction_count,
    
    -- Rolling averages (7 periods = ~28 hours)
    avg(total_volume) OVER (
        PARTITION BY asset
        ORDER BY period_start
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7_period_avg_volume,
    
    avg(transaction_count) OVER (
        PARTITION BY asset
        ORDER BY period_start
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7_period_avg_tx_count,
    
    -- Rolling averages (30 periods = ~5 days)
    avg(total_volume) OVER (
        PARTITION BY asset
        ORDER BY period_start
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_30_period_avg_volume,
    
    -- Trend indicators
    CASE
        WHEN total_volume > avg(total_volume) OVER (
            PARTITION BY asset
            ORDER BY period_start
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) * 1.5 THEN 'Spike'
        WHEN total_volume < avg(total_volume) OVER (
            PARTITION BY asset
            ORDER BY period_start
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) * 0.5 THEN 'Drop'
        ELSE 'Normal'
    END as volume_trend
    
FROM balance_transfers_volume_series_mv
ORDER BY period_start DESC, asset;

-- Volume quantiles view for distribution analysis
CREATE VIEW IF NOT EXISTS balance_transfers_volume_quantiles_view AS
SELECT
    toDate(period_start) as date,
    asset,
    
    -- Volume quantiles across 4-hour periods for the day
    quantile(0.10)(total_volume) as q10_volume,
    quantile(0.25)(total_volume) as q25_volume,
    quantile(0.50)(total_volume) as median_volume,
    quantile(0.75)(total_volume) as q75_volume,
    quantile(0.90)(total_volume) as q90_volume,
    quantile(0.99)(total_volume) as q99_volume,
    
    -- Transaction count quantiles
    quantile(0.10)(transaction_count) as q10_tx_count,
    quantile(0.25)(transaction_count) as q25_tx_count,
    quantile(0.50)(transaction_count) as median_tx_count,
    quantile(0.75)(transaction_count) as q75_tx_count,
    quantile(0.90)(transaction_count) as q90_tx_count,
    quantile(0.99)(transaction_count) as q99_tx_count,
    
    -- Distribution metrics
    max(total_volume) - min(total_volume) as volume_range,
    stddevPop(total_volume) as volume_std_dev,
    varPop(total_volume) as volume_variance,
    
    -- Outlier detection
    count() as periods_in_day,
    countIf(total_volume > quantile(0.95)(total_volume)) as high_volume_periods,
    countIf(total_volume < quantile(0.05)(total_volume)) as low_volume_periods
    
FROM balance_transfers_volume_series_mv
GROUP BY date, asset
ORDER BY date DESC, asset;

-- =============================================================================
-- MIGRATION HELPERS
-- =============================================================================

-- View to compare old vs new aggregation methods for validation
CREATE VIEW IF NOT EXISTS balance_transfers_migration_validation AS
WITH old_weekly AS (
    -- Simulated old method (direct from base table)
    SELECT
        toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as week,
        asset,
        count() as tx_count_old,
        sum(amount) as volume_old
    FROM balance_transfers
    WHERE block_timestamp >= toUnixTimestamp(now() - INTERVAL 1 WEEK) * 1000
    GROUP BY week, asset
),
new_weekly AS (
    -- New method (from daily MV)
    SELECT
        toStartOfWeek(date) as week,
        asset,
        sum(transaction_count) as tx_count_new,
        sum(total_volume) as volume_new
    FROM balance_transfers_daily_mv
    WHERE date >= now() - INTERVAL 1 WEEK
    GROUP BY week, asset
)
SELECT
    COALESCE(old.week, new.week) as week,
    COALESCE(old.asset, new.asset) as asset,
    old.tx_count_old,
    new.tx_count_new,
    old.volume_old,
    new.volume_new,
    abs(old.tx_count_old - new.tx_count_new) as tx_count_diff,
    abs(old.volume_old - new.volume_new) as volume_diff,
    CASE
        WHEN old.tx_count_old > 0 
        THEN abs(old.tx_count_old - new.tx_count_new) / old.tx_count_old * 100
        ELSE 0
    END as tx_count_diff_pct,
    CASE
        WHEN old.volume_old > 0 
        THEN abs(old.volume_old - new.volume_new) / old.volume_old * 100
        ELSE 0
    END as volume_diff_pct
FROM old_weekly old
FULL OUTER JOIN new_weekly new 
    ON old.week = new.week AND old.asset = new.asset;

-- =============================================================================
-- PERFORMANCE MONITORING VIEW
-- =============================================================================

CREATE VIEW IF NOT EXISTS balance_transfers_performance_metrics AS
SELECT
    'balance_transfers' as table_name,
    count() as total_rows,
    sum(rows) as total_parts,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) as compression_ratio
FROM system.parts
WHERE database = currentDatabase() 
    AND table = 'balance_transfers'
    AND active

UNION ALL

SELECT
    'balance_transfers_volume_series_mv' as table_name,
    count() as total_rows,
    sum(rows) as total_parts,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) as compression_ratio
FROM system.parts
WHERE database = currentDatabase() 
    AND table = 'balance_transfers_volume_series_mv'
    AND active

UNION ALL

SELECT
    'balance_transfers_daily_mv' as table_name,
    count() as total_rows,
    sum(rows) as total_parts,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) as compression_ratio
FROM system.parts
WHERE database = currentDatabase() 
    AND table = 'balance_transfers_daily_mv'
    AND active;