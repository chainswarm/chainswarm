-- Balance Transfers Schema - Part 8: Temporal Analysis
-- Analyze transaction patterns over time periods and identify seasonality

-- =============================================================================
-- TEMPORAL ANALYSIS VIEWS
-- =============================================================================

-- Periodic Activity View
-- Analyze activity patterns over weekly time periods for each address
CREATE VIEW IF NOT EXISTS balance_transfers_periodic_activity_view AS
WITH weekly_data AS (
    SELECT
        from_address as address,
        from_address,
        to_address,
        asset,
        amount,
        block_timestamp,
        toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as week_start,
        uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) OVER (PARTITION BY from_address) as active_days
    FROM balance_transfers
    
    UNION ALL
    
    SELECT
        to_address as address,
        from_address,
        to_address,
        asset,
        amount,
        block_timestamp,
        toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as week_start,
        uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) OVER (PARTITION BY to_address) as active_days
    FROM balance_transfers
),
weekly_metrics AS (
    SELECT
        address,
        asset,
        week_start,
        count() as weekly_transactions,
        countIf(address = from_address) as outgoing_transactions,
        countIf(address = to_address) as incoming_transactions,
        sum(amount) as weekly_volume,
        sumIf(amount, address = from_address) as outgoing_volume,
        sumIf(amount, address = to_address) as incoming_volume,
        avg(amount) as avg_transaction_size,
        median(amount) as median_transaction_size,
        max(amount) as max_transaction_size,
        uniq(if(address = from_address, to_address, from_address)) as unique_counterparties,
        uniqIf(to_address, address = from_address) as unique_recipients,
        uniqIf(from_address, address = to_address) as unique_senders,
        count() / max(active_days) as transactions_per_active_day
    FROM weekly_data
    GROUP BY address, asset, week_start
)
SELECT
    address,
    asset,
    week_start,
    
    -- Transaction counts
    weekly_transactions,
    outgoing_transactions,
    incoming_transactions,
    
    -- Volume metrics
    weekly_volume,
    outgoing_volume,
    incoming_volume,
    
    -- Transaction size metrics
    avg_transaction_size,
    median_transaction_size,
    max_transaction_size,
    
    -- Counterparty analysis
    unique_counterparties,
    unique_recipients,
    unique_senders,
    
    -- Activity consistency metrics
    transactions_per_active_day,
    
    -- Week-over-week change (using ClickHouse-compatible functions)
    weekly_volume - any(weekly_volume) OVER (
        PARTITION BY address, asset ORDER BY week_start
        ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
    ) as volume_change_from_previous_week,
    
    weekly_transactions - any(weekly_transactions) OVER (
        PARTITION BY address, asset ORDER BY week_start
        ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
    ) as transaction_count_change_from_previous_week
    
FROM weekly_metrics
ORDER BY week_start DESC, weekly_volume DESC;

-- Seasonality Analysis View
-- Detect temporal patterns in transaction activity
CREATE VIEW IF NOT EXISTS balance_transfers_seasonality_view AS
WITH time_metrics AS (
    SELECT
        asset,
        block_timestamp,
        toHour(toDateTime(intDiv(block_timestamp, 1000))) as hour_of_day,
        toDayOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as day_of_week,
        toMonth(toDateTime(intDiv(block_timestamp, 1000))) as month,
        toQuarter(toDateTime(intDiv(block_timestamp, 1000))) as quarter,
        toYear(toDateTime(intDiv(block_timestamp, 1000))) as year,
        toDate(toDateTime(intDiv(block_timestamp, 1000))) as date,
        toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as week_start,
        toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000))) as month_start,
        amount,
        from_address,
        to_address
    FROM balance_transfers
)
SELECT
    asset,
    hour_of_day,
    day_of_week,
    month,
    quarter,
    year,
    
    -- Transaction metrics
    count() as transaction_count,
    sum(amount) as total_volume,
    avg(amount) as avg_transaction_size,
    
    -- Normalized metrics (percentage of daily/weekly/monthly totals)
    count() / sum(count()) OVER (PARTITION BY asset, date) as pct_of_daily_transactions,
    sum(amount) / sum(sum(amount)) OVER (PARTITION BY asset, date) as pct_of_daily_volume,
    
    count() / sum(count()) OVER (PARTITION BY asset, week_start) as pct_of_weekly_transactions,
    sum(amount) / sum(sum(amount)) OVER (PARTITION BY asset, week_start) as pct_of_weekly_volume,
    
    count() / sum(count()) OVER (PARTITION BY asset, month_start) as pct_of_monthly_transactions,
    sum(amount) / sum(sum(amount)) OVER (PARTITION BY asset, month_start) as pct_of_monthly_volume,
    
    -- Unique addresses
    uniq(from_address) as unique_senders,
    uniq(to_address) as unique_receivers,
    uniq(from_address, to_address) as unique_address_pairs
    
FROM time_metrics
GROUP BY asset, hour_of_day, day_of_week, month, quarter, year, date, week_start, month_start
ORDER BY
    asset,
    year DESC,
    quarter DESC,
    month DESC,
    day_of_week,
    hour_of_day;