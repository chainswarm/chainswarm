# Balance Transfers Indexer

## Overview

The Balance Transfers indexer tracks individual transfer transactions between addresses on Substrate networks. It consumes data from the Block Stream indexer and provides a comprehensive solution for analyzing transaction patterns, network activity, and address behaviors with the following key features:

- **Asset-agnostic design** with universal histogram bins for consistent analysis across different assets
- **Multi-level time aggregation** (4-hour, daily, weekly, monthly) for temporal analysis
- **Optimized materialized views** for efficient querying and analysis
- **Address behavior profiling** for identifying different types of network participants

## Core Table Structure

The foundation of the indexer is the `balance_transfers` table:

```sql
CREATE TABLE IF NOT EXISTS balance_transfers (
    -- Transaction identification
    extrinsic_id String,
    event_idx String,
    
    -- Block information
    block_height UInt32,
    block_timestamp UInt64,
    
    -- Transfer details
    from_address String,
    to_address String,
    asset String,
    amount Decimal128(18),
    fee Decimal128(18),
    
    -- Versioning for updates
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
```

### Key Fields

- **extrinsic_id, event_idx**: Uniquely identify a transaction
- **block_height, block_timestamp**: Blockchain location and time information
- **from_address, to_address**: Transaction participants
- **asset**: Token or currency being transferred
- **amount**: Value transferred
- **fee**: Transaction cost
- **_version**: Used for update management with ReplacingMergeTree engine

## Indexes

The schema includes multiple indexes for optimizing different query patterns:

### Address-Based Indexes
- `idx_from_address`, `idx_to_address`: Bloom filters for efficient address lookups
- `idx_address_timestamp`, `idx_to_address_timestamp`: Composite indexes for time-based address activity

### Asset-Based Indexes
- `idx_asset`: Bloom filter for asset lookups
- `idx_asset_from_address`, `idx_asset_to_address`: Composite indexes for asset-address queries

### Time and Amount Indexes
- `idx_block_height`: Range queries for block heights
- `idx_amount_range`: Range queries for transaction amounts
- `idx_hour_of_day`, `idx_date`: Time-based analysis
- `idx_asset_amount_timestamp`: Composite index for time-series analysis

### Relationship Analysis
- `idx_from_to_asset`: Analyzing connections between addresses

## Views and Materialized Views

### Base Materialized View

The `balance_transfers_volume_series_mv` serves as the foundation for time-series analysis, aggregating data into 4-hour intervals:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_volume_series_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(period_start)
ORDER BY (period_start, asset)
AS
SELECT
    -- Time period information (UTC-based 4-hour intervals aligned to midnight)
    toStartOfDay(toDateTime(intDiv(block_timestamp, 1000))) +
    INTERVAL (intDiv(toHour(toDateTime(intDiv(block_timestamp, 1000))), 4) * 4) HOUR as period_start,
    toStartOfDay(toDateTime(intDiv(block_timestamp, 1000))) +
    INTERVAL ((intDiv(toHour(toDateTime(intDiv(block_timestamp, 1000))), 4) + 1) * 4) HOUR as period_end,
    
    -- Asset information
    asset,
    
    -- Basic volume metrics
    count() as transaction_count,
    uniqExact(from_address) as unique_senders,
    uniqExact(to_address) as unique_receivers,
    sum(amount) as total_volume,
    sum(fee) as total_fees,
    avg(amount) as avg_transfer_amount,
    max(amount) as max_transfer_amount,
    min(amount) as min_transfer_amount,
    median(amount) as median_transfer_amount,
    
    -- Amount distribution quantiles
    quantile(0.10)(amount) as amount_p10,
    quantile(0.25)(amount) as amount_p25,
    quantile(0.50)(amount) as amount_p50,
    quantile(0.75)(amount) as amount_p75,
    quantile(0.90)(amount) as amount_p90,
    quantile(0.95)(amount) as amount_p95,
    quantile(0.99)(amount) as amount_p99,
    
    -- Network activity metrics
    uniq(from_address, to_address) as unique_address_pairs,
    uniqExact(from_address) + uniqExact(to_address) as active_addresses,
    
    -- Network density calculation
    CASE
        WHEN (uniqExact(from_address) + uniqExact(to_address)) > 1 THEN
            toFloat64(uniq(from_address, to_address)) /
            (toFloat64(uniqExact(from_address) + uniqExact(to_address)) *
             toFloat64(uniqExact(from_address) + uniqExact(to_address) - 1) / 2.0)
        ELSE 0.0
    END as network_density,
    
    -- Temporal activity patterns
    toHour(period_start) as period_hour,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(period_start)) as hour_0_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(period_start) + 1) as hour_1_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(period_start) + 2) as hour_2_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(period_start) + 3) as hour_3_tx_count,
    
    -- Statistical measures
    stddevPop(amount) as amount_std_dev,
    varPop(amount) as amount_variance,
    skewPop(amount) as amount_skewness,
    kurtPop(amount) as amount_kurtosis,
    
    -- Fee statistics
    avg(fee) as avg_fee,
    max(fee) as max_fee,
    min(fee) as min_fee,
    stddevPop(fee) as fee_std_dev,
    median(fee) as median_fee,
    
    -- Block information
    min(block_height) as period_start_block,
    max(block_height) as period_end_block,
    max(block_height) - min(block_height) + 1 as blocks_in_period,
    
    -- Histogram bins for transaction categorization
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    
    -- Volume histogram bins
    sumIf(amount, amount < 0.1) as volume_lt_01,
    sumIf(amount, amount >= 0.1 AND amount < 1) as volume_01_to_1,
    sumIf(amount, amount >= 1 AND amount < 10) as volume_1_to_10,
    sumIf(amount, amount >= 10 AND amount < 100) as volume_10_to_100,
    sumIf(amount, amount >= 100 AND amount < 1000) as volume_100_to_1k,
    sumIf(amount, amount >= 1000 AND amount < 10000) as volume_1k_to_10k,
    sumIf(amount, amount >= 10000) as volume_gte_10k
FROM balance_transfers
GROUP BY period_start, period_end, asset
```

This materialized view serves as the foundation for all time-series analysis in the balance transfers schema. It:
- Aggregates transaction data into 4-hour intervals aligned to midnight UTC
- Calculates comprehensive metrics for each interval and asset
- Provides the base data for all other time-based views

### Network Analytics Views

Three views provide network analytics at different time scales:

#### Daily Network Analytics View

```sql
CREATE VIEW IF NOT EXISTS balance_transfers_network_daily_view AS
WITH daily_aggregates AS (
    SELECT
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
        sum(total_fees) as total_fees,
        avg(median_transfer_amount) as median_transaction_size,
        avg(amount_std_dev) as avg_amount_std_dev,
        max(max_transfer_amount) as max_transaction_size,
        min(min_transfer_amount) as min_transaction_size,
        min(period_start_block) as period_start_block,
        max(period_end_block) as period_end_block
    FROM balance_transfers_volume_series_mv
    GROUP BY toDate(period_start), asset
)
SELECT
    'daily' as period_type,
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
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_size,
    max_transaction_size,
    min_transaction_size,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    max_fee,
    min_fee,
    median_transaction_size,
    avg_amount_std_dev
FROM daily_aggregates
ORDER BY period DESC, asset
```

#### Weekly Network Analytics View

```sql
CREATE VIEW IF NOT EXISTS balance_transfers_network_weekly_view AS
WITH weekly_base AS (
    SELECT
        toStartOfWeek(period_start) as period,
        asset,
        sum(transaction_count) as transaction_count,
        max(unique_senders) as unique_senders,
        max(unique_receivers) as unique_receivers,
        max(active_addresses) as active_addresses,
        sum(total_volume) as total_volume,
        sum(total_fees) as total_fees,
        avg(avg_transfer_amount) as avg_transfer_amount,
        max(max_transfer_amount) as max_transfer_amount,
        min(min_transfer_amount) as min_transfer_amount,
        avg(median_transfer_amount) as median_transfer_amount,
        avg(amount_std_dev) as amount_std_dev,
        max(max_fee) as max_fee,
        min(min_fee) as min_fee,
        avg(avg_fee) as avg_fee,
        min(period_start_block) as period_start_block,
        max(period_end_block) as period_end_block,
        avg(network_density) as network_density,
        sum(tx_count_lt_01) as tx_count_lt_01,
        sum(tx_count_01_to_1) as tx_count_01_to_1,
        sum(tx_count_1_to_10) as tx_count_1_to_10,
        sum(tx_count_10_to_100) as tx_count_10_to_100,
        sum(tx_count_100_to_1k) as tx_count_100_to_1k,
        sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
        sum(tx_count_gte_10k) as tx_count_gte_10k
    FROM balance_transfers_volume_series_mv
    GROUP BY period, asset
)
SELECT
    'weekly' as period_type,
    period,
    asset,
    transaction_count,
    total_volume,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    active_addresses as unique_addresses,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    network_density as avg_network_density,
    total_fees,
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_size,
    max_transfer_amount as max_transaction_size,
    min_transfer_amount as min_transaction_size,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    max_fee,
    min_fee,
    median_transfer_amount as median_transaction_size,
    amount_std_dev as avg_amount_std_dev
FROM weekly_base
ORDER BY period DESC, asset
```

#### Monthly Network Analytics View

```sql
CREATE VIEW IF NOT EXISTS balance_transfers_network_monthly_view AS
WITH monthly_base AS (
    SELECT
        toStartOfMonth(period_start) as period,
        asset,
        sum(transaction_count) as transaction_count,
        max(unique_senders) as unique_senders,
        max(unique_receivers) as unique_receivers,
        max(active_addresses) as active_addresses,
        sum(total_volume) as total_volume,
        sum(total_fees) as total_fees,
        avg(avg_transfer_amount) as avg_transfer_amount,
        max(max_transfer_amount) as max_transfer_amount,
        min(min_transfer_amount) as min_transfer_amount,
        avg(median_transfer_amount) as median_transfer_amount,
        avg(amount_std_dev) as amount_std_dev,
        max(max_fee) as max_fee,
        min(min_fee) as min_fee,
        avg(avg_fee) as avg_fee,
        min(period_start_block) as period_start_block,
        max(period_end_block) as period_end_block,
        avg(network_density) as network_density,
        sum(tx_count_lt_01) as tx_count_lt_01,
        sum(tx_count_01_to_1) as tx_count_01_to_1,
        sum(tx_count_1_to_10) as tx_count_1_to_10,
        sum(tx_count_10_to_100) as tx_count_10_to_100,
        sum(tx_count_100_to_1k) as tx_count_100_to_1k,
        sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
        sum(tx_count_gte_10k) as tx_count_gte_10k
    FROM balance_transfers_volume_series_mv
    GROUP BY period, asset
)
SELECT
    'monthly' as period_type,
    period,
    asset,
    transaction_count,
    total_volume,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    active_addresses as unique_addresses,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    network_density as avg_network_density,
    total_fees,
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transaction_size,
    max_transfer_amount as max_transaction_size,
    min_transfer_amount as min_transaction_size,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    max_fee,
    min_fee,
    median_transfer_amount as median_transaction_size,
    amount_std_dev as avg_amount_std_dev,
    period_start_block,
    period_end_block,
    period_end_block - period_start_block + 1 as blocks_in_period
FROM monthly_base
ORDER BY period DESC, asset
```

These network analytics views provide consistent metrics across different time scales (daily, weekly, monthly), including:
- Transaction counts and volumes
- Unique participant counts
- Network density metrics
- Fee statistics
- Transaction size distribution using histogram bins
- Block range information

### Address Analytics View

The `balance_transfers_address_analytics_view` provides comprehensive metrics for each address:

```sql
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
        
        -- Time-based patterns (using hour extraction)
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 0 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 6) as night_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 6 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 12) as morning_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 12 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 18) as afternoon_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) >= 18 AND toHour(toDateTime(intDiv(block_timestamp, 1000))) < 24) as evening_transactions,
        
        -- Universal histogram bins (asset-agnostic)
        countIf(amount < 0.1) as tx_count_lt_01,
        countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
        countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
        countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
        countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
        countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
        countIf(amount >= 10000) as tx_count_gte_10k,
        
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

    -- Address Classification
    CASE
        WHEN total_volume >= 100000 AND unique_recipients >= 100 THEN 'Exchange'
        WHEN total_volume >= 100000 AND unique_recipients < 10 THEN 'Whale'
        WHEN total_volume >= 10000 AND total_transactions >= 1000 THEN 'High_Volume_Trader'
        WHEN unique_recipients >= 50 AND unique_senders >= 50 THEN 'Hub_Address'
        WHEN total_transactions >= 100 AND total_volume < 1000 THEN 'Retail_Active'
        WHEN total_transactions < 10 AND total_volume >= 10000 THEN 'Whale_Inactive'
        WHEN total_transactions < 10 AND total_volume < 100 THEN 'Retail_Inactive'
        ELSE 'Regular_User'
    END as address_type
FROM address_combined_metrics
WHERE total_transactions > 0
```

This view provides comprehensive metrics for each address, including:
- Transaction counts (incoming/outgoing)
- Volume metrics (sent/received)
- Temporal patterns (first/last activity, time distribution)
- Transaction size distribution using histogram bins
- Statistical measures (variance)
- Address classification based on behavior patterns (Exchange, Whale, High_Volume_Trader, etc.)

### Volume Aggregation Views

Similar to network views, volume is aggregated at different time scales:

#### Daily Volume View

```sql
CREATE VIEW IF NOT EXISTS balance_transfers_volume_daily_view AS
SELECT
    toDate(period_start) as date,
    asset,
    sum(transaction_count) as daily_transaction_count,
    max(unique_senders) as max_unique_senders,
    max(unique_receivers) as max_unique_receivers,
    sum(total_volume) as daily_total_volume,
    sum(total_fees) as daily_total_fees,
    sum(total_volume) / sum(transaction_count) as daily_avg_transfer_amount,
    max(max_transfer_amount) as daily_max_transfer_amount,
    min(min_transfer_amount) as daily_min_transfer_amount,
    max(active_addresses) as max_daily_active_addresses,
    avg(network_density) as avg_daily_network_density,
    avg(amount_std_dev) as avg_daily_amount_std_dev,
    avg(median_transfer_amount) as avg_daily_median_amount,
    sum(tx_count_lt_01) as daily_tx_count_lt_01,
    sum(tx_count_01_to_1) as daily_tx_count_01_to_1,
    sum(tx_count_1_to_10) as daily_tx_count_1_to_10,
    sum(tx_count_10_to_100) as daily_tx_count_10_to_100,
    sum(tx_count_100_to_1k) as daily_tx_count_100_to_1k,
    sum(tx_count_1k_to_10k) as daily_tx_count_1k_to_10k,
    sum(tx_count_gte_10k) as daily_tx_count_gte_10k,
    sum(volume_lt_01) as daily_volume_lt_01,
    sum(volume_01_to_1) as daily_volume_01_to_1,
    sum(volume_1_to_10) as daily_volume_1_to_10,
    sum(volume_10_to_100) as daily_volume_10_to_100,
    sum(volume_100_to_1k) as daily_volume_100_to_1k,
    sum(volume_1k_to_10k) as daily_volume_1k_to_10k,
    sum(volume_gte_10k) as daily_volume_gte_10k,
    min(period_start_block) as daily_start_block,
    max(period_end_block) as daily_end_block
FROM balance_transfers_volume_series_mv
GROUP BY date, asset
ORDER BY date DESC, asset
```

#### Weekly Volume View

```sql
CREATE VIEW IF NOT EXISTS balance_transfers_volume_weekly_view AS
WITH weekly_aggregates AS (
    SELECT
        toStartOfWeek(period_start) as week_start,
        asset,
        sum(transaction_count) as weekly_transaction_count,
        max(unique_senders) as unique_senders,
        max(unique_receivers) as unique_receivers,
        max(active_addresses) as active_addresses,
        sum(total_volume) as weekly_total_volume,
        sum(total_fees) as weekly_total_fees,
        max(max_transfer_amount) as weekly_max_transfer_amount,
        min(period_start_block) as weekly_start_block,
        max(period_end_block) as weekly_end_block,
        sum(tx_count_lt_01) as tx_count_lt_01,
        sum(tx_count_01_to_1) as tx_count_01_to_1,
        sum(tx_count_1_to_10) as tx_count_1_to_10,
        sum(tx_count_10_to_100) as tx_count_10_to_100,
        sum(tx_count_100_to_1k) as tx_count_100_to_1k,
        sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
        sum(tx_count_gte_10k) as tx_count_gte_10k,
        sum(volume_lt_01) as volume_lt_01,
        sum(volume_01_to_1) as volume_01_to_1,
        sum(volume_1_to_10) as volume_1_to_10,
        sum(volume_10_to_100) as volume_10_to_100,
        sum(volume_100_to_1k) as volume_100_to_1k,
        sum(volume_1k_to_10k) as volume_1k_to_10k,
        sum(volume_gte_10k) as volume_gte_10k
    FROM balance_transfers_volume_series_mv
    GROUP BY week_start, asset
)
SELECT
    week_start,
    asset,
    weekly_transaction_count,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    weekly_total_volume,
    weekly_total_fees,
    weekly_total_volume / weekly_transaction_count as weekly_avg_transfer_amount,
    weekly_max_transfer_amount,
    active_addresses as max_weekly_active_addresses,
    tx_count_lt_01 as weekly_tx_count_lt_01,
    tx_count_01_to_1 as weekly_tx_count_01_to_1,
    tx_count_1_to_10 as weekly_tx_count_1_to_10,
    tx_count_10_to_100 as weekly_tx_count_10_to_100,
    tx_count_100_to_1k as weekly_tx_count_100_to_1k,
    tx_count_1k_to_10k as weekly_tx_count_1k_to_10k,
    tx_count_gte_10k as weekly_tx_count_gte_10k,
    volume_lt_01 as weekly_volume_lt_01,
    volume_01_to_1 as weekly_volume_01_to_1,
    volume_1_to_10 as weekly_volume_1_to_10,
    volume_10_to_100 as weekly_volume_10_to_100,
    volume_100_to_1k as weekly_volume_100_to_1k,
    volume_1k_to_10k as weekly_volume_1k_to_10k,
    volume_gte_10k as weekly_volume_gte_10k,
    weekly_start_block,
    weekly_end_block
FROM weekly_aggregates
ORDER BY week_start DESC, asset
```

#### Monthly Volume View

```sql
CREATE VIEW IF NOT EXISTS balance_transfers_volume_monthly_view AS
WITH monthly_aggregates AS (
    SELECT
        toStartOfMonth(period_start) as month_start,
        asset,
        sum(transaction_count) as monthly_transaction_count,
        max(unique_senders) as unique_senders,
        max(unique_receivers) as unique_receivers,
        max(active_addresses) as active_addresses,
        sum(total_volume) as monthly_total_volume,
        sum(total_fees) as monthly_total_fees,
        max(max_transfer_amount) as monthly_max_transfer_amount,
        min(period_start_block) as monthly_start_block,
        max(period_end_block) as monthly_end_block,
        sum(tx_count_lt_01) as tx_count_lt_01,
        sum(tx_count_01_to_1) as tx_count_01_to_1,
        sum(tx_count_1_to_10) as tx_count_1_to_10,
        sum(tx_count_10_to_100) as tx_count_10_to_100,
        sum(tx_count_100_to_1k) as tx_count_100_to_1k,
        sum(tx_count_1k_to_10k) as tx_count_1k_to_10k,
        sum(tx_count_gte_10k) as tx_count_gte_10k,
        sum(volume_lt_01) as volume_lt_01,
        sum(volume_01_to_1) as volume_01_to_1,
        sum(volume_1_to_10) as volume_1_to_10,
        sum(volume_10_to_100) as volume_10_to_100,
        sum(volume_100_to_1k) as volume_100_to_1k,
        sum(volume_1k_to_10k) as volume_1k_to_10k,
        sum(volume_gte_10k) as volume_gte_10k
    FROM balance_transfers_volume_series_mv
    GROUP BY month_start, asset
)
SELECT
    month_start,
    asset,
    monthly_transaction_count,
    unique_senders as max_unique_senders,
    unique_receivers as max_unique_receivers,
    monthly_total_volume,
    monthly_total_fees,
    monthly_total_volume / monthly_transaction_count as monthly_avg_transfer_amount,
    monthly_max_transfer_amount,
    active_addresses as max_monthly_active_addresses,
    tx_count_lt_01 as monthly_tx_count_lt_01,
    tx_count_01_to_1 as monthly_tx_count_01_to_1,
    tx_count_1_to_10 as monthly_tx_count_1_to_10,
    tx_count_10_to_100 as monthly_tx_count_10_to_100,
    tx_count_100_to_1k as monthly_tx_count_100_to_1k,
    tx_count_1k_to_10k as monthly_tx_count_1k_to_10k,
    tx_count_gte_10k as monthly_tx_count_gte_10k,
    volume_lt_01 as monthly_volume_lt_01,
    volume_01_to_1 as monthly_volume_01_to_1,
    volume_1_to_10 as monthly_volume_1_to_10,
    volume_10_to_100 as monthly_volume_10_to_100,
    volume_100_to_1k as monthly_volume_100_to_1k,
    volume_1k_to_10k as monthly_volume_1k_to_10k,
    volume_gte_10k as monthly_volume_gte_10k,
    monthly_start_block,
    monthly_end_block
FROM monthly_aggregates
ORDER BY month_start DESC, asset
```

These volume aggregation views provide consistent metrics across different time scales (daily, weekly, monthly), including:
- Transaction counts and volumes
- Unique participant counts
- Fee statistics
- Transaction size distribution using histogram bins
- Volume distribution by transaction size
- Block range information

### Analysis Views

Additional views for specialized analysis:

#### Volume Trends View

```sql
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
    ) as rolling_30_period_avg_volume
    
FROM balance_transfers_volume_series_mv
ORDER BY period_start DESC, asset
```

This view calculates rolling averages for transaction volumes and counts, providing:
- 7-period (approximately 28 hours) rolling averages
- 30-period (approximately 5 days) rolling averages
- Trend analysis capabilities for identifying patterns over time

#### Volume Quantiles View

```sql
CREATE VIEW IF NOT EXISTS balance_transfers_volume_quantiles_view AS
SELECT
    toDate(period_start) as date,
    asset,
    
    -- Volume quantiles
    quantile(0.10)(total_volume) as q10_volume,
    quantile(0.25)(total_volume) as q25_volume,
    quantile(0.50)(total_volume) as median_volume,
    quantile(0.75)(total_volume) as q75_volume,
    quantile(0.90)(total_volume) as q90_volume,
    quantile(0.99)(total_volume) as q99_volume
FROM balance_transfers_volume_series_mv
GROUP BY date, asset
```

This view provides distribution analysis for transaction volumes, calculating:
- Various quantiles (10th, 25th, 50th, 75th, 90th, 99th percentiles)
- Daily aggregation for identifying distribution patterns
- Asset-specific distribution metrics

## Time Interval Aggregation

The schema implements a multi-level time aggregation strategy:

1. **Base 4-hour intervals**: The foundation level in `balance_transfers_volume_series_mv`
2. **Daily aggregation**: Combines 4-hour intervals into daily metrics
3. **Weekly aggregation**: Aggregates from the start of each week
4. **Monthly aggregation**: Aggregates from the start of each month

This approach allows for efficient storage while enabling analysis at different time scales.

## Histogram Bins

Transaction amounts are categorized into standardized bins for consistent analysis across different assets:

| Bin | Amount Range |
|-----|--------------|
| 1 | < 0.1 |
| 2 | 0.1 to < 1 |
| 3 | 1 to < 10 |
| 4 | 10 to < 100 |
| 5 | 100 to < 1,000 |
| 6 | 1,000 to < 10,000 |
| 7 | ≥ 10,000 |

These bins are used consistently throughout the schema for:
- Transaction count distribution
- Volume distribution
- Address behavior profiling

## Usage Examples

### Network Activity Analysis
```sql
-- Get daily network activity for a specific asset
SELECT * FROM balance_transfers_network_daily_view
WHERE asset = 'DOT' AND period >= '2023-01-01'
ORDER BY period DESC;
```

### Address Profiling
```sql
-- Find potential exchange addresses
SELECT address, total_volume, unique_recipients, unique_senders
FROM balance_transfers_address_analytics_view
WHERE address_type = 'Exchange'
ORDER BY total_volume DESC
LIMIT 10;
```

### Volume Trend Analysis
```sql
-- Analyze 7-day rolling average volume trends
SELECT period_start, asset, total_volume, rolling_7_period_avg_volume
FROM balance_transfers_volume_trends_view
WHERE asset = 'KSM'
ORDER BY period_start DESC;