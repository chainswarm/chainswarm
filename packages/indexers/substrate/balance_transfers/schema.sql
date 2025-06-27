-- Balance Transfers Schema (Optimized for ClickHouse 25.5+)
-- Chunked for proper execution

-- CHUNK 1: Core Tables
CREATE TABLE IF NOT EXISTS balance_transfers (
    extrinsic_id String,
    event_idx String,
    block_height UInt32,
    block_timestamp UInt64,
    from_address String,
    to_address String,
    asset String,
    amount Decimal128(18),
    fee Decimal128(18),
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(block_height, 100000)
ORDER BY (extrinsic_id, event_idx, asset)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS known_addresses (
    id UUID,
    network String,
    address String,
    label String,
    source String,
    source_type String,
    last_updated DateTime,
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, address)
SETTINGS index_granularity = 8192;

-- CHUNK 2: Primary Indexes
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_from_address from_address TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_to_address to_address TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset asset TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_block_height block_height TYPE minmax GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 4;

-- CHUNK 3: Secondary Indexes
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_date toDate(toDateTime(intDiv(block_timestamp, 1000))) TYPE minmax GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_amount_range amount TYPE minmax GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset_from_address (asset, from_address) TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset_to_address (asset, to_address) TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_from_to_asset (from_address, to_address, asset) TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_version _version TYPE minmax GRANULARITY 4;

-- CHUNK 4: Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_volume_series_mv_internal
ENGINE = SummingMergeTree((
    transaction_count,
    unique_senders,
    unique_receivers,
    total_volume,
    total_fees,
    unique_address_pairs,
    active_addresses,
    hour_0_tx_count,
    hour_1_tx_count,
    hour_2_tx_count,
    hour_3_tx_count,
    blocks_in_period,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    volume_lt_01,
    volume_01_to_1,
    volume_1_to_10,
    volume_10_to_100,
    volume_100_to_1k,
    volume_1k_to_10k,
    volume_gte_10k
))
PARTITION BY toYYYYMM(period_start)
ORDER BY (period_start, asset)
SETTINGS index_granularity = 8192
AS
SELECT
    toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400) as period_start,
    toDateTime((intDiv(intDiv(block_timestamp, 1000), 14400) + 1) * 14400) as period_end,
    asset,
    count() as transaction_count,
    uniqExact(from_address) as unique_senders,
    uniqExact(to_address) as unique_receivers,
    sum(amount) as total_volume,
    sum(fee) as total_fees,
    uniq(from_address, to_address) as unique_address_pairs,
    uniqExact(from_address) + uniqExact(to_address) as active_addresses,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400))) as hour_0_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400)) + 1) as hour_1_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400)) + 2) as hour_2_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400)) + 3) as hour_3_tx_count,
    max(block_height) - min(block_height) + 1 as blocks_in_period,
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    sumIf(amount, amount < 0.1) as volume_lt_01,
    sumIf(amount, amount >= 0.1 AND amount < 1) as volume_01_to_1,
    sumIf(amount, amount >= 1 AND amount < 10) as volume_1_to_10,
    sumIf(amount, amount >= 10 AND amount < 100) as volume_10_to_100,
    sumIf(amount, amount >= 100 AND amount < 1000) as volume_100_to_1k,
    sumIf(amount, amount >= 1000 AND amount < 10000) as volume_1k_to_10k,
    sumIf(amount, amount >= 10000) as volume_gte_10k,
    argMax(block_height, block_timestamp) as latest_block_height,
    argMin(block_height, block_timestamp) as earliest_block_height,
    argMax(amount, block_timestamp) as max_transfer_amount,
    argMin(amount, block_timestamp) as min_transfer_amount
FROM balance_transfers
GROUP BY period_start, period_end, asset;

-- CHUNK 5: Simple Views
CREATE VIEW IF NOT EXISTS balance_transfers_volume_series_view AS
SELECT
    period_start,
    period_end,
    asset,
    transaction_count,
    unique_senders,
    unique_receivers,
    total_volume,
    total_fees,
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transfer_amount,
    max_transfer_amount,
    min_transfer_amount,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    unique_address_pairs,
    active_addresses,
    CASE WHEN active_addresses > 1 THEN toFloat64(unique_address_pairs) / (toFloat64(active_addresses) * toFloat64(active_addresses - 1) / 2.0) ELSE 0.0 END as network_density,
    toHour(period_start) as period_hour,
    hour_0_tx_count,
    hour_1_tx_count,
    hour_2_tx_count,
    hour_3_tx_count,
    earliest_block_height as period_start_block,
    latest_block_height as period_end_block,
    blocks_in_period,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    volume_lt_01,
    volume_01_to_1,
    volume_1_to_10,
    volume_10_to_100,
    volume_100_to_1k,
    volume_1k_to_10k,
    volume_gte_10k
FROM balance_transfers_volume_series_mv_internal
ORDER BY period_start DESC, asset;

-- CHUNK 6: Network Analytics Views
CREATE VIEW IF NOT EXISTS balance_transfers_network_daily_view AS
SELECT
    'daily' as period_type,
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as period,
    asset,
    count() as transaction_count,
    sum(amount) as total_volume,
    uniqExact(from_address) as max_unique_senders,
    uniqExact(to_address) as max_unique_receivers,
    uniqExact(from_address) + uniqExact(to_address) as unique_addresses,
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    CASE WHEN (uniqExact(from_address) + uniqExact(to_address)) > 1 THEN toFloat64(uniq(from_address, to_address)) / (toFloat64(uniqExact(from_address) + uniqExact(to_address)) * toFloat64(uniqExact(from_address) + uniqExact(to_address) - 1) / 2.0) ELSE 0.0 END as avg_network_density,
    sum(fee) as total_fees,
    CASE WHEN count() > 0 THEN sum(amount) / count() ELSE 0 END as avg_transaction_size,
    max(amount) as max_transaction_size,
    min(amount) as min_transaction_size,
    CASE WHEN count() > 0 THEN sum(fee) / count() ELSE 0 END as avg_fee,
    max(fee) as max_fee,
    min(fee) as min_fee,
    median(amount) as median_transaction_size,
    stddevPop(amount) as avg_amount_std_dev
FROM balance_transfers
GROUP BY period, asset
ORDER BY period DESC, asset;

CREATE VIEW IF NOT EXISTS balance_transfers_network_weekly_view AS
SELECT
    'weekly' as period_type,
    toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as period,
    asset,
    count() as transaction_count,
    sum(amount) as total_volume,
    uniqExact(from_address) as max_unique_senders,
    uniqExact(to_address) as max_unique_receivers,
    uniqExact(from_address) + uniqExact(to_address) as unique_addresses,
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    CASE WHEN (uniqExact(from_address) + uniqExact(to_address)) > 1 THEN toFloat64(uniq(from_address, to_address)) / (toFloat64(uniqExact(from_address) + uniqExact(to_address)) * toFloat64(uniqExact(from_address) + uniqExact(to_address) - 1) / 2.0) ELSE 0.0 END as avg_network_density,
    sum(fee) as total_fees,
    CASE WHEN count() > 0 THEN sum(amount) / count() ELSE 0 END as avg_transaction_size,
    max(amount) as max_transaction_size,
    min(amount) as min_transaction_size,
    CASE WHEN count() > 0 THEN sum(fee) / count() ELSE 0 END as avg_fee,
    max(fee) as max_fee,
    min(fee) as min_fee,
    median(amount) as median_transaction_size,
    stddevPop(amount) as avg_amount_std_dev
FROM balance_transfers
GROUP BY period, asset
ORDER BY period DESC, asset;

CREATE VIEW IF NOT EXISTS balance_transfers_network_monthly_view AS
SELECT
    'monthly' as period_type,
    toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000))) as period,
    asset,
    count() as transaction_count,
    sum(amount) as total_volume,
    uniqExact(from_address) as max_unique_senders,
    uniqExact(to_address) as max_unique_receivers,
    uniqExact(from_address) + uniqExact(to_address) as unique_addresses,
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    CASE WHEN (uniqExact(from_address) + uniqExact(to_address)) > 1 THEN toFloat64(uniq(from_address, to_address)) / (toFloat64(uniqExact(from_address) + uniqExact(to_address)) * toFloat64(uniqExact(from_address) + uniqExact(to_address) - 1) / 2.0) ELSE 0.0 END as avg_network_density,
    sum(fee) as total_fees,
    CASE WHEN count() > 0 THEN sum(amount) / count() ELSE 0 END as avg_transaction_size,
    max(amount) as max_transaction_size,
    min(amount) as min_transaction_size,
    CASE WHEN count() > 0 THEN sum(fee) / count() ELSE 0 END as avg_fee,
    max(fee) as max_fee,
    min(fee) as min_fee,
    median(amount) as median_transaction_size,
    stddevPop(amount) as avg_amount_std_dev,
    min(block_height) as period_start_block,
    max(block_height) as period_end_block,
    max(block_height) - min(block_height) + 1 as blocks_in_period
FROM balance_transfers
GROUP BY period, asset
ORDER BY period DESC, asset;

-- CHUNK 7: Volume Views
CREATE VIEW IF NOT EXISTS balance_transfers_volume_daily_view AS
SELECT
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as date,
    asset,
    count() as daily_transaction_count,
    uniqExact(from_address) as max_unique_senders,
    uniqExact(to_address) as max_unique_receivers,
    sum(amount) as daily_total_volume,
    sum(fee) as daily_total_fees,
    CASE WHEN count() > 0 THEN sum(amount) / count() ELSE 0 END as daily_avg_transfer_amount,
    max(amount) as daily_max_transfer_amount,
    min(amount) as daily_min_transfer_amount,
    uniqExact(from_address) + uniqExact(to_address) as max_daily_active_addresses,
    CASE WHEN (uniqExact(from_address) + uniqExact(to_address)) > 1 THEN toFloat64(uniq(from_address, to_address)) / (toFloat64(uniqExact(from_address) + uniqExact(to_address)) * toFloat64(uniqExact(from_address) + uniqExact(to_address) - 1) / 2.0) ELSE 0.0 END as avg_daily_network_density,
    stddevPop(amount) as avg_daily_amount_std_dev,
    median(amount) as avg_daily_median_amount,
    countIf(amount < 0.1) as daily_tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as daily_tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as daily_tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as daily_tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as daily_tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as daily_tx_count_1k_to_10k,
    countIf(amount >= 10000) as daily_tx_count_gte_10k,
    sumIf(amount, amount < 0.1) as daily_volume_lt_01,
    sumIf(amount, amount >= 0.1 AND amount < 1) as daily_volume_01_to_1,
    sumIf(amount, amount >= 1 AND amount < 10) as daily_volume_1_to_10,
    sumIf(amount, amount >= 10 AND amount < 100) as daily_volume_10_to_100,
    sumIf(amount, amount >= 100 AND amount < 1000) as daily_volume_100_to_1k,
    sumIf(amount, amount >= 1000 AND amount < 10000) as daily_volume_1k_to_10k,
    sumIf(amount, amount >= 10000) as daily_volume_gte_10k,
    min(block_height) as daily_start_block,
    max(block_height) as daily_end_block
FROM balance_transfers
GROUP BY date, asset
ORDER BY date DESC, asset;

CREATE VIEW IF NOT EXISTS balance_transfers_volume_weekly_view AS
SELECT
    toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as week_start,
    asset,
    count() as weekly_transaction_count,
    uniqExact(from_address) as max_unique_senders,
    uniqExact(to_address) as max_unique_receivers,
    sum(amount) as weekly_total_volume,
    sum(fee) as weekly_total_fees,
    CASE WHEN count() > 0 THEN sum(amount) / count() ELSE 0 END as weekly_avg_transfer_amount,
    max(amount) as weekly_max_transfer_amount,
    uniqExact(from_address) + uniqExact(to_address) as max_weekly_active_addresses,
    countIf(amount < 0.1) as weekly_tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as weekly_tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as weekly_tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as weekly_tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as weekly_tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as weekly_tx_count_1k_to_10k,
    countIf(amount >= 10000) as weekly_tx_count_gte_10k,
    sumIf(amount, amount < 0.1) as weekly_volume_lt_01,
    sumIf(amount, amount >= 0.1 AND amount < 1) as weekly_volume_01_to_1,
    sumIf(amount, amount >= 1 AND amount < 10) as weekly_volume_1_to_10,
    sumIf(amount, amount >= 10 AND amount < 100) as weekly_volume_10_to_100,
    sumIf(amount, amount >= 100 AND amount < 1000) as weekly_volume_100_to_1k,
    sumIf(amount, amount >= 1000 AND amount < 10000) as weekly_volume_1k_to_10k,
    sumIf(amount, amount >= 10000) as weekly_volume_gte_10k,
    min(block_height) as weekly_start_block,
    max(block_height) as weekly_end_block
FROM balance_transfers
GROUP BY week_start, asset
ORDER BY week_start DESC, asset;

CREATE VIEW IF NOT EXISTS balance_transfers_volume_monthly_view AS
SELECT
    toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000))) as month_start,
    asset,
    count() as monthly_transaction_count,
    uniqExact(from_address) as max_unique_senders,
    uniqExact(to_address) as max_unique_receivers,
    sum(amount) as monthly_total_volume,
    sum(fee) as monthly_total_fees,
    CASE WHEN count() > 0 THEN sum(amount) / count() ELSE 0 END as monthly_avg_transfer_amount,
    max(amount) as monthly_max_transfer_amount,
    uniqExact(from_address) + uniqExact(to_address) as max_monthly_active_addresses,
    countIf(amount < 0.1) as monthly_tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as monthly_tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as monthly_tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as monthly_tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as monthly_tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as monthly_tx_count_1k_to_10k,
    countIf(amount >= 10000) as monthly_tx_count_gte_10k,
    sumIf(amount, amount < 0.1) as monthly_volume_lt_01,
    sumIf(amount, amount >= 0.1 AND amount < 1) as monthly_volume_01_to_1,
    sumIf(amount, amount >= 1 AND amount < 10) as monthly_volume_1_to_10,
    sumIf(amount, amount >= 10 AND amount < 100) as monthly_volume_10_to_100,
    sumIf(amount, amount >= 100 AND amount < 1000) as monthly_volume_100_to_1k,
    sumIf(amount, amount >= 1000 AND amount < 10000) as monthly_volume_1k_to_10k,
    sumIf(amount, amount >= 10000) as monthly_volume_gte_10k,
    min(block_height) as monthly_start_block,
    max(block_height) as monthly_end_block
FROM balance_transfers
GROUP BY month_start, asset
ORDER BY month_start DESC, asset;

-- CHUNK 8: Address Analytics View
CREATE VIEW IF NOT EXISTS balance_transfers_address_analytics_view AS
WITH outgoing_metrics AS (
    SELECT
        from_address as address,
        asset,
        count() as outgoing_count,
        sum(amount) as total_sent,
        sum(fee) as total_fees_paid,
        uniq(to_address) as unique_recipients,
        min(block_timestamp) as first_activity,
        max(block_timestamp) as last_activity,
        uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as active_days,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 0 AND 5) as night_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 6 AND 11) as morning_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 12 AND 17) as afternoon_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 18 AND 23) as evening_transactions,
        countIf(amount < 0.1) as tx_count_lt_01,
        countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
        countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
        countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
        countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
        countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
        countIf(amount >= 10000) as tx_count_gte_10k,
        varPop(amount) as sent_amount_variance
    FROM balance_transfers
    GROUP BY from_address, asset
),
incoming_metrics AS (
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
    COALESCE(out.address, inc.address) as address,
    COALESCE(out.asset, inc.asset) as asset,
    COALESCE(out.outgoing_count, 0) + COALESCE(inc.incoming_count, 0) as total_transactions,
    COALESCE(out.outgoing_count, 0) as outgoing_count,
    COALESCE(inc.incoming_count, 0) as incoming_count,
    COALESCE(out.total_sent, 0) as total_sent,
    COALESCE(inc.total_received, 0) as total_received,
    COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) as total_volume,
    COALESCE(out.unique_recipients, 0) as unique_recipients,
    COALESCE(inc.unique_senders, 0) as unique_senders,
    COALESCE(out.first_activity, 0) as first_activity,
    COALESCE(out.last_activity, 0) as last_activity,
    COALESCE(out.last_activity, 0) - COALESCE(out.first_activity, 0) as activity_span_seconds,
    COALESCE(out.total_fees_paid, 0) as total_fees_paid,
    CASE WHEN out.outgoing_count > 0 THEN out.total_fees_paid / out.outgoing_count ELSE 0 END as avg_fee_paid,
    COALESCE(out.night_transactions, 0) as night_transactions,
    COALESCE(out.morning_transactions, 0) as morning_transactions,
    COALESCE(out.afternoon_transactions, 0) as afternoon_transactions,
    COALESCE(out.evening_transactions, 0) as evening_transactions,
    COALESCE(out.tx_count_lt_01, 0) as tx_count_lt_01,
    COALESCE(out.tx_count_01_to_1, 0) as tx_count_01_to_1,
    COALESCE(out.tx_count_1_to_10, 0) as tx_count_1_to_10,
    COALESCE(out.tx_count_10_to_100, 0) as tx_count_10_to_100,
    COALESCE(out.tx_count_100_to_1k, 0) as tx_count_100_to_1k,
    COALESCE(out.tx_count_1k_to_10k, 0) as tx_count_1k_to_10k,
    COALESCE(out.tx_count_gte_10k, 0) as tx_count_gte_10k,
    COALESCE(out.sent_amount_variance, 0) as sent_amount_variance,
    COALESCE(inc.received_amount_variance, 0) as received_amount_variance,
    COALESCE(out.active_days, 0) as active_days,
    CASE
        WHEN COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) >= 100000 AND COALESCE(out.unique_recipients, 0) >= 100 THEN 'Exchange'
        WHEN COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) >= 100000 AND COALESCE(out.unique_recipients, 0) < 10 THEN 'Whale'
        WHEN COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) >= 10000 AND COALESCE(out.outgoing_count, 0) + COALESCE(inc.incoming_count, 0) >= 1000 THEN 'High_Volume_Trader'
        WHEN COALESCE(out.unique_recipients, 0) >= 50 AND COALESCE(inc.unique_senders, 0) >= 50 THEN 'Hub_Address'
        WHEN COALESCE(out.outgoing_count, 0) + COALESCE(inc.incoming_count, 0) >= 100 AND COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) < 1000 THEN 'Retail_Active'
        WHEN COALESCE(out.outgoing_count, 0) + COALESCE(inc.incoming_count, 0) < 10 AND COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) >= 10000 THEN 'Whale_Inactive'
        WHEN COALESCE(out.outgoing_count, 0) + COALESCE(inc.incoming_count, 0) < 10 AND COALESCE(out.total_sent, 0) + COALESCE(inc.total_received, 0) < 100 THEN 'Retail_Inactive'
        ELSE 'Regular_User'
    END as address_type
FROM outgoing_metrics out
FULL OUTER JOIN incoming_metrics inc ON out.address = inc.address AND out.asset = inc.asset
WHERE COALESCE(out.outgoing_count, 0) + COALESCE(inc.incoming_count, 0) > 0;

-- CHUNK 9: Volume Trends View
CREATE VIEW IF NOT EXISTS balance_transfers_volume_trends_view AS
SELECT
    period_start,
    asset,
    sum(total_volume) as total_volume,
    sum(transaction_count) as transaction_count,
    avg(total_volume) OVER (PARTITION BY asset ORDER BY period_start ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7_period_avg_volume,
    avg(transaction_count) OVER (PARTITION BY asset ORDER BY period_start ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7_period_avg_tx_count,
    avg(total_volume) OVER (PARTITION BY asset ORDER BY period_start ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as rolling_30_period_avg_volume
FROM balance_transfers_volume_series_mv_internal
GROUP BY period_start, asset
ORDER BY period_start DESC, asset;