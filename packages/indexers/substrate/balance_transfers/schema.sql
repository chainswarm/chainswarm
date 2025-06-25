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

-- CHUNK 6: Daily View
CREATE VIEW IF NOT EXISTS balance_transfers_volume_daily_view AS
SELECT
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as date,
    asset,
    count() as