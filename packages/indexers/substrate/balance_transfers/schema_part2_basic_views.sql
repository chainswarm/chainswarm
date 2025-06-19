-- Balance Transfers Schema - Part 2: Basic Views
-- Basic views for transfer statistics and daily volume

-- =============================================================================
-- BASIC VIEWS
-- =============================================================================

-- View for transfer statistics by address and asset
CREATE VIEW IF NOT EXISTS balance_transfers_statistics_view AS
SELECT
    address,
    asset,
    countIf(address = from_address) as outgoing_transfer_count,
    countIf(address = to_address) as incoming_transfer_count,
    sumIf(amount, address = from_address) as total_sent,
    sumIf(amount, address = to_address) as total_received,
    sumIf(fee, address = from_address) as total_fees_paid,
    min(block_height) as first_activity_block,
    max(block_height) as last_activity_block
FROM (
    SELECT from_address as address, from_address, to_address, asset, amount, fee, block_height FROM balance_transfers
    UNION ALL
    SELECT to_address as address, from_address, to_address, asset, amount, 0 as fee, block_height FROM balance_transfers
)
GROUP BY address, asset;

-- Materialized view for daily transfer volume
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_daily_volume_mv
ENGINE = AggregatingMergeTree()
ORDER BY (date, asset)
AS
SELECT
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as date,
    asset,
    count() as transfer_count,
    uniqExact(from_address) as unique_senders,
    uniqExact(to_address) as unique_receivers,
    sum(amount) as total_volume,
    sum(fee) as total_fees,
    avg(amount) as avg_transfer_amount,
    max(amount) as max_transfer_amount
FROM balance_transfers
GROUP BY date, asset;

-- Simple view for available assets list
CREATE VIEW IF NOT EXISTS available_transfer_assets_view AS
SELECT DISTINCT asset
FROM balance_transfers
WHERE asset != ''
ORDER BY asset;