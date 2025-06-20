-- Balance Transfers Schema - Part 10: Anomaly Detection
-- Detect unusual transaction patterns and potential anomalies

-- =============================================================================
-- ANOMALY DETECTION VIEWS
-- =============================================================================

-- Basic Transaction Anomaly View
-- Simple view to detect unusual transaction patterns
CREATE VIEW IF NOT EXISTS balance_transfers_basic_anomaly_view AS
SELECT
    from_address,
    to_address,
    asset,
    amount,
    block_timestamp,
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as transaction_date,
    toMonth(toDateTime(intDiv(block_timestamp, 1000))) as transaction_month,
    toYear(toDateTime(intDiv(block_timestamp, 1000))) as transaction_year
FROM balance_transfers
WHERE amount > 10 * (
    SELECT avg(amount)
    FROM balance_transfers
    WHERE asset = balance_transfers.asset
)
ORDER BY amount DESC;