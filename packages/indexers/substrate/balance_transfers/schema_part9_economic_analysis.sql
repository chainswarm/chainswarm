-- Balance Transfers Schema - Part 9: Economic Analysis
-- Analyze economic indicators such as token velocity, liquidity concentration, and holding patterns

-- =============================================================================
-- ECONOMIC ANALYSIS VIEWS
-- =============================================================================

-- Velocity of Money View
-- Measure how quickly tokens circulate in the ecosystem
CREATE VIEW IF NOT EXISTS balance_transfers_velocity_view AS
SELECT
    bt.asset,
    toStartOfMonth(toDateTime(intDiv(bt.block_timestamp, 1000))) as month,
    
    -- Transaction metrics
    count() as transaction_count,
    sum(bt.amount) as total_volume,
    
    -- Velocity calculation (transaction volume divided by circulating supply)
    -- Using a join with a subquery to get the circulating supply at month end
    -- Using greatest() to avoid division by zero
    sum(bt.amount) / greatest(max(bc_supply.total_supply), 1) as velocity_of_money,
    
    -- Average transaction size
    avg(bt.amount) as avg_transaction_size,
    
    -- Transaction count per address
    count() / uniq(bt.from_address, bt.to_address) as transactions_per_address,
    
    -- Volume per address
    sum(bt.amount) / uniq(bt.from_address, bt.to_address) as volume_per_address
    
FROM balance_transfers bt
LEFT JOIN (
    -- Subquery to calculate total supply per asset and month
    SELECT
        asset,
        toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000))) as month,
        sum(total_balance) as total_supply
    FROM balance_changes
    WHERE (asset, block_height) IN (
        SELECT
            asset,
            max(block_height) as max_height
        FROM balance_changes
        GROUP BY asset, toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000)))
    )
    GROUP BY asset, month
) bc_supply ON bt.asset = bc_supply.asset
    AND toStartOfMonth(toDateTime(intDiv(bt.block_timestamp, 1000))) = bc_supply.month
GROUP BY bt.asset, month
ORDER BY month DESC, bt.asset;

-- Liquidity Concentration View
-- Analyze how concentrated holdings are within the network
CREATE VIEW IF NOT EXISTS balance_transfers_liquidity_concentration_view AS
WITH monthly_balances AS (
    SELECT
        bc.asset,
        bc.address,
        bc.total_balance as balance,
        toStartOfMonth(toDateTime(intDiv(bc.block_timestamp, 1000))) as month
    FROM balance_changes bc
    JOIN (
        -- Get the latest block height for each address, asset, and month
        SELECT
            address,
            asset,
            toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000))) as month,
            max(block_height) as max_height
        FROM balance_changes
        GROUP BY address, asset, toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000)))
    ) latest ON bc.address = latest.address
        AND bc.asset = latest.asset
        AND toStartOfMonth(toDateTime(intDiv(bc.block_timestamp, 1000))) = latest.month
        AND bc.block_height = latest.max_height
    WHERE bc.total_balance > 0
),
ranked_balances AS (
    SELECT
        asset,
        address,
        balance,
        month,
        row_number() OVER (PARTITION BY asset, month ORDER BY balance) as row_number,
        row_number() OVER (PARTITION BY asset, month ORDER BY balance DESC) /
            count() OVER (PARTITION BY asset, month) as percentile_rank,
        count() OVER (PARTITION BY asset, month) as total_count
    FROM monthly_balances
),
gini_components AS (
    SELECT
        asset,
        month,
        sum(balance) as total_balance,
        sum(row_number * balance) as weighted_sum,
        max(total_count) as address_count
    FROM ranked_balances
    GROUP BY asset, month
)
SELECT
    rb.asset,
    rb.month,
    
    -- Concentration ratios (percentage of total supply held by top addresses)
    sum(if(rb.percentile_rank <= 0.01, rb.balance, 0)) / sum(rb.balance) as top_1pct_concentration,
    sum(if(rb.percentile_rank <= 0.05, rb.balance, 0)) / sum(rb.balance) as top_5pct_concentration,
    sum(if(rb.percentile_rank <= 0.10, rb.balance, 0)) / sum(rb.balance) as top_10pct_concentration,
    
    -- Gini coefficient approximation (measure of inequality)
    1 - 2 * (
        (gc.weighted_sum / gc.address_count - 0.5 * gc.total_balance) / gc.total_balance
    ) as gini_coefficient,
    
    -- Total addresses with non-zero balance
    count() as addresses_with_balance,
    
    -- Total supply
    sum(rb.balance) as total_supply
    
FROM ranked_balances rb
JOIN gini_components gc ON rb.asset = gc.asset AND rb.month = gc.month
GROUP BY rb.asset, rb.month, gc.weighted_sum, gc.address_count, gc.total_balance
ORDER BY rb.month DESC, rb.asset;

-- Token Velocity and Holding Time View
-- Analyze how long addresses hold tokens before transferring them
CREATE VIEW IF NOT EXISTS balance_transfers_holding_time_view AS
-- Simplified approach to avoid type conversion issues
WITH outgoing_transfers AS (
    SELECT
        asset,
        from_address as address,
        count() as outgoing_count,
        sum(amount) as outgoing_volume,
        avg(amount) as avg_outgoing_size,
        min(block_timestamp) as first_outgoing,
        max(block_timestamp) as last_outgoing
    FROM balance_transfers
    GROUP BY asset, from_address
),
incoming_transfers AS (
    SELECT
        asset,
        to_address as address,
        count() as incoming_count,
        sum(amount) as incoming_volume,
        avg(amount) as avg_incoming_size,
        min(block_timestamp) as first_incoming,
        max(block_timestamp) as last_incoming
    FROM balance_transfers
    GROUP BY asset, to_address
),
combined_metrics AS (
    SELECT
        COALESCE(o.asset, i.asset) as asset,
        COALESCE(o.address, i.address) as address,
        o.outgoing_count,
        o.outgoing_volume,
        o.avg_outgoing_size,
        i.incoming_count,
        i.incoming_volume,
        i.avg_incoming_size,
        least(COALESCE(o.first_outgoing, 9223372036854775807), COALESCE(i.first_incoming, 9223372036854775807)) as first_activity,
        greatest(COALESCE(o.last_outgoing, 0), COALESCE(i.last_incoming, 0)) as last_activity
    FROM outgoing_transfers o
    FULL OUTER JOIN incoming_transfers i ON o.address = i.address AND o.asset = i.asset
)
SELECT
    asset,
    address,
    
    -- Activity span metrics (approximation of holding time)
    (last_activity - first_activity) / 86400000 as activity_span_days,
    
    -- Transaction metrics
    COALESCE(outgoing_count, 0) + COALESCE(incoming_count, 0) as transaction_count,
    COALESCE(outgoing_volume, 0) + COALESCE(incoming_volume, 0) as total_volume,
    
    -- Outgoing metrics
    COALESCE(outgoing_count, 0) as outgoing_transactions,
    COALESCE(outgoing_volume, 0) as outgoing_volume,
    COALESCE(avg_outgoing_size, 0) as avg_outgoing_size,
    
    -- Incoming metrics
    COALESCE(incoming_count, 0) as incoming_transactions,
    COALESCE(incoming_volume, 0) as incoming_volume,
    COALESCE(avg_incoming_size, 0) as avg_incoming_size,
    
    -- Velocity metrics (approximation)
    COALESCE(outgoing_volume, 0) / greatest(1, (last_activity - first_activity) / 86400000) as tokens_per_day,
    
    -- Address classification based on activity span
    if((last_activity - first_activity) / 86400000 >= 365, 'Long_Term_Holder',
    if((last_activity - first_activity) / 86400000 >= 30, 'Medium_Term_Holder',
    if((last_activity - first_activity) / 86400000 >= 7, 'Short_Term_Holder',
       'Active_Trader'))) as holder_type
    
FROM combined_metrics
WHERE (COALESCE(outgoing_count, 0) + COALESCE(incoming_count, 0)) >= 3  -- Minimum transactions for meaningful analysis
  AND last_activity > first_activity  -- Ensure valid activity span
ORDER BY activity_span_days DESC;