# Balance Series Indexer

## Overview

The Balance Series indexer tracks account balance changes over time on Substrate networks. It consumes data from the Block Stream indexer and provides a comprehensive solution for monitoring and analyzing balance fluctuations with the following key features:

- **Time-series tracking** with fixed 4-hour interval snapshots
- **Multi-balance type support** (free, reserved, staked, total)
- **Change tracking** between periods with both absolute and percentage metrics
- **Multi-level time aggregation** (4-hour, daily, weekly, monthly) for temporal analysis
- **Optimized views** for efficient querying at different time scales

## Core Table Structure

The foundation of the indexer is the `balance_series` table:

```sql
CREATE TABLE IF NOT EXISTS balance_series (
    -- Time period information
    period_start_timestamp UInt64,
    period_end_timestamp UInt64,
    
    -- Block information for the snapshot
    block_height UInt32,
    
    -- Address and balance information
    address String,
    asset String,
    free_balance Decimal128(18),
    reserved_balance Decimal128(18),
    staked_balance Decimal128(18),
    total_balance Decimal128(18),
    
    -- Change since last period
    free_balance_change Decimal128(18),
    reserved_balance_change Decimal128(18),
    staked_balance_change Decimal128(18),
    total_balance_change Decimal128(18),
    
    -- Percentage change since last period
    total_balance_percent_change Decimal64(6),
    
    -- Versioning for updates
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
```

### Key Fields

- **period_start_timestamp, period_end_timestamp**: Define the 4-hour interval (Unix timestamps in milliseconds)
- **block_height**: Block height at the end of the period
- **address**: Account address being tracked
- **asset**: Token or currency being tracked
- **free_balance, reserved_balance, staked_balance, total_balance**: Different balance types
- **[balance_type]_change**: Absolute change since the previous period
- **total_balance_percent_change**: Percentage change in total balance
- **_version**: Used for update management with ReplacingMergeTree engine

## Indexes

The schema includes multiple indexes for optimizing different query patterns:

### Address and Asset Indexes
- `idx_address`: Bloom filter for efficient address lookups
- `idx_asset`: Bloom filter for asset lookups
- `idx_asset_address`: Composite index for efficient asset-address queries

### Time-Based Indexes
- `idx_period_start`, `idx_period_end`: Range queries for time periods
- `idx_block_height`: Range queries for block heights

These indexes enable efficient querying for specific addresses, assets, time periods, and combinations thereof, which is essential for time-series analysis of balance data.

## Views and Materialized Views

### Latest Balance View

The `balance_series_latest_view` provides the most recent balance snapshot for each address and asset:

```sql
CREATE VIEW IF NOT EXISTS balance_series_latest_view AS
SELECT
    address,
    asset,
    argMax(period_start_timestamp, period_start_timestamp) as latest_period_start,
    argMax(period_end_timestamp, period_start_timestamp) as latest_period_end,
    argMax(block_height, period_start_timestamp) as latest_block_height,
    argMax(free_balance, period_start_timestamp) as free_balance,
    argMax(reserved_balance, period_start_timestamp) as reserved_balance,
    argMax(staked_balance, period_start_timestamp) as staked_balance,
    argMax(total_balance, period_start_timestamp) as total_balance
FROM balance_series
GROUP BY address, asset;
```

This view is particularly useful for quickly retrieving current balances without having to query the entire time series.

### Daily Aggregation View

The `balance_series_daily_view` aggregates data at the daily level:

```sql
CREATE VIEW IF NOT EXISTS balance_series_daily_view AS
SELECT
    toDate(fromUnixTimestamp64Milli(period_start_timestamp)) as date,
    address,
    asset,
    -- Take the last period of each day
    argMax(free_balance, period_start_timestamp) as end_of_day_free_balance,
    argMax(reserved_balance, period_start_timestamp) as end_of_day_reserved_balance,
    argMax(staked_balance, period_start_timestamp) as end_of_day_staked_balance,
    argMax(total_balance, period_start_timestamp) as end_of_day_total_balance,
    -- Calculate daily change
    sum(free_balance_change) as daily_free_balance_change,
    sum(reserved_balance_change) as daily_reserved_balance_change,
    sum(staked_balance_change) as daily_staked_balance_change,
    sum(total_balance_change) as daily_total_balance_change
FROM balance_series
GROUP BY date, address, asset;
```

### Weekly and Monthly Materialized Views

Two materialized views provide aggregated data at weekly and monthly levels:

- `balance_series_weekly_mv`: Aggregates data from the start of each week
- `balance_series_monthly_mv`: Aggregates data from the start of each month

These views calculate:
- End-of-period balances for each balance type
- Cumulative balance changes over the period
- Last block height of the period

## Time-Based Aggregation Approach

The schema implements a multi-level time aggregation strategy:

1. **Base 4-hour intervals**: The foundation level in the main `balance_series` table
2. **Daily aggregation**: Combines 4-hour intervals into daily metrics via `balance_series_daily_view`
3. **Weekly aggregation**: Aggregates from the start of each week via `balance_series_weekly_mv`
4. **Monthly aggregation**: Aggregates from the start of each month via `balance_series_monthly_mv`

This approach provides several benefits:
- **Storage efficiency**: Base data is stored at 4-hour intervals rather than per-block
- **Query performance**: Pre-aggregated views for common time periods
- **Flexible analysis**: Ability to analyze trends at different time scales
- **Reduced computational load**: Aggregations are pre-computed rather than calculated at query time

## Data Structure for Balance Tracking

The balance_series schema is designed to efficiently track balance changes over time:

1. **Snapshot approach**: Each record represents a balance snapshot at the end of a 4-hour period
2. **Change tracking**: Each record includes the change since the previous period
3. **Multiple balance types**: Tracks free, reserved, staked, and total balances separately
4. **Percentage change**: Includes percentage change for total balance for easier trend analysis
5. **Constraints**: Enforces non-negative balances through CHECK constraints

This structure enables:
- Historical balance analysis
- Trend identification
- Volatility assessment
- Correlation with network events
- Address behavior profiling

## Usage Examples

### Current Balance Lookup
```sql
-- Get current balances for a specific address
SELECT * FROM balance_series_latest_view
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
ORDER BY asset;
```

### Balance History Analysis
```sql
-- Get daily balance history for an address and asset
SELECT date, end_of_day_total_balance, daily_total_balance_change
FROM balance_series_daily_view
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
  AND asset = 'DOT'
ORDER BY date DESC;
```

### Monthly Balance Trends
```sql
-- Analyze monthly balance trends for an asset
SELECT month_start, 
       end_of_month_total_balance, 
       monthly_total_balance_change
FROM balance_series_monthly_mv
WHERE address = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
  AND asset = 'DOT'
ORDER BY month_start DESC;