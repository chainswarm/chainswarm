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

## Materialized Views and Regular Views

### Base Materialized View

The `balance_transfers_volume_series_mv` serves as the foundation for time-series analysis, aggregating data into 4-hour intervals:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_transfers_volume_series_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(period_start)
ORDER BY (period_start, asset)
```

This view calculates:
- Basic volume metrics (transaction count, unique participants, volumes)
- Amount distribution quantiles
- Network activity metrics (density, unique pairs)
- Temporal activity patterns
- Statistical measures (standard deviation, variance, skewness)
- Histogram bins for transaction categorization

### Network Analytics Views

Three views provide network analytics at different time scales:
- `balance_transfers_network_daily_view`
- `balance_transfers_network_weekly_view`
- `balance_transfers_network_monthly_view`

These are unified in `balance_transfers_network_analytics_view` for consistent querying.

### Address Analytics

The `balance_transfers_address_analytics_view` provides comprehensive metrics for each address:
- Transaction counts (incoming/outgoing)
- Volume metrics (sent/received)
- Temporal patterns (first/last activity, time distribution)
- Address classification (Exchange, Whale, High_Volume_Trader, etc.)

### Volume Aggregation Views

Similar to network views, volume is aggregated at different time scales:
- `balance_transfers_volume_daily_view`
- `balance_transfers_volume_weekly_view`
- `balance_transfers_volume_monthly_view`

These are unified in `balance_transfers_volume_analytics_view`.

### Analysis Views

Additional views for specialized analysis:
- `balance_transfers_volume_trends_view`: Rolling averages for trend analysis
- `balance_transfers_volume_quantiles_view`: Distribution analysis

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