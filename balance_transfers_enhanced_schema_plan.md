# Enhanced Balance Transfers Schema - Address Analytics Implementation Plan

## Overview

This document provides a comprehensive enhancement plan for the `balance_transfers/schema.sql` file, focusing on advanced address analytics capabilities. The enhancements will provide deep insights into address behavior patterns, relationship mapping, and activity classification.

## Current Schema Analysis

The existing [`packages/indexers/substrate/balance_transfers/schema.sql`](packages/indexers/substrate/balance_transfers/schema.sql) includes:

- **Main Table**: `balance_transfers` with ReplacingMergeTree engine
- **Basic Analytics**: `balance_transfers_statistics_view` and `balance_transfers_daily_volume_mv`
- **Performance Indexes**: Bloom filters and composite indexes
- **Asset Support**: Multi-asset tracking across Substrate networks

## Enhanced Schema Components

### 1. Address Behavior Profiles (Materialized View)

**Purpose**: Create comprehensive behavioral profiles for each address based on transaction patterns.

```sql
-- Address Behavior Profiles Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS address_behavior_profiles_mv
ENGINE = AggregatingMergeTree()
ORDER BY (asset, address)
POPULATE
AS
SELECT
    address,
    asset,
    
    -- Basic Activity Metrics
    count() as total_transactions,
    countIf(from_address = address) as outgoing_count,
    countIf(to_address = address) as incoming_count,
    
    -- Volume Metrics
    sumIf(amount, from_address = address) as total_sent,
    sumIf(amount, to_address = address) as total_received,
    sum(amount) as total_volume,
    
    -- Statistical Metrics
    avgIf(amount, from_address = address) as avg_sent_amount,
    avgIf(amount, to_address = address) as avg_received_amount,
    quantileIf(0.5)(amount, from_address = address) as median_sent_amount,
    quantileIf(0.5)(amount, to_address = address) as median_received_amount,
    quantileIf(0.9)(amount, from_address = address) as p90_sent_amount,
    quantileIf(0.9)(amount, to_address = address) as p90_received_amount,
    
    -- Temporal Patterns
    min(block_timestamp) as first_activity,
    max(block_timestamp) as last_activity,
    max(block_timestamp) - min(block_timestamp) as activity_span_seconds,
    count() / ((max(block_timestamp) - min(block_timestamp)) / 86400 + 1) as avg_transactions_per_day,
    
    -- Unique Counterparties
    uniqIf(to_address, from_address = address) as unique_recipients,
    uniqIf(from_address, to_address = address) as unique_senders,
    uniq(multiIf(from_address = address, to_address, from_address)) as total_unique_counterparties,
    
    -- Fee Analysis
    sumIf(fee, from_address = address) as total_fees_paid,
    avgIf(fee, from_address = address) as avg_fee_paid,
    maxIf(fee, from_address = address) as max_fee_paid,
    
    -- Activity Distribution by Time of Day
    countIf(toHour(intDiv(block_timestamp, 1000)) BETWEEN 0 AND 5) as night_transactions,
    countIf(toHour(intDiv(block_timestamp, 1000)) BETWEEN 6 AND 11) as morning_transactions,
    countIf(toHour(intDiv(block_timestamp, 1000)) BETWEEN 12 AND 17) as afternoon_transactions,
    countIf(toHour(intDiv(block_timestamp, 1000)) BETWEEN 18 AND 23) as evening_transactions,
    
    -- Transaction Size Distribution
    countIf(amount < 100) as micro_transactions,
    countIf(amount >= 100 AND amount < 1000) as small_transactions,
    countIf(amount >= 1000 AND amount < 10000) as medium_transactions,
    countIf(amount >= 10000 AND amount < 100000) as large_transactions,
    countIf(amount >= 100000) as whale_transactions,
    
    -- Behavioral Indicators
    stddevPopIf(amount, from_address = address) as sent_amount_variance,
    stddevPopIf(amount, to_address = address) as received_amount_variance,
    uniq(toDate(intDiv(block_timestamp, 1000))) as active_days,
    
    -- Network Effect Metrics
    countIf(from_address = address AND to_address IN (
        SELECT DISTINCT from_address FROM balance_transfers bt2 
        WHERE bt2.to_address = address AND bt2.asset = bt.asset
    )) as circular_transactions
    
FROM balance_transfers bt
GROUP BY address, asset
HAVING total_transactions >= 1;
```

### 2. Address Classification View

**Purpose**: Classify addresses into behavioral categories with risk assessment.

```sql
-- Address Classification View
CREATE VIEW IF NOT EXISTS address_classification_view AS
SELECT
    address,
    asset,
    total_volume,
    total_transactions,
    unique_recipients,
    unique_senders,
    total_unique_counterparties,
    avg_transactions_per_day,
    activity_span_seconds,
    
    -- Primary Classification Logic
    CASE
        WHEN total_volume >= 1000000 AND unique_recipients >= 100 THEN 'Exchange'
        WHEN total_volume >= 1000000 AND unique_recipients < 10 THEN 'Whale'
        WHEN total_volume >= 100000 AND total_transactions >= 1000 THEN 'High_Volume_Trader'
        WHEN unique_recipients >= 50 AND unique_senders >= 50 THEN 'Hub_Address'
        WHEN total_transactions >= 100 AND total_volume < 10000 THEN 'Retail_Active'
        WHEN total_transactions < 10 AND total_volume >= 50000 THEN 'Whale_Inactive'
        WHEN total_transactions < 10 AND total_volume < 1000 THEN 'Retail_Inactive'
        WHEN circular_transactions >= total_transactions * 0.3 THEN 'Circular_Trader'
        ELSE 'Regular_User'
    END as address_type,
    
    -- Risk Assessment
    CASE
        WHEN unique_recipients = 1 AND total_transactions > 100 THEN 'Potential_Mixer'
        WHEN abs(avg_sent_amount - avg_received_amount) < avg_sent_amount * 0.05 AND total_transactions > 50 THEN 'Potential_Tumbler'
        WHEN night_transactions / total_transactions > 0.8 THEN 'Unusual_Hours'
        WHEN whale_transactions > 0 AND total_transactions < 20 THEN 'Large_Infrequent'
        WHEN sent_amount_variance < avg_sent_amount * 0.1 AND total_transactions > 20 THEN 'Fixed_Amount_Pattern'
        WHEN circular_transactions >= 10 THEN 'High_Circular_Activity'
        ELSE 'Normal'
    END as risk_flag,
    
    -- Activity Level Classification
    CASE
        WHEN activity_span_seconds < 86400 THEN 'Single_Day'
        WHEN activity_span_seconds < 604800 THEN 'Weekly'
        WHEN activity_span_seconds < 2592000 THEN 'Monthly'
        WHEN activity_span_seconds < 31536000 THEN 'Yearly'
        ELSE 'Long_Term'
    END as activity_duration,
    
    -- Volume Classification
    CASE
        WHEN total_volume >= 1000000 THEN 'Ultra_High'
        WHEN total_volume >= 100000 THEN 'High'
        WHEN total_volume >= 10000 THEN 'Medium'
        WHEN total_volume >= 1000 THEN 'Low'
        ELSE 'Micro'
    END as volume_tier,
    
    -- Activity Pattern
    CASE
        WHEN avg_transactions_per_day >= 10 THEN 'Very_Active'
        WHEN avg_transactions_per_day >= 1 THEN 'Active'
        WHEN avg_transactions_per_day >= 0.1 THEN 'Moderate'
        ELSE 'Inactive'
    END as activity_level,
    
    -- Diversification Score (0-1, higher = more diversified)
    least(total_unique_counterparties / 100.0, 1.0) as diversification_score,
    
    -- Behavioral Consistency Score (0-1, higher = more consistent)
    1.0 - least(greatest(sent_amount_variance / nullif(avg_sent_amount, 0), 0), 1.0) as consistency_score
    
FROM address_behavior_profiles_mv;
```

### 3. Address Relationships Materialized View

**Purpose**: Track and analyze relationships between addresses based on transfer patterns.

```sql
-- Address Relationships Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS address_relationships_mv
ENGINE = AggregatingMergeTree()
ORDER BY (asset, from_address, to_address)
POPULATE
AS
SELECT
    from_address,
    to_address,
    asset,
    
    -- Basic Relationship Metrics
    count() as transfer_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    min(amount) as min_amount,
    max(amount) as max_amount,
    stddevPop(amount) as amount_variance,
    
    -- Temporal Analysis
    min(block_timestamp) as first_transfer,
    max(block_timestamp) as last_transfer,
    max(block_timestamp) - min(block_timestamp) as relationship_duration,
    
    -- Frequency Analysis
    count() / ((max(block_timestamp) - min(block_timestamp)) / 86400 + 1) as avg_transfers_per_day,
    uniq(toDate(intDiv(block_timestamp, 1000))) as active_days,
    
    -- Fee Analysis
    sum(fee) as total_fees,
    avg(fee) as avg_fee,
    
    -- Pattern Detection
    countIf(amount = (
        SELECT mode(amount) 
        FROM balance_transfers 
        WHERE from_address = bt.from_address AND to_address = bt.to_address AND asset = bt.asset
    )) as repeated_amounts,
    
    -- Time Pattern Analysis
    uniq(toHour(intDiv(block_timestamp, 1000))) as unique_hours,
    mode(toHour(intDiv(block_timestamp, 1000))) as most_common_hour,
    
    -- Block Pattern Analysis
    avg(block_height) as avg_block_height,
    stddevPop(block_height) as block_variance,
    
    -- Regularity Metrics
    count() / uniq(toDate(intDiv(block_timestamp, 1000))) as transfers_per_active_day
    
FROM balance_transfers bt
WHERE from_address != to_address  -- Exclude self-transfers
GROUP BY from_address, to_address, asset
HAVING transfer_count >= 2;  -- Only relationships with multiple transfers
```

### 4. Transfer Pairs Analysis View

**Purpose**: Identify significant address pairs and classify their interaction patterns.

```sql
-- Transfer Pairs Analysis View
CREATE VIEW IF NOT EXISTS transfer_pairs_analysis_view AS
SELECT
    from_address,
    to_address,
    asset,
    transfer_count,
    total_amount,
    avg_amount,
    relationship_duration,
    avg_transfers_per_day,
    active_days,
    amount_variance,
    
    -- Relationship Strength Score (0-10 scale)
    least(
        (transfer_count * 0.2 + 
         log10(total_amount + 1) * 0.3 + 
         least(avg_transfers_per_day * 5, 5) * 0.3 +
         least(active_days / 30.0, 1) * 0.2) * 2,
        10.0
    ) as relationship_strength,
    
    -- Relationship Type Classification
    CASE
        WHEN transfer_count >= 100 AND avg_transfers_per_day >= 1 THEN 'High_Frequency'
        WHEN total_amount >= 100000 AND transfer_count >= 10 THEN 'High_Value'
        WHEN relationship_duration >= 2592000 AND transfer_count >= 20 THEN 'Long_Term'
        WHEN amount_variance < avg_amount * 0.1 AND transfer_count >= 5 THEN 'Regular_Pattern'
        WHEN repeated_amounts >= transfer_count * 0.8 THEN 'Fixed_Amount'
        WHEN unique_hours <= 3 AND transfer_count >= 10 THEN 'Time_Restricted'
        WHEN transfers_per_active_day >= 5 THEN 'Burst_Pattern'
        ELSE 'Casual'
    END as relationship_type,
    
    -- Bidirectional Analysis
    (SELECT count() FROM address_relationships_mv ar2 
     WHERE ar2.from_address = ar.to_address AND ar2.to_address = ar.from_address AND ar2.asset = ar.asset) as reverse_transfers,
    
    (SELECT total_amount FROM address_relationships_mv ar2 
     WHERE ar2.from_address = ar.to_address AND ar2.to_address = ar.from_address AND ar2.asset = ar.asset) as reverse_amount,
    
    CASE
        WHEN (SELECT count() FROM address_relationships_mv ar2 
              WHERE ar2.from_address = ar.to_address AND ar2.to_address = ar.from_address AND ar2.asset = ar.asset) > 0 
        THEN 'Bidirectional'
        ELSE 'Unidirectional'
    END as flow_direction,
    
    -- Balance Analysis for Bidirectional Relationships
    CASE
        WHEN reverse_transfers > 0 THEN
            CASE
                WHEN abs(total_amount - reverse_amount) / greatest(total_amount, reverse_amount) < 0.1 THEN 'Balanced'
                WHEN total_amount > reverse_amount * 2 THEN 'Heavily_Outbound'
                WHEN reverse_amount > total_amount * 2 THEN 'Heavily_Inbound'
                ELSE 'Moderately_Imbalanced'
            END
        ELSE 'Unidirectional'
    END as flow_balance,
    
    -- Regularity Score (0-1, higher = more regular)
    CASE
        WHEN relationship_duration > 0 THEN
            1.0 - (amount_variance / nullif(avg_amount, 0))
        ELSE 0
    END as regularity_score
    
FROM address_relationships_mv ar
WHERE relationship_strength >= 1.0  -- Filter for significant relationships
ORDER BY relationship_strength DESC;
```

### 5. Address Activity Patterns Materialized View

**Purpose**: Analyze temporal and behavioral patterns in address activity.

```sql
-- Address Activity Patterns Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS address_activity_patterns_mv
ENGINE = AggregatingMergeTree()
ORDER BY (asset, address, activity_date)
POPULATE
AS
SELECT
    address,
    asset,
    toDate(intDiv(block_timestamp, 1000)) as activity_date,
    
    -- Daily Activity Metrics
    count() as daily_transactions,
    sum(amount) as daily_volume,
    countIf(from_address = address) as daily_outgoing,
    countIf(to_address = address) as daily_incoming,
    
    -- Hourly Distribution Analysis
    groupArray(toHour(intDiv(block_timestamp, 1000))) as hourly_activity,
    uniq(toHour(intDiv(block_timestamp, 1000))) as active_hours,
    mode(toHour(intDiv(block_timestamp, 1000))) as peak_hour,
    
    -- Transaction Size Distribution
    countIf(amount < 100) as micro_transactions,
    countIf(amount >= 100 AND amount < 1000) as small_transactions,
    countIf(amount >= 1000 AND amount < 10000) as medium_transactions,
    countIf(amount >= 10000 AND amount < 100000) as large_transactions,
    countIf(amount >= 100000) as whale_transactions,
    
    -- Statistical Measures
    avg(amount) as daily_avg_amount,
    median(amount) as daily_median_amount,
    stddevPop(amount) as daily_amount_variance,
    
    -- Counterparty Analysis
    uniqIf(to_address, from_address = address) as daily_unique_recipients,
    uniqIf(from_address, to_address = address) as daily_unique_senders,
    uniq(multiIf(from_address = address, to_address, from_address)) as daily_unique_counterparties,
    
    -- Fee Patterns
    sumIf(fee, from_address = address) as daily_fees_paid,
    avgIf(fee, from_address = address) as avg_daily_fee,
    
    -- Behavioral Indicators
    countIf(from_address = address AND to_address IN (
        SELECT DISTINCT from_address FROM balance_transfers bt2 
        WHERE bt2.to_address = address AND toDate(intDiv(bt2.block_timestamp, 1000)) = toDate(intDiv(bt.block_timestamp, 1000))
    )) as daily_circular_transactions,
    
    -- Time Concentration
    entropy(groupArray(toHour(intDiv(block_timestamp, 1000)))) as hourly_entropy
    
FROM balance_transfers bt
GROUP BY address, asset, activity_date;
```

### 6. Address Clusters View

**Purpose**: Identify clusters of related addresses based on transaction patterns and shared counterparties.

```sql
-- Address Clusters View
CREATE VIEW IF NOT EXISTS address_clusters_view AS
WITH address_counterparties AS (
    SELECT 
        address,
        asset,
        groupArray(counterparty) as counterparties,
        count() as total_counterparties
    FROM (
        SELECT from_address as address, to_address as counterparty, asset FROM balance_transfers
        UNION ALL
        SELECT to_address as address, from_address as counterparty, asset FROM balance_transfers
    )
    GROUP BY address, asset
    HAVING total_counterparties >= 3  -- Minimum counterparties for clustering
),
cluster_candidates AS (
    SELECT 
        a1.address as address1,
        a2.address as address2,
        a1.asset,
        arrayIntersect(a1.counterparties, a2.counterparties) as common_counterparties,
        length(arrayIntersect(a1.counterparties, a2.counterparties)) as common_count,
        length(arrayIntersect(a1.counterparties, a2.counterparties)) / 
        (length(arrayUnion(a1.counterparties, a2.counterparties)) + 1) as jaccard_similarity,
        
        -- Additional similarity metrics
        a1.total_counterparties as addr1_counterparties,
        a2.total_counterparties as addr2_counterparties
    FROM address_counterparties a1
    JOIN address_counterparties a2 ON a1.asset = a2.asset AND a1.address < a2.address
    WHERE length(arrayIntersect(a1.counterparties, a2.counterparties)) >= 3
)
SELECT 
    address1,
    address2,
    asset,
    common_count,
    jaccard_similarity,
    common_counterparties,
    addr1_counterparties,
    addr2_counterparties,
    
    -- Cluster Strength Classification
    CASE
        WHEN jaccard_similarity >= 0.5 THEN 'Very_High_Similarity'
        WHEN jaccard_similarity >= 0.3 THEN 'High_Similarity'
        WHEN jaccard_similarity >= 0.2 THEN 'Medium_Similarity'
        WHEN jaccard_similarity >= 0.1 THEN 'Low_Similarity'
        ELSE 'Minimal_Similarity'
    END as cluster_strength,
    
    -- Overlap Ratio (common / smaller set)
    common_count / least(addr1_counterparties, addr2_counterparties) as overlap_ratio,
    
    -- Cluster Type Classification
    CASE
        WHEN common_count >= 20 AND jaccard_similarity >= 0.3 THEN 'Strong_Cluster'
        WHEN common_count >= 10 AND jaccard_similarity >= 0.2 THEN 'Moderate_Cluster'
        WHEN overlap_ratio >= 0.8 THEN 'Subset_Relationship'
        WHEN common_count >= 5 THEN 'Weak_Cluster'
        ELSE 'Potential_Cluster'
    END as cluster_type
    
FROM cluster_candidates
WHERE jaccard_similarity >= 0.1
ORDER BY jaccard_similarity DESC, common_count DESC;
```

### 7. Suspicious Activity Detection View

**Purpose**: Identify potentially suspicious or anomalous activity patterns.

```sql
-- Suspicious Activity Detection View
CREATE VIEW IF NOT EXISTS suspicious_activity_view AS
SELECT
    ac.address,
    ac.asset,
    ac.address_type,
    ac.risk_flag,
    abp.total_transactions,
    abp.total_volume,
    abp.avg_transactions_per_day,
    
    -- Suspicious Pattern Indicators
    CASE
        WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0
    END as has_risk_flag,
    
    CASE
        WHEN abp.circular_transactions >= abp.total_transactions * 0.5 THEN 1 ELSE 0
    END as high_circular_activity,
    
    CASE
        WHEN abp.night_transactions >= abp.total_transactions * 0.8 THEN 1 ELSE 0
    END as unusual_time_pattern,
    
    CASE
        WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0
    END as fixed_amount_pattern,
    
    CASE
        WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0
    END as single_recipient_pattern,
    
    CASE
        WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0
    END as large_infrequent_pattern,
    
    -- Composite Suspicion Score (0-6)
    (CASE WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0 END +
     CASE WHEN abp.circular_transactions >= abp.total_transactions * 0.5 THEN 1 ELSE 0 END +
     CASE WHEN abp.night_transactions >= abp.total_transactions * 0.8 THEN 1 ELSE 0 END +
     CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
     CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
     CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) as suspicion_score,
    
    -- Risk Level
    CASE
        WHEN (CASE WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0 END +
              CASE WHEN abp.circular_transactions >= abp.total_transactions * 0.5 THEN 1 ELSE 0 END +
              CASE WHEN abp.night_transactions >= abp.total_transactions * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 4 THEN 'High'
        WHEN (CASE WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0 END +
              CASE WHEN abp.circular_transactions >= abp.total_transactions * 0.5 THEN 1 ELSE 0 END +
              CASE WHEN abp.night_transactions >= abp.total_transactions * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 2 THEN 'Medium'
        WHEN (CASE WHEN ac.risk_flag != 'Normal' THEN 1 ELSE 0 END +
              CASE WHEN abp.circular_transactions >= abp.total_transactions * 0.5 THEN 1 ELSE 0 END +
              CASE WHEN abp.night_transactions >= abp.total_transactions * 0.8 THEN 1 ELSE 0 END +
              CASE WHEN abp.sent_amount_variance < abp.avg_sent_amount * 0.05 AND abp.total_transactions >= 20 THEN 1 ELSE 0 END +
              CASE WHEN abp.unique_recipients = 1 AND abp.total_transactions >= 50 THEN 1 ELSE 0 END +
              CASE WHEN abp.whale_transactions > 0 AND abp.total_transactions <= 5 THEN 1 ELSE 0 END) >= 1 THEN 'Low'
        ELSE 'Normal'
    END as risk_level
    
FROM address_classification_view ac
JOIN address_behavior_profiles_mv abp ON ac.address = abp.address AND ac.asset = abp.asset
WHERE abp.total_transactions >= 5  -- Minimum transactions for analysis
ORDER BY suspicion_score DESC, abp.total_volume DESC;
```

### 8. Enhanced Indexes for Address Analytics

```sql
-- Additional indexes for enhanced address analytics performance

-- Indexes for address behavior profiles
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_address_timestamp (from_address, block_timestamp) TYPE minmax GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_to_address_timestamp (to_address, block_timestamp) TYPE minmax GRANULARITY 4;

-- Indexes for amount-based queries
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_amount_range amount TYPE minmax GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_large_amounts amount TYPE bloom_filter(0.01) GRANULARITY 4 WHERE amount >= 10000;

-- Indexes for time-based analysis
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_hour_of_day toHour(intDiv(block_timestamp, 1000)) TYPE set(24) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_date toDate(intDiv(block_timestamp, 1000)) TYPE minmax GRANULARITY 4;

-- Composite indexes for relationship analysis
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_from_to_asset (from_address, to_address, asset) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers ADD INDEX IF NOT EXISTS idx_asset_amount_timestamp (asset, amount, block_timestamp) TYPE minmax GRANULARITY 4;
```

## Implementation Benefits

### 1. **Comprehensive Address Profiling**
- Detailed behavioral analysis for each address
- Multi-dimensional classification system
- Risk assessment and anomaly detection

### 2. **Relationship Intelligence**
- Deep analysis of address-to-address relationships
- Pattern recognition for transaction flows
- Cluster identification for related addresses

### 3. **Temporal Pattern Analysis**
- Time-based activity patterns
- Seasonal and cyclical behavior detection
- Activity concentration analysis

### 4. **Enhanced Security**
- Suspicious activity detection
- Risk scoring and classification
- Compliance-ready analytics

### 5. **Performance Optimization**
- Materialized views for fast analytics
- Strategic indexing for common queries
- Efficient aggregation patterns

## Usage Examples

### Query Address Behavior Profile
```sql
SELECT * FROM address_behavior_profiles_mv 
WHERE address = 'your_address_here' AND asset = 'TOR';
```

### Find High-Risk Addresses
```sql
SELECT address, risk_level, suspicion_score 
FROM suspicious_activity_view 
WHERE risk_level IN ('High', 'Medium') 
ORDER BY suspicion_score DESC;
```

### Analyze Address Relationships
```sql
SELECT * FROM transfer_pairs_analysis_view 
WHERE (from_address = 'address1' OR to_address = 'address1') 
AND relationship_strength >= 5.0;
```

### Find Address Clusters
```sql
SELECT * FROM address_clusters_view 
WHERE (address1 = 'your_address' OR address2 = 'your_address') 
AND cluster_strength IN ('High_Similarity', 'Very_High_Similarity');
```

## Performance Considerations

1. **Materialized Views**: Use `POPULATE` for initial data loading
2. **Partitioning**: Align with existing partition strategy
3. **Memory Usage**: Monitor memory consumption for large aggregations
4. **Update Frequency**: Consider refresh intervals for materialized views
5. **Query Optimization**: Use appropriate indexes for common access patterns

## Migration Strategy

1. **Phase 1**: Deploy materialized views with `POPULATE`
2. **Phase 2**: Add classification and analysis views
3. **Phase 3**: Implement additional indexes
4. **Phase 4**: Add suspicious activity detection
5. **Phase 5**: Performance tuning and optimization

This enhanced schema provides a comprehensive foundation for advanced address analytics while maintaining compatibility with the existing system architecture.