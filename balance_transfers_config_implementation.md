# Balance Transfers Configuration Implementation Guide

## SQL Implementation Files

This document contains all the SQL code needed to implement the modular configurable thresholds system for balance transfers.

## 1. Configuration Tables Creation

### 1.1 Transaction Size Configuration Table

```sql
-- Configuration for transaction size buckets used in behavior profiles
CREATE TABLE IF NOT EXISTS balance_transfers_transaction_size_config (
    network String,
    asset String,
    
    -- Transaction Size Thresholds for Behavior Profiles View
    micro_transaction_max Decimal128(18) DEFAULT 10,
    small_transaction_min Decimal128(18) DEFAULT 10,
    small_transaction_max Decimal128(18) DEFAULT 100,
    medium_transaction_min Decimal128(18) DEFAULT 100,
    medium_transaction_max Decimal128(18) DEFAULT 1000,
    large_transaction_min Decimal128(18) DEFAULT 1000,
    large_transaction_max Decimal128(18) DEFAULT 10000,
    whale_transaction_min Decimal128(18) DEFAULT 10000,
    
    -- Alternative thresholds for Activity Patterns View (different scale)
    activity_micro_max Decimal128(18) DEFAULT 100,
    activity_small_min Decimal128(18) DEFAULT 100,
    activity_small_max Decimal128(18) DEFAULT 1000,
    activity_medium_min Decimal128(18) DEFAULT 1000,
    activity_medium_max Decimal128(18) DEFAULT 10000,
    activity_large_min Decimal128(18) DEFAULT 10000,
    activity_large_max Decimal128(18) DEFAULT 100000,
    activity_whale_min Decimal128(18) DEFAULT 100000,
    
    -- Metadata
    is_default Bool DEFAULT false,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
SETTINGS index_granularity = 8192
COMMENT 'Transaction size classification thresholds per network and asset';

-- Add indexes for efficient lookups
ALTER TABLE balance_transfers_transaction_size_config ADD INDEX IF NOT EXISTS idx_network_asset (network, asset) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers_transaction_size_config ADD INDEX IF NOT EXISTS idx_is_default is_default TYPE set(2) GRANULARITY 4;
```

### 1.2 Address Classification Configuration Table

```sql
-- Configuration for address type classification
CREATE TABLE IF NOT EXISTS balance_transfers_address_classification_config (
    network String,
    asset String,
    
    -- Exchange Classification Thresholds
    exchange_volume_min Decimal128(18) DEFAULT 1000000,
    exchange_recipients_min UInt32 DEFAULT 100,
    
    -- Whale Classification Thresholds
    whale_volume_min Decimal128(18) DEFAULT 1000000,
    whale_recipients_max UInt32 DEFAULT 10,
    
    -- High Volume Trader Thresholds
    high_volume_trader_volume_min Decimal128(18) DEFAULT 100000,
    high_volume_trader_transactions_min UInt32 DEFAULT 1000,
    
    -- Hub Address Thresholds
    hub_recipients_min UInt32 DEFAULT 50,
    hub_senders_min UInt32 DEFAULT 50,
    
    -- Retail Active Thresholds
    retail_active_transactions_min UInt32 DEFAULT 100,
    retail_active_volume_max Decimal128(18) DEFAULT 10000,
    
    -- Whale Inactive Thresholds
    whale_inactive_transactions_max UInt32 DEFAULT 10,
    whale_inactive_volume_min Decimal128(18) DEFAULT 50000,
    
    -- Retail Inactive Thresholds
    retail_inactive_transactions_max UInt32 DEFAULT 10,
    retail_inactive_volume_max Decimal128(18) DEFAULT 1000,
    
    -- Volume Tier Classification Thresholds
    ultra_high_volume_min Decimal128(18) DEFAULT 100000,
    high_volume_min Decimal128(18) DEFAULT 10000,
    medium_volume_min Decimal128(18) DEFAULT 1000,
    low_volume_min Decimal128(18) DEFAULT 100,
    
    -- Activity Level Thresholds (transactions per day)
    very_active_transactions_per_day Float64 DEFAULT 10,
    active_transactions_per_day Float64 DEFAULT 1,
    moderate_transactions_per_day Float64 DEFAULT 0.1,
    
    -- Diversification Score Parameters
    diversification_max_counterparties UInt32 DEFAULT 100,
    
    -- Metadata
    is_default Bool DEFAULT false,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
SETTINGS index_granularity = 8192
COMMENT 'Address classification thresholds per network and asset';

-- Add indexes for efficient lookups
ALTER TABLE balance_transfers_address_classification_config ADD INDEX IF NOT EXISTS idx_network_asset (network, asset) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers_address_classification_config ADD INDEX IF NOT EXISTS idx_is_default is_default TYPE set(2) GRANULARITY 4;
```

### 1.3 Risk Assessment Configuration Table

```sql
-- Configuration for suspicious activity detection
CREATE TABLE IF NOT EXISTS balance_transfers_risk_assessment_config (
    network String,
    asset String,
    
    -- Potential Mixer Detection Thresholds
    mixer_unique_recipients_max UInt32 DEFAULT 1,
    mixer_transactions_min UInt32 DEFAULT 100,
    
    -- Potential Tumbler Detection Thresholds
    tumbler_variance_threshold Float64 DEFAULT 0.05,
    tumbler_transactions_min UInt32 DEFAULT 50,
    
    -- Unusual Hours Pattern Thresholds
    unusual_hours_night_ratio_threshold Float64 DEFAULT 0.8,
    
    -- Large Infrequent Pattern Thresholds
    large_infrequent_transactions_max UInt32 DEFAULT 20,
    large_infrequent_whale_min UInt32 DEFAULT 1,
    
    -- Fixed Amount Pattern Thresholds
    fixed_pattern_variance_threshold Float64 DEFAULT 0.1,
    fixed_pattern_transactions_min UInt32 DEFAULT 20,
    
    -- Circular Transaction Detection Thresholds
    circular_transactions_threshold UInt32 DEFAULT 10,
    
    -- Single Recipient Pattern Thresholds
    single_recipient_transactions_min UInt32 DEFAULT 50,
    
    -- Analysis Minimum Requirements
    min_transactions_for_risk_analysis UInt32 DEFAULT 5,
    
    -- Suspicion Score Classification Thresholds
    high_risk_score_threshold UInt32 DEFAULT 4,
    medium_risk_score_threshold UInt32 DEFAULT 2,
    low_risk_score_threshold UInt32 DEFAULT 1,
    
    -- Metadata
    is_default Bool DEFAULT false,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
SETTINGS index_granularity = 8192
COMMENT 'Risk assessment thresholds per network and asset';

-- Add indexes for efficient lookups
ALTER TABLE balance_transfers_risk_assessment_config ADD INDEX IF NOT EXISTS idx_network_asset (network, asset) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers_risk_assessment_config ADD INDEX IF NOT EXISTS idx_is_default is_default TYPE set(2) GRANULARITY 4;
```

### 1.4 Relationship Analysis Configuration Table

```sql
-- Configuration for address relationship analysis
CREATE TABLE IF NOT EXISTS balance_transfers_relationship_config (
    network String,
    asset String,
    
    -- Relationship Type Classification Thresholds
    high_value_amount_min Decimal128(18) DEFAULT 10000,
    high_value_count_min UInt32 DEFAULT 10,
    high_frequency_count_min UInt32 DEFAULT 100,
    regular_relationship_count_min UInt32 DEFAULT 5,
    casual_relationship_count_min UInt32 DEFAULT 2,
    
    -- Relationship Strength Calculation Parameters
    relationship_strength_count_weight Float64 DEFAULT 0.4,
    relationship_strength_amount_weight Float64 DEFAULT 0.6,
    relationship_strength_multiplier Float64 DEFAULT 2.0,
    relationship_strength_max Float64 DEFAULT 10.0,
    
    -- Significant Relationship Filter Thresholds
    significant_relationship_count_min UInt32 DEFAULT 2,
    
    -- Metadata
    is_default Bool DEFAULT false,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
SETTINGS index_granularity = 8192
COMMENT 'Relationship analysis thresholds per network and asset';

-- Add indexes for efficient lookups
ALTER TABLE balance_transfers_relationship_config ADD INDEX IF NOT EXISTS idx_network_asset (network, asset) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers_relationship_config ADD INDEX IF NOT EXISTS idx_is_default is_default TYPE set(2) GRANULARITY 4;
```

### 1.5 Behavior Analysis Configuration Table

```sql
-- Configuration for general behavior analysis parameters
CREATE TABLE IF NOT EXISTS balance_transfers_behavior_config (
    network String,
    asset String,
    
    -- Minimum Requirements for Analysis
    min_transactions_for_behavior_analysis UInt32 DEFAULT 1,
    min_transactions_for_classification UInt32 DEFAULT 5,
    
    -- Time-based Analysis Thresholds (in seconds)
    activity_span_single_day_max UInt64 DEFAULT 86400,      -- 1 day
    activity_span_weekly_max UInt64 DEFAULT 604800,        -- 1 week
    activity_span_monthly_max UInt64 DEFAULT 2592000,      -- 30 days
    activity_span_yearly_max UInt64 DEFAULT 31536000,      -- 365 days
    
    -- Statistical Analysis Parameters
    consistency_score_variance_threshold Float64 DEFAULT 1.0,
    
    -- Metadata
    is_default Bool DEFAULT false,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
SETTINGS index_granularity = 8192
COMMENT 'General behavior analysis parameters per network and asset';

-- Add indexes for efficient lookups
ALTER TABLE balance_transfers_behavior_config ADD INDEX IF NOT EXISTS idx_network_asset (network, asset) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE balance_transfers_behavior_config ADD INDEX IF NOT EXISTS idx_is_default is_default TYPE set(2) GRANULARITY 4;
```

## 2. Default Configuration Data

### 2.1 Torus Network (TOR) Default Configurations

```sql
-- Torus Transaction Size Configuration
INSERT INTO balance_transfers_transaction_size_config VALUES
('torus', 'TOR', 
 10, 10, 100, 100, 1000, 1000, 10000, 10000,           -- Behavior profile thresholds
 100, 100, 1000, 1000, 10000, 10000, 100000, 100000,  -- Activity pattern thresholds
 true, now(), now(), 1);

-- Torus Address Classification Configuration
INSERT INTO balance_transfers_address_classification_config VALUES
('torus', 'TOR',
 1000000, 100,      -- Exchange thresholds
 1000000, 10,       -- Whale thresholds
 100000, 1000,      -- High volume trader thresholds
 50, 50,            -- Hub address thresholds
 100, 10000,        -- Retail active thresholds
 10, 50000,         -- Whale inactive thresholds
 10, 1000,          -- Retail inactive thresholds
 100000, 10000, 1000, 100,  -- Volume tier thresholds
 10, 1, 0.1,        -- Activity level thresholds
 100,               -- Diversification parameter
 true, now(), now(), 1);

-- Torus Risk Assessment Configuration
INSERT INTO balance_transfers_risk_assessment_config VALUES
('torus', 'TOR',
 1, 100,            -- Mixer detection thresholds
 0.05, 50,          -- Tumbler detection thresholds
 0.8,               -- Unusual hours threshold
 20, 1,             -- Large infrequent thresholds
 0.1, 20,           -- Fixed pattern thresholds
 10,                -- Circular transactions threshold
 50,                -- Single recipient threshold
 5,                 -- Min transactions for risk analysis
 4, 2, 1,           -- Risk score thresholds
 true, now(), now(), 1);

-- Torus Relationship Configuration
INSERT INTO balance_transfers_relationship_config VALUES
('torus', 'TOR',
 10000, 10, 100, 5, 2,      -- Relationship type thresholds
 0.4, 0.6, 2.0, 10.0,      -- Strength calculation parameters
 2,                         -- Significant relationship threshold
 true, now(), now(), 1);

-- Torus Behavior Configuration
INSERT INTO balance_transfers_behavior_config VALUES
('torus', 'TOR',
 1, 5,                      -- Min transactions for analysis
 86400, 604800, 2592000, 31536000,  -- Time span thresholds
 1.0,                       -- Consistency score threshold
 true, now(), now(), 1);
```

### 2.2 Bittensor Network (TAO) Default Configurations

```sql
-- Bittensor Transaction Size Configuration (adjusted for TAO economics)
INSERT INTO balance_transfers_transaction_size_config VALUES
('bittensor', 'TAO',
 1, 1, 10, 10, 100, 100, 1000, 1000,           -- Behavior profile thresholds
 10, 10, 100, 100, 1000, 1000, 10000, 10000,  -- Activity pattern thresholds
 true, now(), now(), 1);

-- Bittensor Address Classification Configuration
INSERT INTO balance_transfers_address_classification_config VALUES
('bittensor', 'TAO',
 100000, 100,       -- Exchange thresholds
 100000, 10,        -- Whale thresholds
 10000, 1000,       -- High volume trader thresholds
 50, 50,            -- Hub address thresholds
 100, 1000,         -- Retail active thresholds
 10, 5000,          -- Whale inactive thresholds
 10, 100,           -- Retail inactive thresholds
 10000, 1000, 100, 10,  -- Volume tier thresholds
 10, 1, 0.1,        -- Activity level thresholds
 100,               -- Diversification parameter
 true, now(), now(), 1);

-- Bittensor Risk Assessment Configuration
INSERT INTO balance_transfers_risk_assessment_config VALUES
('bittensor', 'TAO',
 1, 100,            -- Mixer detection thresholds
 0.05, 50,          -- Tumbler detection thresholds
 0.8,               -- Unusual hours threshold
 20, 1,             -- Large infrequent thresholds
 0.1, 20,           -- Fixed pattern thresholds
 10,                -- Circular transactions threshold
 50,                -- Single recipient threshold
 5,                 -- Min transactions for risk analysis
 4, 2, 1,           -- Risk score thresholds
 true, now(), now(), 1);

-- Bittensor Relationship Configuration
INSERT INTO balance_transfers_relationship_config VALUES
('bittensor', 'TAO',
 1000, 10, 100, 5, 2,       -- Relationship type thresholds
 0.4, 0.6, 2.0, 10.0,      -- Strength calculation parameters
 2,                         -- Significant relationship threshold
 true, now(), now(), 1);

-- Bittensor Behavior Configuration
INSERT INTO balance_transfers_behavior_config VALUES
('bittensor', 'TAO',
 1, 5,                      -- Min transactions for analysis
 86400, 604800, 2592000, 31536000,  -- Time span thresholds
 1.0,                       -- Consistency score threshold
 true, now(), now(), 1);
```

### 2.3 Polkadot Network (DOT) Default Configurations

```sql
-- Polkadot Transaction Size Configuration
INSERT INTO balance_transfers_transaction_size_config VALUES
('polkadot', 'DOT',
 10, 10, 100, 100, 1000, 1000, 10000, 10000,           -- Behavior profile thresholds
 100, 100, 1000, 1000, 10000, 10000, 100000, 100000,  -- Activity pattern thresholds
 true, now(), now(), 1);

-- Polkadot Address Classification Configuration
INSERT INTO balance_transfers_address_classification_config VALUES
('polkadot', 'DOT',
 1000000, 100,      -- Exchange thresholds
 1000000, 10,       -- Whale thresholds
 100000, 1000,      -- High volume trader thresholds
 50, 50,            -- Hub address thresholds
 100, 10000,        -- Retail active thresholds
 10, 50000,         -- Whale inactive thresholds
 10, 1000,          -- Retail inactive thresholds
 100000, 10000, 1000, 100,  -- Volume tier thresholds
 10, 1, 0.1,        -- Activity level thresholds
 100,               -- Diversification parameter
 true, now(), now(), 1);

-- Polkadot Risk Assessment Configuration
INSERT INTO balance_transfers_risk_assessment_config VALUES
('polkadot', 'DOT',
 1, 100,            -- Mixer detection thresholds
 0.05, 50,          -- Tumbler detection thresholds
 0.8,               -- Unusual hours threshold
 20, 1,             -- Large infrequent thresholds
 0.1, 20,           -- Fixed pattern thresholds
 10,                -- Circular transactions threshold
 50,                -- Single recipient threshold
 5,                 -- Min transactions for risk analysis
 4, 2, 1,           -- Risk score thresholds
 true, now(), now(), 1);

-- Polkadot Relationship Configuration
INSERT INTO balance_transfers_relationship_config VALUES
('polkadot', 'DOT',
 10000, 10, 100, 5, 2,      -- Relationship type thresholds
 0.4, 0.6, 2.0, 10.0,      -- Strength calculation parameters
 2,                         -- Significant relationship threshold
 true, now(), now(), 1);

-- Polkadot Behavior Configuration
INSERT INTO balance_transfers_behavior_config VALUES
('polkadot', 'DOT',
 1, 5,                      -- Min transactions for analysis
 86400, 604800, 2592000, 31536000,  -- Time span thresholds
 1.0,                       -- Consistency score threshold
 true, now(), now(), 1);
```

## 3. Helper Functions

### 3.1 Network-Asset Mapping Function

```sql
-- Helper function to determine network from asset
CREATE OR REPLACE FUNCTION getNetworkFromAsset(asset_param String) -> String AS
    CASE asset_param
        WHEN 'TOR' THEN 'torus'
        WHEN 'TAO' THEN 'bittensor'
        WHEN 'DOT' THEN 'polkadot'
        ELSE 'unknown'
    END;
```

### 3.2 Configuration Lookup Functions

```sql
-- Get transaction size configuration with fallback
CREATE OR REPLACE FUNCTION getTransactionSizeThreshold(network_param String, asset_param String, threshold_name String) -> Decimal128(18) AS
    COALESCE(
        -- Try asset-specific configuration first
        (SELECT 
            CASE threshold_name
                WHEN 'micro_max' THEN micro_transaction_max
                WHEN 'small_min' THEN small_transaction_min
                WHEN 'small_max' THEN small_transaction_max
                WHEN 'medium_min' THEN medium_transaction_min
                WHEN 'medium_max' THEN medium_transaction_max
                WHEN 'large_min' THEN large_transaction_min
                WHEN 'large_max' THEN large_transaction_max
                WHEN 'whale_min' THEN whale_transaction_min
                WHEN 'activity_micro_max' THEN activity_micro_max
                WHEN 'activity_small_min' THEN activity_small_min
                WHEN 'activity_small_max' THEN activity_small_max
                WHEN 'activity_medium_min' THEN activity_medium_min
                WHEN 'activity_medium_max' THEN activity_medium_max
                WHEN 'activity_large_min' THEN activity_large_min
                WHEN 'activity_large_max' THEN activity_large_max
                WHEN 'activity_whale_min' THEN activity_whale_min
                ELSE 0
            END
         FROM balance_transfers_transaction_size_config 
         WHERE network = network_param AND asset = asset_param),
        -- Fallback to network default
        (SELECT 
            CASE threshold_name
                WHEN 'micro_max' THEN micro_transaction_max
                WHEN 'small_min' THEN small_transaction_min
                WHEN 'small_max' THEN small_transaction_max
                WHEN 'medium_min' THEN medium_transaction_min
                WHEN 'medium_max' THEN medium_transaction_max
                WHEN 'large_min' THEN large_transaction_min
                WHEN 'large_max' THEN large_transaction_max
                WHEN 'whale_min' THEN whale_transaction_min
                WHEN 'activity_micro_max' THEN activity_micro_max
                WHEN 'activity_small_min' THEN activity_small_min
                WHEN 'activity_small_max' THEN activity_small_max
                WHEN 'activity_medium_min' THEN activity_medium_min
                WHEN 'activity_medium_max' THEN activity_medium_max
                WHEN 'activity_large_min' THEN activity_large_min
                WHEN 'activity_large_max' THEN activity_large_max
                WHEN 'activity_whale_min' THEN activity_whale_min
                ELSE 0
            END
         FROM balance_transfers_transaction_size_config 
         WHERE network = network_param AND is_default = true
         LIMIT 1)
    );

-- Get address classification configuration with fallback
CREATE OR REPLACE FUNCTION getAddressClassificationThreshold(network_param String, asset_param String, threshold_name String) -> Decimal128(18) AS
    COALESCE(
        -- Try asset-specific configuration first
        (SELECT 
            CASE threshold_name
                WHEN 'exchange_volume_min' THEN exchange_volume_min
                WHEN 'whale_volume_min' THEN whale_volume_min
                WHEN 'high_volume_trader_volume_min' THEN high_volume_trader_volume_min
                WHEN 'retail_active_volume_max' THEN retail_active_volume_max
                WHEN 'whale_inactive_volume_min' THEN whale_inactive_volume_min
                WHEN 'retail_inactive_volume_max' THEN retail_inactive_volume_max
                WHEN 'ultra_high_volume_min' THEN ultra_high_volume_min
                WHEN 'high_volume_min' THEN high_volume_min
                WHEN 'medium_volume_min' THEN medium_volume_min
                WHEN 'low_volume_min' THEN low_volume_min
                ELSE 0
            END
         FROM balance_transfers_address_classification_config 
         WHERE network = network_param AND asset = asset_param),
        -- Fallback to network default
        (SELECT 
            CASE threshold_name
                WHEN 'exchange_volume_min' THEN exchange_volume_min
                WHEN 'whale_volume_min' THEN whale_volume_min
                WHEN 'high_volume_trader_volume_min' THEN high_volume_trader_volume_min
                WHEN 'retail_active_volume_max' THEN retail_active_volume_max
                WHEN 'whale_inactive_volume_min' THEN whale_inactive_volume_min
                WHEN 'retail_inactive_volume_max' THEN retail_inactive_volume_max
                WHEN 'ultra_high_volume_min' THEN ultra_high_volume_min
                WHEN 'high_volume_min' THEN high_volume_min
                WHEN 'medium_volume_min' THEN medium_volume_min
                WHEN 'low_volume_min' THEN low_volume_min
                ELSE 0
            END
         FROM balance_transfers_address_classification_config 
         WHERE network = network_param AND is_default = true
         LIMIT 1)
    );

-- Similar functions for other configuration types...
-- (Additional functions for risk assessment, relationship, and behavior configs)
```

## 4. Configuration Management Views

### 4.1 Configuration Summary View

```sql
-- Comprehensive view of all configurations
CREATE VIEW IF NOT EXISTS balance_transfers_config_summary AS
SELECT 
    'transaction_size' as config_type,
    network,
    asset,
    is_default,
    toString(micro_transaction_max) as micro_threshold,
    toString(whale_transaction_min) as whale_threshold,
    created_at,
    updated_at
FROM balance_transfers_transaction_size_config
UNION ALL
SELECT 
    'address_classification' as config_type,
    network,
    asset,
    is_default,
    toString(exchange_volume_min) as micro_threshold,
    toString(whale_volume_min) as whale_threshold,
    created_at,
    updated_at
FROM balance_transfers_address_classification_config
UNION ALL
SELECT 
    'risk_assessment' as config_type,
    network,
    asset,
    is_default,
    toString(mixer_transactions_min) as micro_threshold,
    toString(tumbler_variance_threshold) as whale_threshold,
    created_at,
    updated_at
FROM balance_transfers_risk_assessment_config
UNION ALL
SELECT 
    'relationship' as config_type,
    network,
    asset,
    is_default,
    toString(high_value_amount_min) as micro_threshold,
    toString(high_frequency_count_min) as whale_threshold,
    created_at,
    updated_at
FROM balance_transfers_relationship_config
UNION ALL
SELECT 
    'behavior' as config_type,
    network,
    asset,
    is_default,
    toString(min_transactions_for_behavior_analysis) as micro_threshold,
    toString(activity_span_single_day_max) as whale_threshold,
    created_at,
    updated_at
FROM balance_transfers_behavior_config
ORDER BY config_type, network, asset;
```

### 4.2 Configuration Validation View

```sql
-- Validate configuration consistency
CREATE VIEW IF NOT EXISTS balance_transfers_config_validation AS
SELECT 
    network,
    asset,
    'transaction_size' as config_type,
    CASE 
        WHEN micro_transaction_max <= small_transaction_min THEN 'OK'
        ELSE 'ERROR: micro_max > small_min'
    END as validation_1,
    CASE
        WHEN small_transaction_max <= medium_transaction_min THEN 'OK'
        ELSE 'ERROR: small_max > medium_min'
    END as validation_2,
    CASE
        WHEN medium_transaction_max <= large_transaction_min THEN 'OK'
        ELSE 'ERROR: medium_max > large_min'
    END as validation_3,
    CASE
        WHEN large_transaction_max <= whale_transaction_min THEN 'OK'
        ELSE 'ERROR: large_max > whale_min'
    END as validation_4
FROM balance_transfers_transaction_size_config
UNION ALL
SELECT 
    network,
    asset,
    'address_classification' as config_type,
    CASE 
        WHEN low_volume_min <= medium_volume_min THEN 'OK'
        ELSE 'ERROR: low_min > medium_min'
    END as validation_1,
    CASE
        WHEN medium_volume_min <= high_volume_min THEN 'OK'
        ELSE 'ERROR: medium_min > high_min'
    END as validation_2,
    CASE
        WHEN high_volume_min <= ultra_high_volume_min THEN 'OK'
        ELSE 'ERROR: high_min > ultra_high_min'
    END as validation_3,
    'N/A' as validation_4
FROM balance_transfers_address_classification_config;
```

## 5. Updated Views Implementation

### 5.1 Updated Address Behavior Profiles View

```sql
-- Updated view using configurable thresholds
CREATE OR REPLACE VIEW balance_transfers_address_behavior_profiles_view AS
SELECT
    address,
    asset,
    
    -- Basic Activity Metrics
    count() as total_transactions,
    countIf(address = from_address) as outgoing_count,
    countIf(address = to_address) as incoming_count,
    
    -- Volume Metrics
    sumIf(amount, address = from_address) as total_sent,
    sumIf(amount, address = to_address) as total_received,
    sum(amount) as total_volume,
    
    -- Statistical Metrics
    avgIf(amount, address = from_address) as avg_sent_amount,
    avgIf(amount, address = to_address) as avg_received_amount,
    quantileIf(0.5)(amount, address = from_address) as median_sent_amount,
    quantileIf(0.5)(amount, address = to_address) as median_received_amount,
    quantileIf(0.9)(amount, address = from_address) as p90_sent_amount,
    quantileIf(0.9)(amount, address = to_address) as p90_received_amount,
    
    -- Temporal Patterns
    min(block_timestamp) as first_activity,
    max(block_timestamp) as last_activity,
    max(block_timestamp) - min(block_timestamp) as activity_span_seconds,
    count() / ((max(block_timestamp) - min(block_timestamp)) / 86400 + 1) as avg_transactions_per_day,
    
    -- Unique Counterparties
    uniqIf(to_address, address = from_address) as unique_recipients,
    uniqIf(from_address, address = to_address) as unique_senders,
    uniq(multiIf(address = from_address, to_address, from_address)) as total_unique_counterparties,
    
    -- Fee Analysis
    sumIf(fee, address = from_address) as total_fees_paid,
    avgIf(fee, address = from_address) as avg_fee_paid,
    maxIf(fee, address = from_address) as max_fee_paid,
    
    -- Activity Distribution by Time of Day
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 0 AND 5) as night_transactions,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 6 AND 11) as morning_transactions,
    countIf(toHour(toDateTime(intDiv