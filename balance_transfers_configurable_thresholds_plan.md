# Balance Transfers Configurable Thresholds Implementation Plan

## Overview

This plan implements configurable thresholds for balance transfer analysis by creating separate configuration tables for each view/functionality area. This modular approach provides better organization, readability, and maintainability.

## Current Hardcoded Thresholds Analysis

### Views with Hardcoded Thresholds:
1. **balance_transfers_address_behavior_profiles_view** - Transaction size classification
2. **balance_transfers_address_classification_view** - Address type classification, volume tiers, activity levels
3. **balance_transfers_pairs_analysis_view** - Relationship analysis thresholds
4. **balance_transfers_address_activity_patterns_view** - Daily activity thresholds
5. **balance_transfers_suspicious_activity_view** - Risk assessment thresholds

## Modular Configuration Tables Design

### 1. Transaction Size Classification Configuration

```sql
-- Configuration for transaction size buckets used in behavior profiles
CREATE TABLE IF NOT EXISTS balance_transfers_transaction_size_config (
    network String,
    asset String,
    
    -- Transaction Size Thresholds
    micro_transaction_max Decimal128(18) DEFAULT 10,
    small_transaction_min Decimal128(18) DEFAULT 10,
    small_transaction_max Decimal128(18) DEFAULT 100,
    medium_transaction_min Decimal128(18) DEFAULT 100,
    medium_transaction_max Decimal128(18) DEFAULT 1000,
    large_transaction_min Decimal128(18) DEFAULT 1000,
    large_transaction_max Decimal128(18) DEFAULT 10000,
    whale_transaction_min Decimal128(18) DEFAULT 10000,
    
    -- Alternative thresholds for activity patterns view (different scale)
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
COMMENT 'Transaction size classification thresholds per network and asset';
```

### 2. Address Classification Configuration

```sql
-- Configuration for address type classification
CREATE TABLE IF NOT EXISTS balance_transfers_address_classification_config (
    network String,
    asset String,
    
    -- Exchange Classification
    exchange_volume_min Decimal128(18) DEFAULT 1000000,
    exchange_recipients_min UInt32 DEFAULT 100,
    
    -- Whale Classification  
    whale_volume_min Decimal128(18) DEFAULT 1000000,
    whale_recipients_max UInt32 DEFAULT 10,
    
    -- High Volume Trader
    high_volume_trader_volume_min Decimal128(18) DEFAULT 100000,
    high_volume_trader_transactions_min UInt32 DEFAULT 1000,
    
    -- Hub Address
    hub_recipients_min UInt32 DEFAULT 50,
    hub_senders_min UInt32 DEFAULT 50,
    
    -- Retail Active
    retail_active_transactions_min UInt32 DEFAULT 100,
    retail_active_volume_max Decimal128(18) DEFAULT 10000,
    
    -- Whale Inactive
    whale_inactive_transactions_max UInt32 DEFAULT 10,
    whale_inactive_volume_min Decimal128(18) DEFAULT 50000,
    
    -- Retail Inactive
    retail_inactive_transactions_max UInt32 DEFAULT 10,
    retail_inactive_volume_max Decimal128(18) DEFAULT 1000,
    
    -- Volume Tier Thresholds
    ultra_high_volume_min Decimal128(18) DEFAULT 100000,
    high_volume_min Decimal128(18) DEFAULT 10000,
    medium_volume_min Decimal128(18) DEFAULT 1000,
    low_volume_min Decimal128(18) DEFAULT 100,
    
    -- Activity Level Thresholds (transactions per day)
    very_active_transactions_per_day Float64 DEFAULT 10,
    active_transactions_per_day Float64 DEFAULT 1,
    moderate_transactions_per_day Float64 DEFAULT 0.1,
    
    -- Diversification Score Threshold
    diversification_max_counterparties UInt32 DEFAULT 100,
    
    -- Metadata
    is_default Bool DEFAULT false,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
COMMENT 'Address classification thresholds per network and asset';
```

### 3. Risk Assessment Configuration

```sql
-- Configuration for suspicious activity detection
CREATE TABLE IF NOT EXISTS balance_transfers_risk_assessment_config (
    network String,
    asset String,
    
    -- Potential Mixer Detection
    mixer_unique_recipients_max UInt32 DEFAULT 1,
    mixer_transactions_min UInt32 DEFAULT 100,
    
    -- Potential Tumbler Detection
    tumbler_variance_threshold Float64 DEFAULT 0.05,
    tumbler_transactions_min UInt32 DEFAULT 50,
    
    -- Unusual Hours Pattern
    unusual_hours_night_ratio_threshold Float64 DEFAULT 0.8,
    
    -- Large Infrequent Pattern
    large_infrequent_transactions_max UInt32 DEFAULT 20,
    large_infrequent_whale_min UInt32 DEFAULT 1,
    
    -- Fixed Amount Pattern
    fixed_pattern_variance_threshold Float64 DEFAULT 0.1,
    fixed_pattern_transactions_min UInt32 DEFAULT 20,
    
    -- Circular Transaction Detection
    circular_transactions_threshold UInt32 DEFAULT 10,
    
    -- Single Recipient Pattern
    single_recipient_transactions_min UInt32 DEFAULT 50,
    
    -- Analysis Minimums
    min_transactions_for_risk_analysis UInt32 DEFAULT 5,
    
    -- Suspicion Score Thresholds
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
COMMENT 'Risk assessment thresholds per network and asset';
```

### 4. Relationship Analysis Configuration

```sql
-- Configuration for address relationship analysis
CREATE TABLE IF NOT EXISTS balance_transfers_relationship_config (
    network String,
    asset String,
    
    -- Relationship Type Classification
    high_value_amount_min Decimal128(18) DEFAULT 10000,
    high_value_count_min UInt32 DEFAULT 10,
    high_frequency_count_min UInt32 DEFAULT 100,
    regular_relationship_count_min UInt32 DEFAULT 5,
    casual_relationship_count_min UInt32 DEFAULT 2,
    
    -- Relationship Strength Calculation
    relationship_strength_count_weight Float64 DEFAULT 0.4,
    relationship_strength_amount_weight Float64 DEFAULT 0.6,
    relationship_strength_multiplier Float64 DEFAULT 2.0,
    relationship_strength_max Float64 DEFAULT 10.0,
    
    -- Significant Relationship Filter
    significant_relationship_count_min UInt32 DEFAULT 2,
    
    -- Metadata
    is_default Bool DEFAULT false,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
COMMENT 'Relationship analysis thresholds per network and asset';
```

### 5. Behavior Analysis Configuration

```sql
-- Configuration for general behavior analysis parameters
CREATE TABLE IF NOT EXISTS balance_transfers_behavior_config (
    network String,
    asset String,
    
    -- Minimum Requirements for Analysis
    min_transactions_for_behavior_analysis UInt32 DEFAULT 1,
    min_transactions_for_classification UInt32 DEFAULT 5,
    
    -- Time-based Analysis
    activity_span_single_day_max UInt64 DEFAULT 86400,      -- 1 day in seconds
    activity_span_weekly_max UInt64 DEFAULT 604800,        -- 1 week in seconds  
    activity_span_monthly_max UInt64 DEFAULT 2592000,      -- 30 days in seconds
    activity_span_yearly_max UInt64 DEFAULT 31536000,      -- 365 days in seconds
    
    -- Statistical Analysis Parameters
    consistency_score_variance_threshold Float64 DEFAULT 1.0,
    
    -- Metadata
    is_default Bool DEFAULT false,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    _version UInt64
    
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (network, asset)
COMMENT 'General behavior analysis parameters per network and asset';
```

## Default Configuration Data

### Network-Specific Default Configurations

```sql
-- Torus Network (TOR) Defaults
INSERT INTO balance_transfers_transaction_size_config VALUES
('torus', 'TOR', 10, 10, 100, 100, 1000, 1000, 10000, 10000, 100, 100, 1000, 1000, 10000, 10000, 100000, 100000, true, now(), now(), 1);

INSERT INTO balance_transfers_address_classification_config VALUES  
('torus', 'TOR', 1000000, 100, 1000000, 10, 100000, 1000, 50, 50, 100, 10000, 10, 50000, 10, 1000, 100000, 10000, 1000, 100, 10, 1, 0.1, 100, true, now(), now(), 1);

INSERT INTO balance_transfers_risk_assessment_config VALUES
('torus', 'TOR', 1, 100, 0.05, 50, 0.8, 20, 1, 0.1, 20, 10, 50, 5, 4, 2, 1, true, now(), now(), 1);

INSERT INTO balance_transfers_relationship_config VALUES
('torus', 'TOR', 10000, 10, 100, 5, 2, 0.4, 0.6, 2.0, 10.0, 2, true, now(), now(), 1);

INSERT INTO balance_transfers_behavior_config VALUES
('torus', 'TOR', 1, 5, 86400, 604800, 2592000, 31536000, 1.0, true, now(), now(), 1);

-- Bittensor Network (TAO) Defaults - Adjusted for different token economics
INSERT INTO balance_transfers_transaction_size_config VALUES
('bittensor', 'TAO', 1, 1, 10, 10, 100, 100, 1000, 1000, 10, 10, 100, 100, 1000, 1000, 10000, 10000, true, now(), now(), 1);

INSERT INTO balance_transfers_address_classification_config VALUES
('bittensor', 'TAO', 100000, 100, 100000, 10, 10000, 1000, 50, 50, 100, 1000, 10, 5000, 10, 100, 10000, 1000, 100, 10, 10, 1, 0.1, 100, true, now(), now(), 1);

INSERT INTO balance_transfers_risk_assessment_config VALUES
('bittensor', 'TAO', 1, 100, 0.05, 50, 0.8, 20, 1, 0.1, 20, 10, 50, 5, 4, 2, 1, true, now(), now(), 1);

INSERT INTO balance_transfers_relationship_config VALUES
('bittensor', 'TAO', 1000, 10, 100, 5, 2, 0.4, 0.6, 2.0, 10.0, 2, true, now(), now(), 1);

INSERT INTO balance_transfers_behavior_config VALUES
('bittensor', 'TAO', 1, 5, 86400, 604800, 2592000, 31536000, 1.0, true, now(), now(), 1);

-- Polkadot Network (DOT) Defaults
INSERT INTO balance_transfers_transaction_size_config VALUES
('polkadot', 'DOT', 10, 10, 100, 100, 1000, 1000, 10000, 10000, 100, 100, 1000, 1000, 10000, 10000, 100000, 100000, true, now(), now(), 1);

INSERT INTO balance_transfers_address_classification_config VALUES
('polkadot', 'DOT', 1000000, 100, 1000000, 10, 100000, 1000, 50, 50, 100, 10000, 10, 50000, 10, 1000, 100000, 10000, 1000, 100, 10, 1, 0.1, 100, true, now(), now(), 1);

INSERT INTO balance_transfers_risk_assessment_config VALUES
('polkadot', 'DOT', 1, 100, 0.05, 50, 0.8, 20, 1, 0.1, 20, 10, 50, 5, 4, 2, 1, true, now(), now(), 1);

INSERT INTO balance_transfers_relationship_config VALUES
('polkadot', 'DOT', 10000, 10, 100, 5, 2, 0.4, 0.6, 2.0, 10.0, 2, true, now(), now(), 1);

INSERT INTO balance_transfers_behavior_config VALUES
('polkadot', 'DOT', 1, 5, 86400, 604800, 2592000, 31536000, 1.0, true, now(), now(), 1);
```

## Helper Functions and Views

### Configuration Lookup Functions

```sql
-- Helper function to get network from asset
CREATE OR REPLACE FUNCTION getNetworkFromAsset(asset_param String) -> String AS
    CASE asset_param
        WHEN 'TOR' THEN 'torus'
        WHEN 'TAO' THEN 'bittensor'
        WHEN 'DOT' THEN 'polkadot'
        ELSE 'unknown'
    END;

-- Generic configuration getter with fallback
CREATE OR REPLACE FUNCTION getTransactionSizeConfig(network_param String, asset_param String, config_key String) -> Decimal128(18) AS
    COALESCE(
        (SELECT 
            CASE config_key
                WHEN 'micro_max' THEN micro_transaction_max
                WHEN 'small_min' THEN small_transaction_min
                WHEN 'small_max' THEN small_transaction_max
                WHEN 'medium_min' THEN medium_transaction_min
                WHEN 'medium_max' THEN medium_transaction_max
                WHEN 'large_min' THEN large_transaction_min
                WHEN 'large_max' THEN large_transaction_max
                WHEN 'whale_min' THEN whale_transaction_min
                ELSE 0
            END
         FROM balance_transfers_transaction_size_config 
         WHERE network = network_param AND asset = asset_param),
        -- Fallback to network default
        (SELECT 
            CASE config_key
                WHEN 'micro_max' THEN micro_transaction_max
                WHEN 'small_min' THEN small_transaction_min
                WHEN 'small_max' THEN small_transaction_max
                WHEN 'medium_min' THEN medium_transaction_min
                WHEN 'medium_max' THEN medium_transaction_max
                WHEN 'large_min' THEN large_transaction_min
                WHEN 'large_max' THEN large_transaction_max
                WHEN 'whale_min' THEN whale_transaction_min
                ELSE 0
            END
         FROM balance_transfers_transaction_size_config 
         WHERE network = network_param AND is_default = true
         LIMIT 1)
    );
```

### Configuration Management Views

```sql
-- Summary view of all configurations
CREATE VIEW IF NOT EXISTS balance_transfers_config_summary AS
SELECT 
    'transaction_size' as config_type,
    network,
    asset,
    is_default,
    toString(micro_transaction_max) as key_threshold_1,
    toString(whale_transaction_min) as key_threshold_2,
    created_at,
    updated_at
FROM balance_transfers_transaction_size_config
UNION ALL
SELECT 
    'address_classification' as config_type,
    network,
    asset,
    is_default,
    toString(exchange_volume_min) as key_threshold_1,
    toString(whale_volume_min) as key_threshold_2,
    created_at,
    updated_at
FROM balance_transfers_address_classification_config
UNION ALL
SELECT 
    'risk_assessment' as config_type,
    network,
    asset,
    is_default,
    toString(mixer_transactions_min) as key_threshold_1,
    toString(tumbler_variance_threshold) as key_threshold_2,
    created_at,
    updated_at
FROM balance_transfers_risk_assessment_config
UNION ALL
SELECT 
    'relationship' as config_type,
    network,
    asset,
    is_default,
    toString(high_value_amount_min) as key_threshold_1,
    toString(high_frequency_count_min) as key_threshold_2,
    created_at,
    updated_at
FROM balance_transfers_relationship_config
UNION ALL
SELECT 
    'behavior' as config_type,
    network,
    asset,
    is_default,
    toString(min_transactions_for_behavior_analysis) as key_threshold_1,
    toString(activity_span_single_day_max) as key_threshold_2,
    created_at,
    updated_at
FROM balance_transfers_behavior_config
ORDER BY config_type, network, asset;
```

## Implementation Strategy

### Phase 1: Configuration Infrastructure
1. Create all 5 configuration tables
2. Insert default configurations for all supported networks (torus, bittensor, polkadot)
3. Create helper functions for configuration lookup
4. Add indexes for efficient configuration queries

### Phase 2: View Updates (Per View)
1. Update `balance_transfers_address_behavior_profiles_view` to use `balance_transfers_transaction_size_config`
2. Update `balance_transfers_address_classification_view` to use `balance_transfers_address_classification_config`
3. Update `balance_transfers_suspicious_activity_view` to use `balance_transfers_risk_assessment_config`
4. Update `balance_transfers_pairs_analysis_view` to use `balance_transfers_relationship_config`
5. Update `balance_transfers_address_activity_patterns_view` to use `balance_transfers_transaction_size_config` (activity thresholds)

### Phase 3: Configuration Management
1. Create configuration management views
2. Add configuration validation logic
3. Create procedures for updating configurations
4. Add configuration versioning support

### Phase 4: Testing and Validation
1. Test with different network configurations
2. Validate fallback logic works correctly
3. Performance testing with configuration lookups
4. Create configuration migration scripts

## Benefits of Modular Approach

1. **Better Organization**: Each configuration table serves a specific purpose
2. **Easier Maintenance**: Changes to one area don't affect others
3. **Clearer Naming**: Configuration parameters are clearly named by their usage context
4. **Flexible Scaling**: Easy to add new configuration types without affecting existing ones
5. **Targeted Updates**: Can update specific functionality configurations independently
6. **Better Documentation**: Each table is self-documenting for its specific use case

## Example View Update

Here's how the address behavior profiles view would be updated:

```sql
CREATE VIEW IF NOT EXISTS balance_transfers_address_behavior_profiles_view AS
SELECT
    address,
    asset,
    -- ... existing metrics ...
    
    -- Transaction Size Distribution (using modular config)
    countIf(amount < (
        SELECT micro_transaction_max 
        FROM balance_transfers_transaction_size_config 
        WHERE network = getNetworkFromAsset(asset) AND asset = asset 
        LIMIT 1
    )) as micro_transactions,
    
    countIf(amount >= (
        SELECT small_transaction_min 
        FROM balance_transfers_transaction_size_config 
        WHERE network = getNetworkFromAsset(asset) AND asset = asset 
        LIMIT 1
    ) AND amount < (
        SELECT small_transaction_max 
        FROM balance_transfers_transaction_size_config 
        WHERE network = getNetworkFromAsset(asset) AND asset = asset 
        LIMIT 1
    )) as small_transactions,
    
    -- ... continue for all size categories ...
    
FROM (
    SELECT from_address as address, from_address, to_address, asset, amount, fee, block_height, block_timestamp 
    FROM balance_transfers
    UNION ALL
    SELECT to_address as address, from_address, to_address, asset, amount, 0 as fee, block_height, block_timestamp 
    FROM balance_transfers
)
GROUP BY address, asset
HAVING total_transactions >= (
    SELECT min_transactions_for_behavior_analysis 
    FROM balance_transfers_behavior_config 
    WHERE network = getNetworkFromAsset(asset) AND asset = asset 
    LIMIT 1
);
```

This modular approach provides much better organization and makes it clear which configuration affects which functionality.