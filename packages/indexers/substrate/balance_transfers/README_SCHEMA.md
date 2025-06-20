# Balance Transfers Schema Documentation

## Overview

The Balance Transfers schema is designed to track and analyze transfer transactions on Substrate networks. The schema is split into multiple files to avoid ClickHouse query length limitations and to organize the views logically.

## Schema Structure

The schema is organized into the following parts:

### 1. Core Tables and Indexes (schema_part1_core.sql)

- **balance_transfers**: Main table storing individual transfer transactions between addresses
- Various indexes for efficient querying

### 2. Basic Views (schema_part2_basic_views.sql)

- **balance_transfers_statistics_view**: Basic statistics by address and asset
- **balance_transfers_daily_volume_mv**: Materialized view for daily transfer volume
- **available_transfer_assets_view**: Simple view listing available assets

### 3. Behavior Profiles (schema_part3_behavior_profiles.sql)

- **balance_transfers_address_behavior_profiles_view**: Comprehensive behavioral analysis for each address with asset-specific thresholds

### 4. Address Classification (schema_part4_classification.sql)

- **balance_transfers_address_classification_view**: Classifies addresses into behavioral categories with asset-specific thresholds

### 5. Suspicious Activity Detection (schema_part5_suspicious_activity.sql)

- **balance_transfers_suspicious_activity_view**: Identifies potentially suspicious or anomalous activity patterns

### 6. Relationship Analysis (schema_part6_relationships_activity.sql)

- **balance_transfers_address_relationships_view**: Tracks and analyzes relationships between addresses
- **balance_transfers_address_activity_patterns_view**: Analyzes temporal and behavioral patterns in address activity

### 7. Network Flow Analysis (schema_part7_network_flow.sql)

- **balance_transfers_network_flow_view**: Provides a high-level overview of network activity by day and asset, including transaction counts, volumes, unique addresses, and network density metrics

### 8. Temporal Analysis (schema_part8_temporal_analysis.sql)

- **balance_transfers_periodic_activity_view**: Analyzes activity patterns over weekly time periods for each address
- **balance_transfers_seasonality_view**: Detects temporal patterns in transaction activity (hour-of-day, day-of-week, monthly patterns)

### 9. Economic Analysis (schema_part9_economic_analysis.sql)

- **balance_transfers_velocity_view**: Measures how quickly tokens circulate in the ecosystem
- **balance_transfers_liquidity_concentration_view**: Analyzes how concentrated holdings are within the network
- **balance_transfers_holding_time_view**: Analyzes how long addresses hold tokens before transferring them

### 10. Anomaly Detection (schema_part10_anomaly_detection.sql)

- **balance_transfers_anomaly_detection_view**: Detects unusual changes in transaction patterns using statistical methods (z-scores)

## Asset-Specific Thresholds

Throughout the schema, asset-specific thresholds are used to account for the different value scales of various assets:

- **TOR**: $1 = 1 TOR
- **TAO**: $350 = 1 TAO
- **DOT**: $4 = 1 DOT

These thresholds are implemented using nested if() functions with the pattern:

```sql
if(asset = 'TOR', ...,
if(asset = 'TAO', ...,
if(asset = 'DOT', ...,
   default_value)))
```

## Execution

The schema files can be executed individually using the `execute_schema_parts.py` script:

```bash
python execute_schema_parts.py --database your_database_name
```

This script executes each schema file in the correct order, handling any dependencies between views.

## Maintenance

When adding new views or modifying existing ones:

1. Place the view in the appropriate schema file based on its functionality
2. Update the `execute_schema_parts.py` script if adding new schema files
3. Update the `schema_main.sql` file to include any new schema files
4. Use asset-specific thresholds where appropriate
5. Follow the established naming conventions