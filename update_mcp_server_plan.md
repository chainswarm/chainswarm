# MCP Server Update Plan

## Overview

This document outlines the plan for updating the MCP server's guidance and instruction methods in `packages/api/mcp_server.py` to reflect the new balance transfers functionality that has been added. The goal is to update the "### 💰 Balance & Transaction Analysis" section to include information about the new balance transfers views and combine it with the existing balance tracking functionality under a common name like "Account Balance Analytics".

## Current State

Currently, the MCP server has two main methods that provide documentation and instructions:

1. `get_user_guide()` - User-facing documentation for MCP server capabilities
2. `get_instructions()` - Comprehensive LLM instructions for blockchain analytics tools

The "### 💰 Balance & Transaction Analysis" section in the `get_instructions()` function only mentions balance tracking functionality but doesn't reflect the new balance transfers views that have been added.

## New Balance Transfers Views

The new balance transfers functionality includes several SQL views organized in different schema files:

1. **Core Tables and Indexes** (schema_part1_core.sql)
   - **balance_transfers**: Main table storing individual transfer transactions between addresses

2. **Basic Views** (schema_part2_basic_views.sql)
   - **balance_transfers_statistics_view**: Basic statistics by address and asset
   - **balance_transfers_daily_volume_mv**: Materialized view for daily transfer volume
   - **available_transfer_assets_view**: Simple view listing available assets

3. **Behavior Profiles** (schema_part3_behavior_profiles.sql)
   - **balance_transfers_address_behavior_profiles_view**: Comprehensive behavioral analysis for each address with asset-specific thresholds

4. **Address Classification** (schema_part4_classification.sql)
   - **balance_transfers_address_classification_view**: Classifies addresses into behavioral categories with asset-specific thresholds

5. **Suspicious Activity Detection** (schema_part5_suspicious_activity.sql)
   - **balance_transfers_suspicious_activity_view**: Identifies potentially suspicious or anomalous activity patterns

6. **Relationship Analysis** (schema_part6_relationships_activity.sql)
   - **balance_transfers_address_relationships_view**: Tracks and analyzes relationships between addresses
   - **balance_transfers_address_activity_patterns_view**: Analyzes temporal and behavioral patterns in address activity

7. **Network Flow Analysis** (schema_part7_network_flow.sql)
   - **balance_transfers_network_flow_view**: Provides a high-level overview of network activity by day and asset

8. **Temporal Analysis** (schema_part8_temporal_analysis.sql)
   - **balance_transfers_periodic_activity_view**: Analyzes activity patterns over weekly time periods for each address
   - **balance_transfers_seasonality_view**: Detects temporal patterns in transaction activity

9. **Economic Analysis** (schema_part9_economic_analysis.sql)
   - **balance_transfers_velocity_view**: Measures how quickly tokens circulate in the ecosystem
   - **balance_transfers_liquidity_concentration_view**: Analyzes how concentrated holdings are within the network
   - **balance_transfers_holding_time_view**: Analyzes how long addresses hold tokens before transferring them

10. **Anomaly Detection** (schema_part10_anomaly_detection.sql)
    - **balance_transfers_basic_anomaly_view**: Detects unusual transaction patterns

## Proposed Changes

### 1. Update `get_user_guide()` Function

In the `get_user_guide()` function, we need to update the "## 💰 Balance & Address Intelligence" section to "## 💰 Account Balance Analytics" and include information about the new balance transfers functionality.

#### Current Section:

```
## 💰 Balance & Address Intelligence
    
**What it does**: Tracks balance changes over time and maintains a database of known/labeled addresses (exchanges, treasuries, bridges, etc.).
    
**You can ask about**:
- Historical balance changes for any address
- Known addresses and their labels/purposes
- Transaction history with detailed records
- Asset movements and distributions
    
**Example questions**:
- "What are the well-known addresses on this blockchain?"
- "Show me the transaction history for [ADDRESS]"
- "What's the balance history of [ADDRESS] over the last month?"
- "Find all treasury and DAO addresses"
```

#### Updated Section:

```
## 💰 Account Balance Analytics
    
**What it does**: Tracks balance changes over time, analyzes transfer transactions between addresses, and maintains a database of known/labeled addresses (exchanges, treasuries, bridges, etc.).
    
**You can ask about**:
- Historical balance changes for any address
- Known addresses and their labels/purposes
- Transaction history with detailed records
- Address behavior patterns and classifications
- Relationship analysis between addresses
- Network flow and economic indicators
- Suspicious activity and anomaly detection
    
**Example questions**:
- "What are the well-known addresses on this blockchain?"
- "Show me the transaction history for [ADDRESS]"
- "What's the balance history of [ADDRESS] over the last month?"
- "Find all treasury and DAO addresses"
- "Identify addresses with suspicious transaction patterns"
- "Analyze the relationship between [ADDRESS1] and [ADDRESS2]"
- "What's the token velocity for [ASSET] over the last quarter?"
- "Show me addresses classified as 'whales' for [ASSET]"
```

### 2. Update `get_instructions()` Function

In the `get_instructions()` function, we need to rename the "### 💰 Balance & Transaction Analysis" section to "### 💰 Account Balance Analytics" and include information about the new balance transfers views.

#### Current Section:

```
### 💰 Balance & Transaction Analysis

**Balance Tracking**
- Tool: `balance_query`
- Purpose: Historical balance changes, balance change deltas, known addresses, transactions
- **Database**: ClickHouse
- **Schema**: {balance_schema}

**ClickHouse Query Guidelines:**
- Use ClickHouse SQL dialect (not standard SQL)
- Available aggregation functions: `sum()`, `avg()`, `max()`, `min()`, `count()`
- Time functions: `toStartOfDay()`, `toStartOfMonth()`, etc.
- Array functions: `arrayJoin()`, `arrayElement()`, etc.
```

#### Updated Section:

```
### 💰 Account Balance Analytics

**Balance Tracking**
- Tool: `balance_query`
- Purpose: Historical balance changes, balance change deltas, known addresses
- **Database**: ClickHouse
- **Schema**: {balance_schema}

**Balance Transfers Analysis**
- Tool: `balance_query` (same tool, different views)
- Purpose: Analyze individual transfer transactions between addresses
- **Database**: ClickHouse
- **Available Views**:
  - **Basic Views**: 
    - `balance_transfers_statistics_view`: Basic statistics by address and asset
    - `balance_transfers_daily_volume_mv`: Materialized view for daily transfer volume
    - `available_transfer_assets_view`: Simple view listing available assets
  
  - **Behavior Analysis**:
    - `balance_transfers_address_behavior_profiles_view`: Comprehensive behavioral analysis for each address
    - `balance_transfers_address_classification_view`: Classifies addresses into behavioral categories
    - `balance_transfers_suspicious_activity_view`: Identifies potentially suspicious activity patterns
  
  - **Relationship Analysis**:
    - `balance_transfers_address_relationships_view`: Tracks relationships between addresses
    - `balance_transfers_address_activity_patterns_view`: Analyzes temporal and behavioral patterns
  
  - **Network Analysis**:
    - `balance_transfers_network_flow_view`: High-level overview of network activity
    - `balance_transfers_periodic_activity_view`: Activity patterns over weekly time periods
    - `balance_transfers_seasonality_view`: Temporal patterns in transaction activity
  
  - **Economic Analysis**:
    - `balance_transfers_velocity_view`: Measures token circulation speed
    - `balance_transfers_liquidity_concentration_view`: Analyzes holding concentration
    - `balance_transfers_holding_time_view`: Analyzes token holding duration
  
  - **Anomaly Detection**:
    - `balance_transfers_basic_anomaly_view`: Detects unusual transaction patterns

**Example Queries**:
```sql
-- Get basic statistics for an address
SELECT * FROM balance_transfers_statistics_view 
WHERE address = '0x123...' AND asset = 'TOR';

-- Find suspicious activity
SELECT * FROM balance_transfers_suspicious_activity_view
WHERE risk_level = 'High' 
ORDER BY suspicion_score DESC LIMIT 10;

-- Analyze relationships between addresses
SELECT * FROM balance_transfers_address_relationships_view
WHERE from_address = '0x123...' OR to_address = '0x123...'
ORDER BY relationship_strength DESC;

-- Analyze token velocity
SELECT * FROM balance_transfers_velocity_view
WHERE asset = 'TOR' 
ORDER BY month DESC LIMIT 12;
```

**ClickHouse Query Guidelines:**
- Use ClickHouse SQL dialect (not standard SQL)
- Available aggregation functions: `sum()`, `avg()`, `max()`, `min()`, `count()`
- Time functions: `toStartOfDay()`, `toStartOfMonth()`, etc.
- Array functions: `arrayJoin()`, `arrayElement()`, etc.
```

## Implementation Plan

1. Switch to Code mode to implement these changes
2. Update the `get_user_guide()` function to rename the section and include information about balance transfers
3. Update the `get_instructions()` function to rename the section and include detailed information about the balance transfers views
4. Test the changes to ensure they work as expected

## Diagram

```mermaid
graph TD
    A[Account Balance Analytics] --> B[Balance Tracking]
    A --> C[Balance Transfers]
    
    B --> B1[Historical balance changes]
    B --> B2[Balance change deltas]
    B --> B3[Known addresses lookup]
    
    C --> C1[Basic Views]
    C --> C2[Behavior Analysis]
    C --> C3[Relationship Analysis]
    C --> C4[Network Analysis]
    C --> C5[Economic Analysis]
    C --> C6[Anomaly Detection]
    
    C1 --> C1_1[Statistics View]
    C1 --> C1_2[Daily Volume MV]
    C1 --> C1_3[Available Assets View]
    
    C2 --> C2_1[Behavior Profiles View]
    C2 --> C2_2[Classification View]
    C2 --> C2_3[Suspicious Activity View]
    
    C3 --> C3_1[Address Relationships View]
    C3 --> C3_2[Activity Patterns View]
    
    C4 --> C4_1[Network Flow View]
    C4 --> C4_2[Periodic Activity View]
    C4 --> C4_3[Seasonality View]
    
    C5 --> C5_1[Velocity View]
    C5 --> C5_2[Liquidity Concentration View]
    C5 --> C5_3[Holding Time View]
    
    C6 --> C6_1[Basic Anomaly View]