# Schema Consolidation Plan

## Overview

This document outlines the planned changes to consolidate and optimize the SQL views in the balance transfers schema. The goal is to eliminate redundancy, improve maintainability, and ensure that `schema.sql` is the single source of truth for all database objects.

## Key Principles

1. **schema.sql will be the only source of truth** for all table and view declarations
2. **Remove redundant views**, merging their functionality where appropriate
3. **Remove all "enhanced" and "extended" views**, incorporating their features into base views
4. **Standardize on consolidated approach** for analytics views

## Specific Changes

### 1. Network Analytics Views

Merge `balance_transfers_network_daily_view` and `balance_transfers_network_flow_view` into a comprehensive view that includes all metrics from both.

**New View**: `balance_transfers_network_daily_analytics_view`

This name is chosen to be descriptive and easily understood by LLM agents, clearly indicating:
- The data domain (balance_transfers)
- The analytical focus (network)
- The time period (daily)
- The comprehensive nature (analytics)

This view will:
- Maintain period framework structure for consistency with weekly/monthly views
- Include all statistical metrics from flow view (min/max/median transaction sizes, fee details)
- Use the most efficient query pattern

### 2. Address Analytics

Use the consolidated approach from `schema_part3_address_analytics.sql` but move it to `schema.sql`.

Keep only `balance_transfers_address_analytics_view` and remove:
- `balance_transfers_address_behavior_profiles_view`
- `balance_transfers_address_classification_view`
- `balance_transfers_suspicious_activity_view`

### 3. Transaction/Relationship Views

Keep only `balance_transfers_relationships_view` with complete metrics, and remove any duplicate functionality.

Keep only one activity pattern view (`balance_transfers_daily_patterns_view`), removing redundant views.

### 4. Volume Series Views

Keep only `balance_transfers_volume_series_mv` and remove `balance_transfers_daily_volume_mv`.

### 5. Enhanced Views

Remove all "enhanced" views, incorporating their features into base views:
- Remove `balance_transfers_network_enhanced_view`
- Remove `balance_transfers_transaction_analytics_view`

## Implementation Steps

1. Update `schema.sql` with all the consolidated view definitions
2. Remove all separate schema part files after migration is complete
3. Test all views to ensure functionality is preserved
4. Update any application code that depends on removed views

## Specific View Mapping

| Action | Old View(s) | New View |
|--------|-------------|----------|
| Merge | `balance_transfers_network_daily_view` + `balance_transfers_network_flow_view` | `balance_transfers_network_daily_analytics_view` |
| Keep | `balance_transfers_address_analytics_view` | (same) |
| Remove | `balance_transfers_address_behavior_profiles_view` | (use analytics view) |
| Remove | `balance_transfers_address_classification_view` | (use analytics view) |
| Remove | `balance_transfers_suspicious_activity_view` | (use analytics view) |
| Keep | `balance_transfers_relationships_view` | (same) |
| Remove | `balance_transfers_transaction_analytics_view` | (use relationships view) |
| Keep | `balance_transfers_daily_patterns_view` | (same) |
| Remove | `balance_transfers_address_activity_patterns_view` | (use daily patterns view) |