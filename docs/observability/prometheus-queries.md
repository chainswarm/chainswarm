# ChainSwarm Prometheus Queries Reference

This document provides a comprehensive reference for all available Prometheus queries for monitoring ChainSwarm indexers and services.

## Table of Contents

- [Overview](#overview)
- [Service Health Monitoring](#service-health-monitoring)
- [Block Processing Metrics](#block-processing-metrics)
- [Database Performance](#database-performance)
- [Indexer-Specific Metrics](#indexer-specific-metrics)
- [Error Monitoring](#error-monitoring)
- [Performance Analysis](#performance-analysis)
- [Dashboard Queries](#dashboard-queries)
- [Alerting Queries](#alerting-queries)

## Overview

ChainSwarm exposes metrics for the following services (Torus network example):
- **torus-balance-transfers** (Port 9101)
- **torus-balance-series** (Port 9102)
- **torus-money-flow** (Port 9103)
- **torus-block-stream** (Port 9104)
- **torus-api** (Port 9200)
- **torus-block-stream-api** (Port 9201)
- **torus-mcp-server** (Port 9202)

## Service Health Monitoring

### Service Status Overview
```promql
# All service health statuses
service_health_status

# Unhealthy services
service_health_status != 1

# Service uptime
time() - service_start_time_seconds
```

### Service Information
```promql
# Service metadata
service_info_info

# Services by network
service_info_info{network="torus"}
```

## Block Processing Metrics

### Current Block Heights
```promql
# All indexer current block heights
indexer_current_block_height

# Specific indexer block height
indexer_current_block_height{indexer="block_stream"}

# Block heights by network
indexer_current_block_height{network="torus"}
```

### Block Processing Rates
```promql
# Current processing rates (blocks per second)
indexer_processing_rate_blocks_per_second

# Average processing rate over 5 minutes
rate(indexer_blocks_processed_total[5m])

# Processing rate by indexer
indexer_processing_rate_blocks_per_second{indexer="balance_series"}
```

### Sync Status
```promql
# Blocks behind latest
indexer_blocks_behind_latest

# Services that are behind
indexer_blocks_behind_latest > 0

# Blocks behind by network
indexer_blocks_behind_latest{network="torus"}
```

### Block Processing Duration
```promql
# Average block processing time
rate(indexer_block_processing_duration_seconds_sum[5m]) / rate(indexer_block_processing_duration_seconds_count[5m])

# 95th percentile processing time
histogram_quantile(0.95, indexer_block_processing_duration_seconds_bucket)

# 99th percentile processing time
histogram_quantile(0.99, indexer_block_processing_duration_seconds_bucket)

# Processing time by indexer
histogram_quantile(0.95, indexer_block_processing_duration_seconds_bucket{indexer="money_flow"})
```

## Database Performance

### Database Operations
```promql
# Total database operations
indexer_database_operations_total

# Database operations rate
rate(indexer_database_operations_total[5m])

# Operations by type
indexer_database_operations_total{operation="insert"}
indexer_database_operations_total{operation="select"}

# Operations by table
indexer_database_operations_total{table="balance_series"}
```

### Database Operation Duration
```promql
# Average database operation duration
rate(indexer_database_operation_duration_seconds_sum[5m]) / rate(indexer_database_operation_duration_seconds_count[5m])

# 95th percentile database operation time
histogram_quantile(0.95, indexer_database_operation_duration_seconds_bucket)

# Slow database operations (>1 second)
histogram_quantile(0.95, indexer_database_operation_duration_seconds_bucket) > 1

# Database performance by operation type
histogram_quantile(0.95, indexer_database_operation_duration_seconds_bucket{operation="insert"})
```

### Database Errors
```promql
# Total database errors
indexer_database_errors_total

# Database error rate
rate(indexer_database_errors_total[5m])

# Database errors by indexer
indexer_database_errors_total{indexer="balance_transfers"}
```

## Indexer-Specific Metrics

### Balance Series Indexer
```promql
# Period processing duration
consumer_period_processing_duration_seconds{indexer="balance_series"}

# Addresses processed
consumer_addresses_processed_total{indexer="balance_series"}

# Periods processed
consumer_periods_processed_total{indexer="balance_series"}

# Period processing rate
rate(consumer_periods_processed_total{indexer="balance_series"}[5m])
```

### Money Flow Indexer
```promql
# Batch processing duration
consumer_batch_processing_duration_seconds{indexer="money_flow"}

# Community detection performance
consumer_community_detection_duration_seconds{indexer="money_flow"}

# Page rank calculation time
consumer_page_rank_duration_seconds{indexer="money_flow"}

# Embeddings update time
consumer_embeddings_update_duration_seconds{indexer="money_flow"}

# Blocks processed by money flow
consumer_blocks_processed_total{indexer="money_flow"}
```

### Block Stream Indexer
```promql
# Batch processing duration
consumer_batch_processing_duration_seconds{indexer="block_stream"}

# Blocks fetched from blockchain
consumer_blocks_fetched_total{indexer="block_stream"}

# Empty batches encountered
consumer_empty_batches_total{indexer="block_stream"}
```

### Balance Transfers Indexer
```promql
# Balance transfers specific metrics
indexer_blocks_processed_total{indexer="balance_transfers"}
indexer_current_block_height{indexer="balance_transfers"}
```

## Error Monitoring

### Service Errors
```promql
# All service errors
service_errors_total

# Service error rate
rate(service_errors_total[5m])

# Errors by type
service_errors_total{error_type="connection_error"}
```

### Consumer Errors
```promql
# All consumer errors
consumer_errors_total

# Consumer error rate
rate(consumer_errors_total[5m])

# Errors by indexer
consumer_errors_total{indexer="balance_series"}

# Errors by type
consumer_errors_total{error_type="processing_error"}
```

### Failed Events
```promql
# Failed events total
indexer_failed_events_total

# Failed event rate
rate(indexer_failed_events_total[5m])
```

## Performance Analysis

### Throughput Analysis
```promql
# Total events processed per second
rate(indexer_events_processed_total[5m])

# Blocks processed per minute
rate(indexer_blocks_processed_total[1m]) * 60

# Addresses processed per hour (balance series)
rate(consumer_addresses_processed_total{indexer="balance_series"}[1h]) * 3600
```

### Resource Utilization
```promql
# Processing efficiency (events per second per block)
rate(indexer_events_processed_total[5m]) / rate(indexer_blocks_processed_total[5m])

# Database operations per block
rate(indexer_database_operations_total[5m]) / rate(indexer_blocks_processed_total[5m])
```

### Latency Analysis
```promql
# Processing latency distribution
histogram_quantile(0.50, indexer_block_processing_duration_seconds_bucket)  # Median
histogram_quantile(0.90, indexer_block_processing_duration_seconds_bucket)  # 90th percentile
histogram_quantile(0.95, indexer_block_processing_duration_seconds_bucket)  # 95th percentile
histogram_quantile(0.99, indexer_block_processing_duration_seconds_bucket)  # 99th percentile
```

## Dashboard Queries

### Multi-Indexer Overview Dashboard
```promql
# Current block heights (table view)
indexer_current_block_height

# Processing rates (graph view)
indexer_processing_rate_blocks_per_second

# Sync status (table view)
indexer_blocks_behind_latest

# Error rates (graph view)
rate(service_errors_total[5m]) + rate(consumer_errors_total[5m])
```

### Performance Dashboard
```promql
# Processing duration trends
rate(indexer_block_processing_duration_seconds_sum[5m]) / rate(indexer_block_processing_duration_seconds_count[5m])

# Database performance trends
rate(indexer_database_operation_duration_seconds_sum[5m]) / rate(indexer_database_operation_duration_seconds_count[5m])

# Throughput trends
rate(indexer_blocks_processed_total[5m])
```

### Health Dashboard
```promql
# Service health overview
service_health_status

# Error rate overview
rate(service_errors_total[5m])

# Sync status overview
indexer_blocks_behind_latest
```

## Alerting Queries

### Critical Alerts
```promql
# Service down
service_health_status == 0

# High error rate (>5 errors per minute)
rate(service_errors_total[5m]) > 5

# Severely behind sync (>1000 blocks)
indexer_blocks_behind_latest > 1000

# Very slow processing (>10 seconds per block)
histogram_quantile(0.95, indexer_block_processing_duration_seconds_bucket) > 10
```

### Warning Alerts
```promql
# Moderate sync lag (>100 blocks behind)
indexer_blocks_behind_latest > 100

# Slow processing (>5 seconds per block)
histogram_quantile(0.95, indexer_block_processing_duration_seconds_bucket) > 5

# Low processing rate (<0.1 blocks per second)
indexer_processing_rate_blocks_per_second < 0.1

# Database errors present
rate(indexer_database_errors_total[5m]) > 0
```

### Performance Alerts
```promql
# Slow database operations (>2 seconds)
histogram_quantile(0.95, indexer_database_operation_duration_seconds_bucket) > 2

# High batch processing time (>30 seconds)
histogram_quantile(0.95, consumer_batch_processing_duration_seconds_bucket) > 30

# Community detection taking too long (>300 seconds)
histogram_quantile(0.95, consumer_community_detection_duration_seconds_bucket{indexer="money_flow"}) > 300
```

## Advanced Queries

### Correlation Analysis
```promql
# Processing rate vs error rate correlation
(rate(indexer_blocks_processed_total[5m])) / (rate(service_errors_total[5m]) + 1)

# Database performance impact on processing
rate(indexer_block_processing_duration_seconds_sum[5m]) / rate(indexer_database_operation_duration_seconds_sum[5m])
```

### Trend Analysis
```promql
# Processing rate trend (increase/decrease over time)
deriv(rate(indexer_blocks_processed_total[5m])[1h:])

# Error rate trend
deriv(rate(service_errors_total[5m])[1h:])
```

### Capacity Planning
```promql
# Projected time to catch up (hours)
indexer_blocks_behind_latest / (indexer_processing_rate_blocks_per_second * 3600)

# Processing capacity utilization
indexer_processing_rate_blocks_per_second / on(indexer) group_left() (max(indexer_processing_rate_blocks_per_second) by (indexer))
```

## Query Tips

### Time Ranges
- Use `[5m]` for recent trends
- Use `[1h]` for hourly analysis
- Use `[1d]` for daily patterns
- Use `[7d]` for weekly trends

### Aggregation Functions
- `rate()` - Calculate per-second rate
- `increase()` - Calculate total increase
- `avg()` - Average values
- `max()` - Maximum values
- `min()` - Minimum values
- `sum()` - Sum values
- `histogram_quantile()` - Calculate percentiles

### Filtering
- `{indexer="block_stream"}` - Filter by indexer
- `{network="torus"}` - Filter by network
- `{error_type="processing_error"}` - Filter by error type
- `{operation="insert"}` - Filter by operation type

### Combining Queries
```promql
# Multiple metrics in one query
(indexer_current_block_height) or (service_errors_total) or (indexer_processing_rate_blocks_per_second)

# Conditional queries
indexer_blocks_behind_latest > 0 and service_health_status == 1
```

---

## Quick Reference

### Essential Monitoring Queries
1. **Service Health**: `service_health_status`
2. **Block Heights**: `indexer_current_block_height`
3. **Sync Status**: `indexer_blocks_behind_latest`
4. **Processing Rates**: `indexer_processing_rate_blocks_per_second`
5. **Error Rates**: `rate(service_errors_total[5m])`
6. **Database Performance**: `histogram_quantile(0.95, indexer_database_operation_duration_seconds_bucket)`

### Access URLs
- **Prometheus UI**: http://localhost:9090
- **Metrics Endpoints** (Torus network example):
  - Torus Balance Transfers: http://localhost:9101/metrics
  - Torus Balance Series: http://localhost:9102/metrics
  - Torus Money Flow: http://localhost:9103/metrics
  - Torus Block Stream: http://localhost:9104/metrics
  - Torus API: http://localhost:9200/metrics
  - Torus Block Stream API: http://localhost:9201/metrics
  - Torus MCP Server: http://localhost:9202/metrics