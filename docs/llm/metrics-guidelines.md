# ChainSwarm Metrics Guidelines for LLM Agents

## Overview

This document provides comprehensive guidelines for LLM agents working on the ChainSwarm project regarding Prometheus metrics implementation. These guidelines ensure consistent metrics collection that complements our logging strategy and provides comprehensive observability.

## Core Principles

### 1. Metrics for Quantitative Data
- **Counters**: Total counts that only increase (requests, errors, processed items)
- **Gauges**: Current values that can go up/down (queue size, active connections, current block height)
- **Histograms**: Distribution of values (request duration, processing time, data sizes)

### 2. Consistent Labeling Strategy
- Use consistent label names across all metrics
- Include network, service, and component labels
- Avoid high-cardinality labels (user IDs, timestamps, etc.)

### 3. Complement Logging
- Metrics provide the "what" and "how much"
- Logs provide the "why" and context
- Use correlation IDs to link metrics anomalies to logs

## Existing Metrics Infrastructure

### Base Metrics (All Services)
```python
# Service information
service_info{service_name="substrate-torus-balance-transfers", version="1.0.0"}
service_start_time_seconds
service_health_status  # 1=healthy, 0=unhealthy
service_errors_total{error_type="database_timeout", component="indexer"}
```

### Indexer Metrics (IndexerMetrics class)
```python
# Block processing
indexer_blocks_processed_total{network="torus", indexer="balance_transfers"}
indexer_current_block_height{network="torus", indexer="balance_transfers"}
indexer_blocks_behind_latest{network="torus", indexer="balance_transfers"}
indexer_block_processing_duration_seconds{network="torus", indexer="balance_transfers"}
indexer_processing_rate_blocks_per_second{network="torus", indexer="balance_transfers"}

# Database operations
indexer_database_operations_total{network="torus", indexer="balance_transfers", operation="insert", table="balance_transfers"}
indexer_database_operation_duration_seconds{network="torus", indexer="balance_transfers", operation="insert"}
indexer_database_errors_total{network="torus", indexer="balance_transfers", error_type="timeout"}

# Event processing
indexer_events_processed_total{network="torus", indexer="balance_transfers", event_type="Balances.Transfer"}
indexer_failed_events_total{network="torus", indexer="balance_transfers", error_type="validation_error"}
```

### API Metrics (PrometheusMiddleware)
```python
# HTTP requests
http_requests_total{method="GET", endpoint="/balance_series", status="200", network="torus"}
http_request_duration_seconds{method="GET", endpoint="/balance_series", network="torus"}
http_requests_in_progress{method="GET", endpoint="/balance_series", network="torus"}
http_request_size_bytes{method="POST", endpoint="/balance_series", network="torus"}
http_response_size_bytes{method="GET", endpoint="/balance_series", network="torus"}
```

## When to Add New Metrics

### Add Metrics For
- **Counts**: Number of operations, requests, errors, processed items
- **Rates**: Operations per second, error rates, throughput
- **Durations**: Processing time, response time, queue wait time
- **Sizes**: Data volumes, queue lengths, memory usage
- **States**: Current values, active connections, health status

### Don't Add Metrics For
- **Debugging information**: Use logs instead
- **High-cardinality data**: User IDs, specific addresses, timestamps
- **One-time events**: Use logs for unique occurrences
- **Complex context**: Use structured logs

## Metric Naming Conventions

### Format
```
{component}_{what}_{unit}
```

### Examples
```python
# Good naming
indexer_blocks_processed_total          # Counter
indexer_current_block_height           # Gauge  
indexer_processing_duration_seconds    # Histogram
api_database_queries_total             # Counter
mcp_active_sessions                    # Gauge

# Bad naming
indexer_blocks                         # Unclear what it measures
processing_time                        # Missing component prefix
blocks_per_second_rate                 # Redundant "rate" suffix
```

### Suffixes
- `_total`: Counters (monotonically increasing)
- `_seconds`: Time durations
- `_bytes`: Data sizes
- `_ratio`: Ratios (0.0 to 1.0)
- No suffix: Gauges (current values)

## Label Guidelines

### Standard Labels
```python
# Always include these where applicable
network="torus"           # Network being processed
service="balance-transfers" # Service component
indexer="balance_transfers" # Indexer type
component="api"           # Component type
```

### HTTP-Specific Labels
```python
method="GET"              # HTTP method
endpoint="/balance_series" # API endpoint (normalized)
status="200"              # HTTP status code
```

### Database Labels
```python
operation="insert"        # Database operation type
table="balance_transfers" # Table name
database="clickhouse"     # Database type
```

### Error Labels
```python
error_type="timeout"      # Classified error type
error_category="database" # Error category
```

### Avoid High-Cardinality Labels
```python
# ❌ BAD - Too many unique values
user_id="user123"
block_hash="0xabc123..."
timestamp="2025-06-29T08:30:00Z"
client_ip="192.168.1.100"

# ✅ GOOD - Limited unique values
error_type="timeout"      # ~10 types
status="200"              # ~20 status codes
network="torus"           # ~3 networks
```

## Implementation Patterns

### Using Existing Metrics Registry
```python
from packages.indexers.base.metrics import get_metrics_registry

# Get existing registry
metrics_registry = get_metrics_registry(service_name)
if metrics_registry:
    # Create custom metrics
    custom_counter = metrics_registry.create_counter(
        'custom_operations_total',
        'Total custom operations',
        ['operation_type', 'status']
    )
    
    # Record metrics
    custom_counter.labels(operation_type='validation', status='success').inc()
```

### Creating New Metrics Classes
```python
class APIMetrics:
    def __init__(self, registry, service_name):
        self.registry = registry
        self.service_name = service_name
        
        # Request metrics
        self.requests_total = registry.create_counter(
            'api_requests_total',
            'Total API requests',
            ['method', 'endpoint', 'status']
        )
        
        self.request_duration = registry.create_histogram(
            'api_request_duration_seconds',
            'API request duration',
            ['method', 'endpoint'],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, float('inf'))
        )
    
    def record_request(self, method, endpoint, status, duration):
        self.requests_total.labels(method=method, endpoint=endpoint, status=status).inc()
        self.request_duration.labels(method=method, endpoint=endpoint).observe(duration)
```

### Indexer Metrics Usage
```python
from packages.indexers.base.metrics import IndexerMetrics

# Initialize in indexer
self.metrics = IndexerMetrics(metrics_registry, network, "balance_transfers")

# Record block processing
start_time = time.time()
# ... process block ...
processing_time = time.time() - start_time
self.metrics.record_block_processed(block_height, processing_time)

# Record database operations
start_time = time.time()
# ... database operation ...
duration = time.time() - start_time
self.metrics.record_database_operation("insert", "balance_transfers", duration, success=True)

# Record events
self.metrics.record_event_processed("Balances.Transfer", count=5)
```

## Component-Specific Guidelines

### Indexers
**Required Metrics**:
- Block processing progress and performance
- Database operation metrics
- Event processing counts
- Error rates by type
- Processing lag (blocks behind)

**Example Implementation**:
```python
class BalanceTransfersIndexer:
    def __init__(self, network):
        service_name = f"substrate-{network}-balance-transfers"
        metrics_registry = get_metrics_registry(service_name)
        self.metrics = IndexerMetrics(metrics_registry, network, "balance_transfers")
        
        # Custom metrics for this indexer
        self.transfers_extracted = metrics_registry.create_counter(
            'indexer_transfers_extracted_total',
            'Total balance transfers extracted',
            ['network', 'transfer_type']
        )
    
    def process_block(self, block):
        start_time = time.time()
        try:
            # Process block
            transfers = self.extract_transfers(block)
            
            # Record metrics
            processing_time = time.time() - start_time
            self.metrics.record_block_processed(block.height, processing_time)
            self.transfers_extracted.labels(
                network=self.network, 
                transfer_type='balance'
            ).inc(len(transfers))
            
        except Exception as e:
            self.metrics.record_failed_event(type(e).__name__)
            raise
```

### APIs
**Required Metrics**:
- HTTP request metrics (count, duration, status)
- Database query performance
- Rate limiting metrics
- Error rates by endpoint

**Example Implementation**:
```python
# Already implemented in PrometheusMiddleware
# Custom endpoint metrics
class BalanceSeriesRouter:
    def __init__(self):
        metrics_registry = get_metrics_registry(f"{network}-api")
        self.query_duration = metrics_registry.create_histogram(
            'api_balance_series_query_duration_seconds',
            'Balance series query duration',
            ['query_type']
        )
    
    async def get_balance_series(self, address: str):
        start_time = time.time()
        try:
            result = await self.service.get_balance_series(address)
            duration = time.time() - start_time
            self.query_duration.labels(query_type='single_address').observe(duration)
            return result
        except Exception as e:
            # Error metrics handled by middleware
            raise
```

### MCP Server
**Required Metrics**:
- Tool execution metrics
- Session management
- Query performance by tool
- Rate limiting metrics

**Example Implementation**:
```python
class MCPMetrics:
    def __init__(self, registry, network):
        self.tool_calls_total = registry.create_counter(
            'mcp_tool_calls_total',
            'Total MCP tool calls',
            ['tool', 'network', 'status']
        )
        
        self.tool_duration = registry.create_histogram(
            'mcp_tool_duration_seconds',
            'MCP tool execution duration',
            ['tool', 'network']
        )
        
        self.active_sessions = registry.create_gauge(
            'mcp_active_sessions',
            'Active MCP sessions',
            ['network']
        )
    
    def record_tool_call(self, tool_name, duration, success, error_type=None):
        status = 'success' if success else 'error'
        self.tool_calls_total.labels(
            tool=tool_name, 
            network=self.network, 
            status=status
        ).inc()
        self.tool_duration.labels(tool=tool_name, network=self.network).observe(duration)
```

## Histogram Buckets

### Pre-defined Bucket Sets
```python
# From packages/indexers/base/metrics.py
DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, float('inf'))
SIZE_BUCKETS = (64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, float('inf'))
COUNT_BUCKETS = (1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, float('inf'))
```

### Usage
```python
# Request duration (use DURATION_BUCKETS)
request_duration = registry.create_histogram(
    'api_request_duration_seconds',
    'Request duration',
    ['endpoint'],
    buckets=DURATION_BUCKETS
)

# Data sizes (use SIZE_BUCKETS)
response_size = registry.create_histogram(
    'api_response_size_bytes',
    'Response size',
    ['endpoint'],
    buckets=SIZE_BUCKETS
)

# Item counts (use COUNT_BUCKETS)
batch_size = registry.create_histogram(
    'indexer_batch_size',
    'Processing batch size',
    ['indexer'],
    buckets=COUNT_BUCKETS
)
```

## Error Metrics Strategy

### Error Classification
```python
# Classify errors by type for better alerting
def classify_error(error):
    error_type = type(error).__name__
    if 'Connection' in error_type or 'Timeout' in error_type:
        return 'connection_error'
    elif 'Validation' in error_type or 'ValueError' in str(error):
        return 'validation_error'
    elif 'Database' in error_type:
        return 'database_error'
    else:
        return 'unknown_error'

# Record classified errors
self.metrics.record_error(classify_error(e), component="indexer")
```

### Error Rate Calculation
```python
# Use rate() function in Prometheus queries
# Error rate over 5 minutes
rate(service_errors_total[5m])

# Error ratio (errors / total operations)
rate(service_errors_total[5m]) / rate(indexer_blocks_processed_total[5m])
```

## Performance Considerations

### Metric Collection Overhead
- Counters: Very low overhead (~1-2 nanoseconds)
- Gauges: Very low overhead
- Histograms: Higher overhead due to bucket calculations

### High-Frequency Metrics
```python
# For high-frequency operations, batch updates
class BatchMetrics:
    def __init__(self):
        self.pending_increments = defaultdict(int)
        self.last_flush = time.time()
    
    def increment(self, metric_name, labels, value=1):
        key = (metric_name, tuple(labels.items()))
        self.pending_increments[key] += value
        
        # Flush every 10 seconds
        if time.time() - self.last_flush > 10:
            self.flush()
    
    def flush(self):
        for (metric_name, labels), value in self.pending_increments.items():
            # Apply increments to actual metrics
            pass
        self.pending_increments.clear()
        self.last_flush = time.time()
```

## Testing Metrics

### Validation Checklist
- [ ] All metrics follow naming conventions
- [ ] Labels have reasonable cardinality (<100 unique values)
- [ ] Histograms use appropriate buckets
- [ ] Metrics complement logs (no duplication)
- [ ] Error metrics are properly classified

### Testing Patterns
```python
def test_indexer_metrics():
    # Setup
    registry = MetricsRegistry("test-service")
    metrics = IndexerMetrics(registry, "test", "balance_transfers")
    
    # Test metric recording
    metrics.record_block_processed(12345, 1.5)
    
    # Validate metrics
    assert registry.get_metric_value('indexer_blocks_processed_total') == 1
    assert registry.get_metric_value('indexer_current_block_height') == 12345
```

## Integration with Alerting

### Prometheus Alert Rules
```yaml
groups:
  - name: chainswarm_indexers
    rules:
      - alert: IndexerHighErrorRate
        expr: rate(indexer_failed_events_total[5m]) > 0.1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High error rate in {{ $labels.indexer }} indexer"
          
      - alert: IndexerLagging
        expr: indexer_blocks_behind_latest > 100
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Indexer {{ $labels.indexer }} is lagging behind"
```

### Grafana Dashboard Queries
```promql
# Processing rate
rate(indexer_blocks_processed_total[5m])

# Error rate by type
rate(service_errors_total[5m]) by (error_type)

# 95th percentile response time
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Current lag
indexer_blocks_behind_latest
```

## Common Patterns

### Service Health Metrics
```python
def update_service_health(self, healthy: bool):
    """Update service health based on various factors"""
    self.metrics_registry.set_health_status(healthy)
    
    # Additional health indicators
    if hasattr(self, 'connection_pool'):
        self.connection_health.set(1 if self.connection_pool.is_healthy() else 0)
```

### Rate Limiting Metrics
```python
def record_rate_limit_hit(self, client_ip: str, endpoint: str):
    self.rate_limit_hits.labels(endpoint=endpoint).inc()
    # Don't use client_ip as label (high cardinality)
    
def record_rate_limit_bypass(self, endpoint: str, reason: str):
    self.rate_limit_bypassed.labels(endpoint=endpoint, reason=reason).inc()
```

### Database Connection Metrics
```python
def monitor_connection_pool(self):
    """Monitor database connection pool health"""
    self.db_active_connections.set(self.pool.active_connections)
    self.db_idle_connections.set(self.pool.idle_connections)
    self.db_max_connections.set(self.pool.max_connections)
```

## Migration from Logs to Metrics

### Identify Candidates
Look for log patterns like:
```python
# These should become metrics
logger.info(f"Processed {count} items")  # → Counter
logger.info(f"Processing took {duration}s")  # → Histogram
logger.info(f"Queue size: {size}")  # → Gauge
logger.info(f"Success rate: {rate}%")  # → Calculated metric
```

### Conversion Examples
```python
# Before (logs)
logger.info(f"Processed {len(blocks)} blocks in {duration:.2f}s")

# After (metrics)
self.metrics.record_block_processed(block_height, duration)
# Metrics automatically track count, duration, and rates
```

## Future Considerations

- **Custom metrics**: Add business-specific metrics as needed
- **Distributed tracing**: Integrate with tracing systems
- **Cost optimization**: Monitor metrics cardinality and storage costs
- **Advanced analytics**: Use metrics for capacity planning and optimization