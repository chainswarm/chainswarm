# ChainSwarm Logging Guidelines for LLM Agents

## Overview

This document provides comprehensive guidelines for LLM agents working on the ChainSwarm project regarding logging practices. These guidelines ensure consistency with our metrics-first observability strategy and maintain high-quality, actionable logs.

## Core Principles

### 1. Metrics for Quantitative, Logs for Qualitative
- **Use Metrics For**: Counts, rates, durations, success/failure ratios
- **Use Logs For**: Context, business decisions, error details, troubleshooting information

### 2. Context-Rich Error Logging
- Always include correlation IDs, input parameters, system state
- Provide full stack traces and error classification
- Include relevant business context

### 3. Strategic Information Logging
- Log service lifecycle events (start, stop, configuration changes)
- Log business logic decisions with reasoning
- Avoid progress updates that metrics already track

## When to Log vs Use Metrics

| Scenario | Use Metrics | Use Logs | Example |
|----------|-------------|----------|---------|
| Request count | ✅ | ❌ | `http_requests_total` |
| Processing time | ✅ | ❌ | `block_processing_duration_seconds` |
| Success/failure rates | ✅ | ❌ | `indexer_blocks_processed_total` |
| Error details | ❌ | ✅ | Full error context with parameters |
| Business decisions | ❌ | ✅ | "Genesis initialization skipped - existing records found" |
| Configuration changes | ❌ | ✅ | Service startup with config summary |
| Service lifecycle | ❌ | ✅ | "Indexer starting with batch_size=100" |
| Debugging context | ❌ | ✅ | Error with system state and input data |

## Logging Levels

### ERROR - Enhanced Context Required
**Use for**: Failures that require investigation
**Required fields**: error_type, correlation_id, input_params, system_state, stack_trace

```python
logger.error(
    "Block processing failed",
    extra={
        "error_type": type(e).__name__,
        "block_height": block_height,
        "block_hash": block_hash,
        "network": self.network,
        "indexer": self.indexer_type,
        "input_params": {
            "start_height": start_height,
            "end_height": end_height,
            "batch_size": len(blocks)
        },
        "system_state": {
            "memory_usage": get_memory_usage(),
            "active_connections": get_connection_count()
        },
        "correlation_id": correlation_id,
        "stack_trace": traceback.format_exc()
    }
)
```

### WARNING - Contextual Alerts
**Use for**: Issues that don't stop processing but need attention
**Required fields**: correlation_id, context explaining why it's concerning

```python
logger.warning(
    "Substrate connection unstable - multiple retries required",
    extra={
        "method": method.__name__,
        "retry_count": retry_count,
        "error_pattern": classify_error(e),
        "connection_state": get_connection_health(),
        "correlation_id": correlation_id
    }
)
```

### INFO - Strategic Information Only
**Use for**: Business decisions, service lifecycle, configuration changes
**Avoid**: Progress updates, success confirmations, counts

```python
# ✅ GOOD - Business decision
logger.info(
    "Genesis balance initialization required",
    extra={
        "network": self.network,
        "asset": self.asset,
        "genesis_file": file_path,
        "existing_records": record_count,
        "decision_reason": "no_existing_genesis_records"
    }
)

# ❌ BAD - Metrics handle this
logger.info(f"Processing {len(blocks)} blocks for transfer extraction")
```

### DEBUG - Do Not Use
**Status**: Deprecated - Remove all debug logs
**Replacement**: Use metrics or eliminate entirely

## Component-Specific Guidelines

### Indexers
**Remove**:
- Progress logging ("Processing X blocks", "Batch Y completed")
- Success confirmations ("Successfully processed X records")
- Performance data (durations, rates, counts)
- Connection retry details (unless pattern indicates problem)

**Keep & Enhance**:
- Service startup/shutdown with configuration
- Business logic decisions (genesis initialization, schema changes)
- Error logs with full context
- Configuration changes

### APIs
**Add**:
- Request correlation IDs to all endpoints
- Enhanced error context with request details
- Client information for troubleshooting

**Remove**:
- Success confirmations for normal operations
- Query execution confirmations

**Enhance**:
- Error logs with request context, client info, processing time

### Database Operations
**Remove**:
- Query success logging
- Performance data (duration, row counts)
- Connection success confirmations

**Keep**:
- Schema changes and migrations
- Connection failures with context
- Data validation errors

**Enhance**:
- Error logs with query context, connection state, data samples

## Structured Logging Format

### Standard Structure
```python
{
    "timestamp": "2025-06-29T08:30:00Z",
    "level": "ERROR",
    "service": "substrate-torus-balance-transfers",
    "correlation_id": "req_abc123def456",
    "message": "Human-readable message",
    "error_type": "SubstrateConnectionError",  # For errors
    "context": {
        # Relevant business/technical context
        "block_height": 12345,
        "network": "torus",
        "input_params": {...},
        "system_state": {...}
    },
    "stack_trace": "..."  # For errors
}
```

### Required Fields by Level
- **ERROR**: correlation_id, error_type, context, stack_trace
- **WARNING**: correlation_id, context
- **INFO**: correlation_id (if part of request flow), context

## Code Patterns

### Error Context Manager
```python
from packages.indexers.base.error_context import ErrorContextManager

error_ctx = ErrorContextManager(service_name)

try:
    # Operation
    pass
except Exception as e:
    error_ctx.log_error(
        "Operation failed",
        error=e,
        operation="block_processing",
        block_height=height,
        additional_context={...}
    )
```

### Correlation ID Usage
```python
# In API endpoints
correlation_id = request.state.correlation_id

# In indexers
correlation_id = generate_correlation_id()

# Always include in logs
logger.error("Message", extra={"correlation_id": correlation_id, ...})
```

### Business Decision Logging
```python
def log_business_decision(decision, reason, **context):
    logger.info(
        f"Business decision: {decision}",
        extra={
            "decision": decision,
            "reason": reason,
            "correlation_id": get_correlation_id(),
            **context
        }
    )

# Usage
log_business_decision(
    "skip_genesis_initialization",
    "existing_records_found",
    network=self.network,
    existing_count=count
)
```

## Migration Checklist

When refactoring existing logging:

### Remove These Patterns
- [ ] `logger.info(f"Processing {count} items")`
- [ ] `logger.success(f"Completed {operation}")`
- [ ] `logger.debug(...)` (all debug logs)
- [ ] `logger.info(f"Query executed successfully")`
- [ ] Progress/batch completion logs

### Enhance These Patterns
- [ ] Simple error logs → Rich context errors
- [ ] Generic warnings → Specific contextual warnings
- [ ] Service startup → Include configuration summary
- [ ] Connection errors → Include connection state

### Add These Patterns
- [ ] Correlation IDs to all request flows
- [ ] Business decision logging
- [ ] Enhanced error context
- [ ] Service lifecycle logging

## Testing Logging Changes

### Validation Checklist
- [ ] Log volume reduced by 70-80%
- [ ] All errors include correlation_id and context
- [ ] No duplicate information between logs and metrics
- [ ] Business decisions are clearly logged
- [ ] Error logs provide sufficient troubleshooting context

### Log Analysis Queries
```bash
# Check log volume reduction
grep -c "INFO" old_logs.log vs new_logs.log

# Verify error context completeness
jq 'select(.level=="ERROR") | has("correlation_id", "error_type", "context", "stack_trace")' logs.json

# Confirm no metric duplicates
grep -E "(Processing|Completed|Successfully)" logs.log | wc -l  # Should be minimal
```

## Common Mistakes to Avoid

1. **Logging what metrics already track** - Check existing metrics before adding logs
2. **Missing correlation IDs** - Always include for traceability
3. **Insufficient error context** - Include input params, system state
4. **Generic error messages** - Be specific about what failed and why
5. **Progress logging** - Let metrics handle counts and progress
6. **Debug logs in production** - Remove all debug level logging

## Integration with Metrics

### Complementary Usage
```python
# Metrics track the "what" and "how much"
self.metrics.record_error("database_timeout", "balance_transfers")

# Logs provide the "why" and "context"
logger.error(
    "Database timeout during high load period",
    extra={
        "error_details": {...},
        "metrics_context": {
            "current_processing_rate": get_current_metric("processing_rate"),
            "error_rate_last_5min": get_current_metric("error_rate_5m"),
            "memory_usage_percent": get_current_metric("memory_usage")
        }
    }
)
```

### Alert Correlation
- Metrics trigger alerts based on thresholds
- Logs provide context for investigating alerts
- Correlation IDs link metric anomalies to log entries

## Future Considerations

- **Log aggregation**: Structured logs work well with Loki/ELK
- **Alerting**: Log-based alerts for qualitative issues
- **Correlation**: Link logs to distributed traces
- **Analysis**: Use structured data for pattern analysis