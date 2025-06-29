# ADR-001: Logging Strategy Redesign for Metrics-First Observability

## Status
Accepted

## Date
2025-06-29

## Context

ChainSwarm has implemented a comprehensive Prometheus metrics system that tracks quantitative data across all components (indexers, APIs, MCP server). With 297 logging entries across the codebase, we identified significant redundancy between logs and metrics, leading to:

1. **High log volume** with 80% of logs duplicating metric data
2. **Poor signal-to-noise ratio** in logs due to progress updates and success confirmations
3. **Insufficient error context** for effective troubleshooting
4. **Lack of correlation** between logs and metrics for incident investigation

### Current Logging Analysis

| Component | Log Count | Primary Issues |
|-----------|-----------|----------------|
| Indexers (Substrate) | 180 | Excessive progress logging, metric duplicates |
| API Components | 45 | Minimal error context, missing correlation IDs |
| Base Infrastructure | 35 | Verbose connection management |
| Services & Tools | 25 | Success confirmations, performance data |
| Middleware | 12 | Generic error messages |

### Metrics Coverage Analysis

Our Prometheus implementation already tracks:
- Block processing rates and counts (`indexer_blocks_processed_total`)
- Request metrics (`http_requests_total`, `http_request_duration_seconds`)
- Database operations (`indexer_database_operations_total`)
- Error rates (`service_errors_total`)
- System health (`service_health_status`)

## Decision

We will redesign the logging strategy to complement metrics by focusing on qualitative insights while eliminating quantitative redundancy.

### Core Principles

1. **Metrics for Quantitative, Logs for Qualitative**
   - Metrics: Counts, rates, durations, success/failure ratios
   - Logs: Context, business decisions, error details, troubleshooting information

2. **Context-Rich Error Logging**
   - Include correlation IDs, input parameters, system state
   - Provide full stack traces and error classification
   - Add relevant business context

3. **Strategic Information Logging**
   - Service lifecycle events (start, stop, configuration changes)
   - Business logic decisions with reasoning
   - Eliminate progress updates that metrics already track

### Logging Level Changes

| Level | Current Count | New Strategy | Reduction |
|-------|---------------|--------------|-----------|
| DEBUG | 15 | Remove entirely | 100% |
| INFO | 165 | Strategic only (33 entries) | 80% |
| WARNING | 45 | Enhanced context (45 entries) | 0% |
| ERROR | 65 | Enhanced context (65 entries) | 0% |
| SUCCESS | 7 | Remove (1 entry) | 85% |

**Total reduction: 70-80% log volume**

### Enhanced Error Logging Structure

```python
{
    "timestamp": "2025-06-29T08:30:00Z",
    "level": "ERROR",
    "service": "substrate-torus-balance-transfers",
    "correlation_id": "req_abc123def456",
    "message": "Human-readable message",
    "error_type": "SubstrateConnectionError",
    "context": {
        "block_height": 12345,
        "network": "torus",
        "input_params": {...},
        "system_state": {...}
    },
    "stack_trace": "..."
}
```

## Implementation Plan

### Phase 1: Infrastructure Enhancement
- Enhanced logger setup with correlation IDs
- Error context manager framework
- Structured logging standards

### Phase 2: Component Refactoring
- **Indexers**: Remove 85% of info logs, enhance error context
- **APIs**: Add correlation IDs, enhance error logging
- **Database**: Remove success logging, enhance error context

### Phase 3: Integration
- Log-based alerting for qualitative issues
- Correlation between metrics anomalies and log entries
- Enhanced troubleshooting workflows

## Consequences

### Positive
- **Reduced log volume** by 70-80%, improving storage costs and analysis speed
- **Enhanced error context** reducing troubleshooting time by 50-70%
- **Clear separation** between quantitative (metrics) and qualitative (logs) data
- **Improved correlation** between metrics alerts and log investigation
- **Better signal-to-noise ratio** in logs for operational teams

### Negative
- **Migration effort** required across 297 log entries
- **Learning curve** for developers adapting to new patterns
- **Temporary inconsistency** during migration period

### Risks and Mitigations
- **Risk**: Loss of historical log patterns during migration
  - **Mitigation**: Gradual rollout with validation at each step
- **Risk**: Insufficient error context in new format
  - **Mitigation**: Comprehensive testing and feedback loops
- **Risk**: Developer resistance to new patterns
  - **Mitigation**: Clear guidelines and examples in documentation

## Alternatives Considered

### Alternative 1: Keep Current Logging + Add Metrics
- **Rejected**: Would maintain redundancy and high log volume
- **Issue**: Storage costs and analysis complexity would remain high

### Alternative 2: Metrics-Only Approach
- **Rejected**: Metrics cannot provide sufficient context for troubleshooting
- **Issue**: Would lose valuable qualitative information needed for debugging

### Alternative 3: Structured Logging Without Volume Reduction
- **Rejected**: Would improve format but not address volume issues
- **Issue**: High storage costs and noise would persist

## Implementation Details

### New Infrastructure Components
1. **Enhanced Logger Setup** (`packages/indexers/base/enhanced_logging.py`)
2. **Error Context Manager** (`packages/indexers/base/error_context.py`)
3. **Correlation Middleware** (`packages/api/middleware/correlation_middleware.py`)

### Migration Strategy
- **Week 1-2**: Infrastructure implementation
- **Week 3-4**: Indexer refactoring
- **Week 5-6**: API refactoring
- **Week 7-8**: Testing and validation

### Success Metrics
- Log volume reduction of 70-80%
- All errors include correlation_id and context
- Zero duplication between logs and metrics
- Reduced mean time to resolution (MTTR) for incidents

## References
- [Logging Guidelines for LLM Agents](../llm/logging-guidelines.md)
- [Metrics Guidelines for LLM Agents](../llm/metrics-guidelines.md)
- [Prometheus Metrics Plan](../observability/prometheus-metrics-plan.md)
- [Current Logging Analysis](../observability/logging-analysis-2025-06-29.md)

## Approval
- **Architect**: Approved
- **Engineering Lead**: Pending
- **Operations Team**: Pending

---

*This ADR establishes the foundation for a metrics-first observability strategy that reduces log noise while enhancing troubleshooting capabilities through context-rich error logging and strategic information capture.*