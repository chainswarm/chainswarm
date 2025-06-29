# ADR-001 Implementation Plan: Enhanced Logging Infrastructure

## Overview

This document provides the detailed implementation plan for the logging strategy redesign outlined in ADR-001. It includes all code files that need to be created or modified to implement the new metrics-first logging approach.

## Implementation Files

### 1. Enhanced Logging Infrastructure

#### File: `packages/indexers/base/enhanced_logging.py`

```python
"""
Enhanced logging infrastructure for ChainSwarm.

This module provides enhanced logging capabilities that complement the Prometheus
metrics system by focusing on qualitative insights and context-rich error logging.
"""

import os
import sys
import time
import uuid
import traceback
import threading
from typing import Dict, Any, Optional
from loguru import logger
import psutil


# Thread-local storage for correlation IDs
_correlation_context = threading.local()


def generate_correlation_id() -> str:
    """Generate a unique correlation ID for request tracing."""
    return f"req_{uuid.uuid4().hex[:12]}"


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID from thread-local storage."""
    return getattr(_correlation_context, 'correlation_id', None)


def set_correlation_id(correlation_id: str):
    """Set the correlation ID in thread-local storage."""
    _correlation_context.correlation_id = correlation_id


def get_system_state() -> Dict[str, Any]:
    """Get current system state for error context."""
    try:
        process = psutil.Process()
        return {
            "memory_usage_mb": round(process.memory_info().rss / 1024 / 1024, 2),
            "cpu_percent": process.cpu_percent(),
            "open_files": len(process.open_files()),
            "threads": process.num_threads(),
            "timestamp": time.time()
        }
    except Exception:
        return {"error": "unable_to_get_system_state"}


def setup_enhanced_logger(service_name: str):
    """
    Setup enhanced logger with correlation ID support and structured context.
    
    Args:
        service_name: Name of the service (e.g., 'substrate-torus-balance-transfers')
    """
    def patch_record(record):
        record["extra"]["service"] = service_name
        record["extra"]["correlation_id"] = get_correlation_id() or "no_correlation"
        record["extra"]["timestamp"] = time.time()
        return True

    # Get the absolute path to the project root directory
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    logs_dir = os.path.join(project_root, "logs")
    
    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    logger.remove()

    # File logger with JSON serialization for Loki ingestion
    # Changed level from DEBUG to INFO to reduce volume
    logger.add(
        os.path.join(logs_dir, f"{service_name}.log"),
        rotation="500 MB",
        level="INFO",  # Changed from DEBUG
        filter=patch_record,
        serialize=True,
        format="{time} | {level} | {extra[service]} | {extra[correlation_id]} | {message} | {extra}"
    )

    # Console logger with human-readable format
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{extra[service]}</cyan> | <blue>{extra[correlation_id]}</blue> | <white>{message}</white>",
        level="INFO",  # Changed from DEBUG
        filter=patch_record,
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )


class ErrorContextManager:
    """
    Enhanced error context manager for structured error logging.
    
    Provides methods to log errors with rich context including correlation IDs,
    system state, and business context.
    """
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.correlation_id = None
    
    def start_operation(self, operation_name: str, **context) -> 'OperationContext':
        """
        Start a new operation with correlation tracking.
        
        Args:
            operation_name: Name of the operation being performed
            **context: Additional context for the operation
            
        Returns:
            OperationContext: Context manager for the operation
        """
        correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)
        return OperationContext(correlation_id, operation_name, context)
    
    def log_error(self, message: str, error: Exception, **context):
        """
        Log an error with enhanced context.
        
        Args:
            message: Human-readable error message
            error: The exception that occurred
            **context: Additional context for the error
        """
        logger.error(
            message,
            extra={
                "error_type": type(error).__name__,
                "error_message": str(error),
                "correlation_id": get_correlation_id(),
                "service": self.service_name,
                "system_state": get_system_state(),
                "stack_trace": traceback.format_exc(),
                **context
            }
        )
    
    def log_business_decision(self, decision: str, reason: str, **context):
        """
        Log a business logic decision.
        
        Args:
            decision: The decision that was made
            reason: The reason for the decision
            **context: Additional context for the decision
        """
        logger.info(
            f"Business decision: {decision}",
            extra={
                "decision": decision,
                "reason": reason,
                "correlation_id": get_correlation_id(),
                "service": self.service_name,
                **context
            }
        )
    
    def log_service_lifecycle(self, event: str, **context):
        """
        Log service lifecycle events.
        
        Args:
            event: The lifecycle event (start, stop, config_change, etc.)
            **context: Additional context for the event
        """
        logger.info(
            f"Service lifecycle: {event}",
            extra={
                "lifecycle_event": event,
                "correlation_id": get_correlation_id(),
                "service": self.service_name,
                "timestamp": time.time(),
                **context
            }
        )


class OperationContext:
    """Context manager for tracking operations with correlation IDs."""
    
    def __init__(self, correlation_id: str, operation_name: str, context: Dict[str, Any]):
        self.correlation_id = correlation_id
        self.operation_name = operation_name
        self.context = context
        self.start_time = time.time()
    
    def __enter__(self):
        set_correlation_id(self.correlation_id)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clear correlation ID when operation completes
        set_correlation_id(None)
        
        if exc_type is not None:
            # Log operation failure
            logger.error(
                f"Operation failed: {self.operation_name}",
                extra={
                    "operation": self.operation_name,
                    "correlation_id": self.correlation_id,
                    "duration": time.time() - self.start_time,
                    "error_type": exc_type.__name__,
                    "error_message": str(exc_val),
                    "context": self.context,
                    "stack_trace": traceback.format_exc()
                }
            )


def classify_error(error: Exception) -> str:
    """
    Classify errors into categories for better metrics and alerting.
    
    Args:
        error: The exception to classify
        
    Returns:
        str: Error category
    """
    error_type = type(error).__name__
    error_message = str(error).lower()
    
    if 'connection' in error_type.lower() or 'timeout' in error_type.lower():
        return 'connection_error'
    elif 'validation' in error_type.lower() or 'valueerror' in error_type:
        return 'validation_error'
    elif 'database' in error_type.lower() or 'sql' in error_message:
        return 'database_error'
    elif 'substrate' in error_type.lower() or 'rpc' in error_message:
        return 'substrate_error'
    elif 'permission' in error_message or 'auth' in error_message:
        return 'authorization_error'
    else:
        return 'unknown_error'


# Convenience functions for common logging patterns
def log_service_start(service_name: str, **config):
    """Log service startup with configuration."""
    error_ctx = ErrorContextManager(service_name)
    error_ctx.log_service_lifecycle(
        "service_start",
        configuration=config,
        pid=os.getpid()
    )


def log_service_stop(service_name: str, **context):
    """Log service shutdown."""
    error_ctx = ErrorContextManager(service_name)
    error_ctx.log_service_lifecycle(
        "service_stop",
        **context
    )


def log_configuration_change(service_name: str, old_config: Dict, new_config: Dict):
    """Log configuration changes."""
    error_ctx = ErrorContextManager(service_name)
    error_ctx.log_service_lifecycle(
        "configuration_change",
        old_configuration=old_config,
        new_configuration=new_config
    )
```

#### File: `packages/api/middleware/correlation_middleware.py`

```python
"""
Correlation ID middleware for FastAPI applications.

This middleware ensures that all API requests have correlation IDs for tracing
and links them to the enhanced logging system.
"""

import time
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from packages.indexers.base.enhanced_logging import generate_correlation_id, set_correlation_id


class CorrelationMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle correlation IDs for request tracing.
    
    This middleware:
    1. Extracts correlation ID from request headers or generates a new one
    2. Sets the correlation ID in thread-local storage for logging
    3. Adds the correlation ID to response headers
    4. Ensures all logs within the request context include the correlation ID
    """
    
    async def dispatch(self, request: Request, call_next):
        # Extract or generate correlation ID
        correlation_id = (
            request.headers.get("X-Correlation-ID") or 
            request.headers.get("x-correlation-id") or
            generate_correlation_id()
        )
        
        # Store in request state and thread-local storage
        request.state.correlation_id = correlation_id
        set_correlation_id(correlation_id)
        
        # Add request start time for duration calculation
        request.state.start_time = time.time()
        
        try:
            # Process the request
            response = await call_next(request)
            
            # Add correlation ID to response headers
            response.headers["X-Correlation-ID"] = correlation_id
            
            return response
            
        except Exception as e:
            # Ensure correlation ID is in error response
            response = Response(
                content=f"Internal server error: {str(e)}",
                status_code=500,
                headers={"X-Correlation-ID": correlation_id}
            )
            return response
        
        finally:
            # Clear correlation ID from thread-local storage
            set_correlation_id(None)


def get_request_context(request: Request) -> dict:
    """
    Extract request context for logging.
    
    Args:
        request: FastAPI request object
        
    Returns:
        dict: Request context for logging
    """
    return {
        "method": request.method,
        "url": str(request.url),
        "path": request.url.path,
        "query_params": dict(request.query_params),
        "client_ip": request.client.host if request.client else "unknown",
        "user_agent": request.headers.get("user-agent", "unknown"),
        "correlation_id": getattr(request.state, 'correlation_id', None),
        "processing_time": time.time() - getattr(request.state, 'start_time', time.time())
    }


def sanitize_params(params: dict) -> dict:
    """
    Sanitize request parameters for logging (remove sensitive data).
    
    Args:
        params: Request parameters
        
    Returns:
        dict: Sanitized parameters
    """
    sensitive_keys = {'password', 'token', 'api_key', 'secret', 'auth'}
    sanitized = {}
    
    for key, value in params.items():
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            sanitized[key] = "[REDACTED]"
        else:
            sanitized[key] = value
    
    return sanitized
```

### 2. Refactored Indexer Logging

#### File: `packages/indexers/substrate/balance_transfers/balance_transfers_indexer_base.py` (Modifications)

```python
# Add these imports at the top
from packages.indexers.base.enhanced_logging import (
    ErrorContextManager, 
    setup_enhanced_logger,
    log_service_start,
    classify_error
)

class BalanceTransfersIndexerBase:
    def __init__(self, network: str):
        # ... existing initialization ...
        
        # Setup enhanced logging
        self.service_name = f"substrate-{network}-balance-transfers"
        setup_enhanced_logger(self.service_name)
        self.error_ctx = ErrorContextManager(self.service_name)
        
        # Log service startup with configuration
        log_service_start(
            self.service_name,
            network=network,
            batch_size=getattr(self, 'batch_size', 'unknown'),
            database_config=self._get_db_config_summary()
        )
    
    def _get_db_config_summary(self) -> dict:
        """Get database configuration summary for logging."""
        return {
            "host": getattr(self, 'host', 'unknown'),
            "database": getattr(self, 'database', 'unknown'),
            "connection_pool_size": getattr(self, 'pool_size', 'unknown')
        }
    
    def _initialize_tables(self):
        """Initialize database tables with enhanced error logging."""
        try:
            # ... existing table initialization code ...
            
            # REMOVE: All success logging like:
            # logger.info(f"✓ Chunk {i + 1} executed successfully")
            # logger.info("Schema execution complete: ...")
            
            # KEEP: Only business decisions
            self.error_ctx.log_business_decision(
                "schema_initialization_completed",
                "tables_created_or_verified",
                schema_chunks_processed=len(chunks),
                tables_affected=self._get_table_names()
            )
            
        except Exception as e:
            # ENHANCED: Error logging with full context
            self.error_ctx.log_error(
                "Database schema initialization failed",
                error=e,
                operation="schema_initialization",
                schema_file=schema_path,
                database_config=self._get_db_config_summary(),
                error_category=classify_error(e)
            )
            raise
    
    def bulk_insert_blocks(self, blocks):
        """Bulk insert blocks with enhanced error logging."""
        try:
            # ... existing bulk insert code ...
            
            # REMOVE: Success logging like:
            # logger.success(f"Bulk inserted {len(sorted_blocks)} blocks with {len(all_balance_transfers)} transfers")
            
            # Metrics handle the counts and performance data
            
        except Exception as e:
            # ENHANCED: Error logging with context
            self.error_ctx.log_error(
                "Bulk insert operation failed",
                error=e,
                operation="bulk_insert",
                block_count=len(blocks),
                transfer_count=len(all_balance_transfers) if 'all_balance_transfers' in locals() else 0,
                first_block_height=min(block['block_height'] for block in blocks) if blocks else None,
                last_block_height=max(block['block_height'] for block in blocks) if blocks else None,
                database_state=self._get_db_connection_state(),
                error_category=classify_error(e)
            )
            raise
    
    def _get_db_connection_state(self) -> dict:
        """Get database connection state for error context."""
        try:
            return {
                "active_connections": getattr(self.client, 'active_connections', 'unknown'),
                "connection_healthy": self._test_connection(),
                "last_successful_query": getattr(self, '_last_query_time', 'unknown')
            }
        except Exception:
            return {"error": "unable_to_get_connection_state"}
    
    def _test_connection(self) -> bool:
        """Test database connection health."""
        try:
            # Simple query to test connection
            self.client.query("SELECT 1")
            return True
        except Exception:
            return False
```

#### File: `packages/indexers/substrate/balance_transfers/balance_transfers_consumer.py` (Modifications)

```python
# Add these imports
from packages.indexers.base.enhanced_logging import (
    ErrorContextManager,
    setup_enhanced_logger,
    log_service_start,
    log_service_stop
)

class BalanceTransfersConsumer:
    def __init__(self, network: str, start_height: int = None, batch_size: int = 100):
        # ... existing initialization ...
        
        # Setup enhanced logging
        self.service_name = f"substrate-{network}-balance-transfers-consumer"
        setup_enhanced_logger(self.service_name)
        self.error_ctx = ErrorContextManager(self.service_name)
        
        # Log service startup
        log_service_start(
            self.service_name,
            network=network,
            start_height=start_height,
            batch_size=batch_size,
            continuous_mode=True
        )
    
    def run(self):
        """Main consumer loop with enhanced logging."""
        try:
            # REMOVE: Progress logging like:
            # logger.info(f"Starting balance transfers consumer from block {start_height}")
            # logger.info(f"Batch size for block processing: {self.batch_size}")
            
            # Business decision logging
            if self.start_height:
                self.error_ctx.log_business_decision(
                    "resume_from_specific_height",
                    "start_height_provided",
                    start_height=self.start_height
                )
            else:
                last_height = self._get_last_processed_height()
                self.error_ctx.log_business_decision(
                    "resume_from_last_processed",
                    "no_start_height_provided",
                    last_processed_height=last_height
                )
            
            while not self.terminate_event.is_set():
                try:
                    # ... existing processing logic ...
                    
                    # REMOVE: All progress logging like:
                    # logger.info(f"Fetching blocks from {start_height} to {end_height}")
                    # logger.info(f"Processing {len(blocks)} blocks for transfer extraction")
                    # logger.success(f"Processed {len(blocks)} blocks for transfer extraction")
                    
                    # Only log significant events or business decisions
                    if not blocks:
                        # Only log if this is unusual (e.g., multiple consecutive empty responses)
                        self._handle_empty_block_response(start_height, end_height)
                    
                except Exception as e:
                    # ENHANCED: Error logging with full context
                    self.error_ctx.log_error(
                        "Block processing batch failed",
                        error=e,
                        operation="batch_processing",
                        start_height=start_height,
                        end_height=end_height,
                        batch_size=self.batch_size,
                        processing_state=self._get_processing_state(),
                        error_category=classify_error(e)
                    )
                    time.sleep(5)  # Brief pause before continuing
            
            # Log service shutdown
            log_service_stop(
                self.service_name,
                reason="terminate_event_received",
                last_processed_height=self._get_last_processed_height()
            )
            
        except KeyboardInterrupt:
            log_service_stop(
                self.service_name,
                reason="keyboard_interrupt"
            )
        except Exception as e:
            self.error_ctx.log_error(
                "Fatal consumer error",
                error=e,
                operation="consumer_main_loop",
                error_category=classify_error(e)
            )
        finally:
            self._cleanup()
    
    def _handle_empty_block_response(self, start_height: int, end_height: int):
        """Handle empty block responses with smart logging."""
        # Only log if we've seen multiple consecutive empty responses
        if not hasattr(self, '_empty_response_count'):
            self._empty_response_count = 0
        
        self._empty_response_count += 1
        
        # Log warning after 3 consecutive empty responses
        if self._empty_response_count >= 3:
            logger.warning(
                "Multiple consecutive empty block responses detected",
                extra={
                    "consecutive_empty_responses": self._empty_response_count,
                    "height_range": f"{start_height}-{end_height}",
                    "possible_causes": ["network_lag", "indexer_ahead_of_chain", "node_sync_issues"]
                }
            )
            self._empty_response_count = 0  # Reset counter
    
    def _get_processing_state(self) -> dict:
        """Get current processing state for error context."""
        return {
            "last_processed_height": self._get_last_processed_height(),
            "terminate_event_set": self.terminate_event.is_set(),
            "batch_size": self.batch_size,
            "active_connections": self._get_connection_count()
        }
```

### 3. Enhanced API Logging

#### File: `packages/api/main.py` (Modifications)

```python
# Add these imports
from packages.indexers.base.enhanced_logging import setup_enhanced_logger, ErrorContextManager
from packages.api.middleware.correlation_middleware import CorrelationMiddleware

# Modify the setup section
network = os.getenv("NETWORK", "torus").lower()
service_name = f"{network}-api"

# Setup enhanced logging instead of basic logging
setup_enhanced_logger(service_name)
error_ctx = ErrorContextManager(service_name)

# Add correlation middleware BEFORE other middleware
app.add_middleware(CorrelationMiddleware)

# Modify the metrics endpoint error handling
async def aggregated_metrics_endpoint():
    """Prometheus metrics endpoint with aggregated metrics from all services"""
    try:
        # ... existing metrics aggregation code ...
        return Response(content=combined_metrics, media_type=CONTENT_TYPE_LATEST)
        
    except Exception as e:
        # ENHANCED: Error logging with context
        error_ctx.log_error(
            "Metrics aggregation failed",
            error=e,
            operation="metrics_aggregation",
            service_registries_count=len(_service_registries),
            error_category=classify_error(e)
        )
        return JSONResponse(
            status_code=503,
            content={"error": "Metrics aggregation failed"}
        )
```

#### File: `packages/api/services/balance_transfers_service.py` (Example modifications)

```python
# Add enhanced error logging to service methods
from packages.indexers.base.enhanced_logging import ErrorContextManager, get_correlation_id, classify_error

class BalanceTransfersService:
    def __init__(self):
        # ... existing initialization ...
        self.error_ctx = ErrorContextManager("balance-transfers-service")
    
    async def get_balance_transfers(self, address: str, limit: int = 100):
        """Get balance transfers with enhanced error logging."""
        try:
            # ... existing query logic ...
            
            # REMOVE: Success logging like:
            # logger.info(f"Query executed successfully: {query}")
            
            return result
            
        except Exception as e:
            # ENHANCED: Error logging with request context
            self.error_ctx.log_error(
                "Balance transfers query failed",
                error=e,
                operation="get_balance_transfers",
                query_parameters={
                    "address": address,
                    "limit": limit
                },
                correlation_id=get_correlation_id(),
                database_state=self._get_db_state(),
                error_category=classify_error(e)
            )
            raise
    
    def _get_db_state(self) -> dict:
        """Get database state for error context."""
        try:
            return {
                "connection_healthy": self._test_connection(),
                "active_queries": getattr(self.client, 'active_queries', 'unknown'),
                "last_successful_query": getattr(self, '_last_query_time', 'unknown')
            }
        except Exception:
            return {"error": "unable_to_get_db_state"}
```

### 4. Migration Script

#### File: `scripts/migrate_logging.py`

```python
#!/usr/bin/env python3
"""
Migration script to update existing logging patterns to the new enhanced logging system.

This script:
1. Identifies old logging patterns that should be removed or enhanced
2. Provides a report of changes needed
3. Can optionally apply automatic fixes for simple patterns
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Dict, Tuple


class LoggingMigrationAnalyzer:
    """Analyzes code for logging patterns that need migration."""
    
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.patterns_to_remove = [
            r'logger\.info\(f?"Processing \{.*?\} .*?"\)',
            r'logger\.info\(f?"Processed \{.*?\} .*?"\)',
            r'logger\.success\(f?".*?completed.*?"\)',
            r'logger\.info\(f?"Query executed successfully.*?"\)',
            r'logger\.debug\(.*?\)',
            r'logger\.info\(f?"Batch \{.*?\} .*?"\)',
        ]
        
        self.patterns_to_enhance = [
            r'logger\.error\(f?".*?"\)',
            r'logger\.warning\(f?".*?"\)',
        ]
    
    def analyze_file(self, file_path: Path) -> Dict[str, List[Tuple[int, str]]]:
        """Analyze a single file for logging patterns."""
        results = {
            'remove': [],
            'enhance': [],
            'keep': []
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                
                # Check patterns to remove
                for pattern in self.patterns_to_remove:
                    if re.search(pattern, line):
                        results['remove'].append((line_num, line))
                        break
                
                # Check patterns to enhance
                for pattern in self.patterns_to_enhance:
                    if re.search(pattern, line):
                        results['enhance'].append((line_num, line))
                        break
        
        except Exception as e:
            print(f"Error analyzing {file_path}: {e}")
        
        return results
    
    def analyze_project(self) -> Dict[str, Dict]:
        """Analyze the entire project for logging patterns."""
        results = {}
        
        # Find all Python files
        python_files = list(self.project_root.rglob("*.py"))
        
        for file_path in python_files:
            # Skip migration script itself and test files
            if 'migrate_logging.py' in str(file_path) or '/tests/' in str(file_path):
                continue
            
            file_results = self.analyze_file(file_path)
            if any(file_results.values()):
                results[str(file_path)] = file_results
        
        return results
    
    def generate_report(self, analysis_results: Dict) -> str:
        """Generate a migration report."""
        report = []
        report.append("# Logging Migration Analysis Report")
        report.append(f"Generated: {datetime.now().isoformat()}")
        report.append("")
        
        total_remove = sum(len(data['remove']) for data in analysis_results.values())
        total_enhance = sum(len(data['enhance']) for data in analysis_results.values())
        
        report.append(f"## Summary")
        report.append(f"- Files to modify: {len(analysis_results)}")
        report.append(f"- Log entries to remove: {total_remove}")
        report.append(f"- Log entries to enhance: {total_enhance}")
        report.append("")
        
        for file_path, data in analysis_results.items():
            if not any(data.values()):
                continue
            
            report.append(f"## {file_path}")
            report.append("")
            
            if data['remove']:
                report.append("### Remove these log entries:")
                for line_num, line in data['remove']:
                    report.append(f"- Line {line_num}: `{line}`")
                report.append("")
            
            if data['enhance']:
                report.append("### Enhance these log entries:")
                for line_num, line in data['enhance']:
                    report.append(f"- Line {line_num}: `{line}`")
                report.append("")
        
        return "\n".join(report)


def main():
    """Main migration analysis function."""
    if len(sys.argv) != 2:
        print("Usage: python migrate_logging.py <project_root>")
        sys.exit(1)
    
    project_root = sys.argv[1]
    analyzer = LoggingMigrationAnalyzer(project_root)
    
    print("Analyzing project for logging patterns...")
    results = analyzer.analyze_project()
    
    print("Generating migration report...")
    report = analyzer.generate_report(results)
    
    # Write report to file
    report_file = Path(project_root) / "docs" / "architecture" / "logging-migration-report.md"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"Migration report written to: {report_file}")
    print(f"Found {len(results)} files that need migration")


if __name__ == "__main__":
    main()
```

## Implementation Steps

### Phase 1: Infrastructure (Week 1-2)
1. Create `packages/indexers/base/enhanced_logging.py`
2. Create `packages/api/middleware/correlation_middleware.py`
3. Update `packages/indexers/base/__init__.py` to export new functions
4. Add required dependencies to `requirements.txt`:
   - `psutil>=5.9.0` (for system state monitoring)

### Phase 2: Indexer Migration (Week 3-4)
1. Update all indexer base classes to use enhanced logging
2. Remove progress and success logging from consumers
3. Enhance error logging with full context
4. Add business decision logging

### Phase 3: API Migration (Week 5-6)
1. Add correlation middleware to all FastAPI applications
2. Update service classes with enhanced error logging
3. Remove success confirmations from API operations
4. Add request context to all error logs

### Phase 4: Testing & Validation (Week 7-8)
1. Run migration analysis script
2