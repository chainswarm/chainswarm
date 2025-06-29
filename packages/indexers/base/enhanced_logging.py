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