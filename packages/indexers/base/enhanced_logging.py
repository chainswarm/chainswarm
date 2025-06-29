"""
Simplified logging infrastructure for ChainSwarm.

This module provides simple logging capabilities with auto-detection of service names
and optional correlation ID support for API requests. Uses standard loguru patterns.
"""

import os
import sys
import time
import uuid
import inspect
import threading
from typing import Optional
from loguru import logger


# Thread-local storage for correlation IDs (only used for API requests)
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

def setup_logger(service_name: str):
    """
    Setup simple logger with auto-detection of service name.
    
    Args:
        service_name: Optional service name. If not provided, auto-detects from file path.
    """

    def patch_record(record):
        record["extra"]["service"] = service_name
        correlation_id = get_correlation_id()
        if correlation_id:
            record["extra"]["correlation_id"] = correlation_id
        record["extra"]["timestamp"] = time.time()
        return True

    # Get the absolute path to the project root directory
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    logs_dir = os.path.join(project_root, "logs")
    
    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    logger.remove()

    # File logger with JSON serialization for Loki ingestion
    logger.add(
        os.path.join(logs_dir, f"{service_name}.log"),
        rotation="500 MB",
        level="INFO",
        filter=patch_record,
        serialize=True,
        format="{time} | {level} | {extra[service]} | {message} | {extra}"
    )

    # Console logger with human-readable format
    console_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{extra[service]}</cyan> | {message} | <white>{extra}</white>"
    if get_correlation_id():
        console_format += " | <yellow>{extra[correlation_id]}</yellow>"
    
    logger.add(
        sys.stdout,
        format=console_format,
        level="INFO",
        filter=patch_record,
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )
    
    return service_name
