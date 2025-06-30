#!/usr/bin/env python3
"""
Test script for the simplified logging approach.

This script tests the new simplified logging system that:
1. Auto-detects service names from file paths
2. Uses standard loguru patterns
3. Removes complex context manager classes
4. Supports correlation IDs for API requests
"""

import sys
import os
import time
from loguru import logger

# Add the packages directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'packages'))

from packages.indexers.base import (
    setup_logger, get_logger, log_service_start, log_service_stop,
    log_business_decision, log_error_with_context, classify_error,
    generate_correlation_id, set_correlation_id, get_correlation_id,
    auto_detect_service_name
)


def test_auto_detection():
    """Test auto-detection of service names"""
    print("\n=== Testing Auto-Detection ===")
    
    # Test auto-detection
    service_name = auto_detect_service_name()
    print(f"Auto-detected service name: {service_name}")
    
    # Setup logger with auto-detection
    detected_name = setup_logger()
    print(f"Setup logger detected name: {detected_name}")
    
    # Test get_logger function
    logger_instance, detected_name2 = get_logger()
    print(f"get_logger detected name: {detected_name2}")


def test_simple_logging():
    """Test simple logging patterns"""
    print("\n=== Testing Simple Logging ===")
    
    # Setup logger
    setup_logger()
    
    # Test basic logging
    logger.info("This is a simple info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Test logging with extra context
    logger.info(
        "Processing started",
        extra={
            "operation": "test_processing",
            "batch_size": 100,
            "network": "torus"
        }
    )


def test_convenience_functions():
    """Test convenience logging functions"""
    print("\n=== Testing Convenience Functions ===")
    
    # Test service lifecycle logging
    log_service_start(
        network="torus",
        batch_size=100,
        mode="test"
    )
    
    # Test business decision logging
    log_business_decision(
        "use_cached_data",
        "cache_hit_found",
        cache_key="test_key",
        cache_age_seconds=30
    )
    
    # Test error logging with context
    try:
        raise ValueError("This is a test error")
    except Exception as e:
        log_error_with_context(
            "Test error occurred",
            e,
            operation="test_operation",
            test_param="test_value",
            error_category=classify_error(e)
        )
    
    # Test service stop
    log_service_stop(reason="test_completed")


def test_correlation_ids():
    """Test correlation ID functionality"""
    print("\n=== Testing Correlation IDs ===")
    
    # Generate and set correlation ID
    correlation_id = generate_correlation_id()
    print(f"Generated correlation ID: {correlation_id}")
    
    set_correlation_id(correlation_id)
    retrieved_id = get_correlation_id()
    print(f"Retrieved correlation ID: {retrieved_id}")
    
    # Test logging with correlation ID
    setup_logger()  # This will pick up the correlation ID
    
    logger.info(
        "API request processed",
        extra={
            "endpoint": "/api/balance-transfers",
            "method": "GET",
            "status_code": 200
        }
    )
    
    # Clear correlation ID
    set_correlation_id(None)
    retrieved_id = get_correlation_id()
    print(f"Cleared correlation ID: {retrieved_id}")


def test_error_classification():
    """Test error classification"""
    print("\n=== Testing Error Classification ===")
    
    test_errors = [
        ConnectionError("Connection failed"),
        ValueError("Invalid value provided"),
        RuntimeError("Database connection lost"),
        Exception("Unknown error"),
        TimeoutError("Request timed out")
    ]
    
    for error in test_errors:
        category = classify_error(error)
        print(f"{type(error).__name__}: {category}")


def test_milestone_logging():
    """Test milestone logging pattern"""
    print("\n=== Testing Milestone Logging ===")
    
    setup_logger()
    
    # Simulate processing with milestone logging
    total_items = 50
    milestone_interval = 10
    last_milestone = 0
    
    for i in range(1, total_items + 1):
        # Simulate some work
        time.sleep(0.01)
        
        # Log milestone progress
        if i - last_milestone >= milestone_interval:
            logger.info(
                "Processing milestone reached",
                extra={
                    "items_processed": i,
                    "total_items": total_items,
                    "progress_percent": round((i / total_items) * 100, 1),
                    "milestone_interval": milestone_interval
                }
            )
            last_milestone = i
    
    logger.info(
        "Processing completed",
        extra={
            "total_items_processed": total_items,
            "final_status": "success"
        }
    )


def main():
    """Run all tests"""
    print("Testing Simplified Logging Approach")
    print("=" * 50)
    
    try:
        test_auto_detection()
        test_simple_logging()
        test_convenience_functions()
        test_correlation_ids()
        test_error_classification()
        test_milestone_logging()
        
        print("\n" + "=" * 50)
        print("✅ All tests completed successfully!")
        print("\nKey benefits of the simplified approach:")
        print("- Auto-detects service names from file paths")
        print("- Uses standard loguru patterns everyone knows")
        print("- No complex context manager classes")
        print("- Simple helper functions for common patterns")
        print("- Optional correlation ID support for API requests")
        print("- Strategic milestone logging reduces volume")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())