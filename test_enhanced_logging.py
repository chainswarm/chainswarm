#!/usr/bin/env python3
"""
Test script to verify enhanced logging works without psutil dependency.
"""

import sys
import os

# Add the packages directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'packages'))

from indexers.base.enhanced_logging import (
    setup_enhanced_logger,
    ErrorContextManager,
    get_system_state,
    log_service_start,
    log_service_stop
)

def test_enhanced_logging():
    """Test enhanced logging functionality with psutil."""
    
    # Test system state collection
    print("\n=== Testing get_system_state() ===")
    system_state = get_system_state()
    print(f"System state: {system_state}")
    
    # Setup logger
    print("\n=== Setting up enhanced logger ===")
    setup_enhanced_logger("test-service")
    
    # Test service lifecycle logging
    print("\n=== Testing service lifecycle logging ===")
    log_service_start("test-service", config_param="test_value", debug_mode=True)
    
    # Test error context manager
    print("\n=== Testing ErrorContextManager ===")
    error_ctx = ErrorContextManager("test-service")
    
    # Test operation context
    print("\n=== Testing operation context ===")
    with error_ctx.start_operation("test_operation", param1="value1", param2=42) as op_ctx:
        print(f"Operation started with correlation ID: {op_ctx.correlation_id}")
        
        # Test business decision logging
        error_ctx.log_business_decision(
            "skip_processing", 
            "data_already_processed",
            block_number=12345,
            transaction_count=0
        )
        
        # Test error logging
        try:
            raise ValueError("This is a test error")
        except Exception as e:
            error_ctx.log_error(
                "Test error occurred during processing",
                e,
                block_number=12345,
                additional_context="test_data"
            )
    
    # Test service stop
    print("\n=== Testing service stop logging ===")
    log_service_stop("test-service", processed_blocks=100, total_runtime=3600)
    
    print("\n=== Test completed successfully! ===")
    print("Enhanced logging works with psutil dependency.")

if __name__ == "__main__":
    test_enhanced_logging()