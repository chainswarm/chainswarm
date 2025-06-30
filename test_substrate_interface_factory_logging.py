#!/usr/bin/env python3
"""
Test script to verify SubstrateInterfaceFactory simplified logging implementation.
"""

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath('.'))

from packages.indexers.base import setup_logger
from packages.indexers.substrate.node.substrate_interface_factory import SubstrateInterfaceFactory
from loguru import logger

def test_unsupported_network():
    """Test logging for unsupported network"""
    print("=== Testing Unsupported Network Logging ===")
    
    try:
        SubstrateInterfaceFactory.create_substrate_interface("unsupported_network", "ws://localhost:9944")
    except ValueError as e:
        print(f"✓ Expected ValueError caught: {e}")
    except Exception as e:
        print(f"✗ Unexpected error: {e}")

def test_connection_failure():
    """Test logging for connection failures"""
    print("\n=== Testing Connection Failure Logging ===")
    
    try:
        # Try to connect to a non-existent endpoint
        SubstrateInterfaceFactory.create_substrate_interface("torus", "ws://nonexistent:9944")
    except Exception as e:
        print(f"✓ Expected connection error caught: {e}")

def test_valid_network_detection():
    """Test that valid networks are detected correctly"""
    print("\n=== Testing Valid Network Detection ===")
    
    # Test that the method doesn't raise ValueError for supported networks
    # (even if connection fails, it should get past network validation)
    supported_networks = ["bittensor", "torus", "polkadot"]
    
    for network in supported_networks:
        try:
            SubstrateInterfaceFactory.create_substrate_interface(network, "ws://nonexistent:9944")
        except ValueError as e:
            if "Unsupported network" in str(e):
                print(f"✗ Network {network} incorrectly marked as unsupported")
            else:
                print(f"✓ Network {network} passed validation (other ValueError: {e})")
        except Exception as e:
            print(f"✓ Network {network} passed validation (connection error expected: {type(e).__name__})")

if __name__ == "__main__":
    # Setup logger once in main method with explicit service name
    setup_logger("substrate-interface-factory-test")
    
    print("Testing SubstrateInterfaceFactory Simplified Logging")
    print("=" * 60)
    
    test_unsupported_network()
    test_connection_failure()
    test_valid_network_detection()
    
    print("\n" + "=" * 60)
    print("Test completed! Check the logs above for proper error logging.")