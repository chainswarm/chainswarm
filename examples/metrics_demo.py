#!/usr/bin/env python3
"""
Demonstration script for Prometheus metrics collection in ChainSwarm.

This script shows how to:
1. Initialize metrics for different service types
2. Collect various types of metrics
3. Access metrics endpoints
4. View metrics in Prometheus format

Usage:
    python examples/metrics_demo.py
"""

import time
import asyncio
import requests
from packages.indexers.base import setup_metrics, IndexerMetrics
from packages.api.middleware.prometheus_middleware import PrometheusMiddleware
from loguru import logger

def demo_indexer_metrics():
    """Demonstrate indexer metrics collection"""
    print("\n=== Indexer Metrics Demo ===")
    
    # Setup metrics for a sample indexer
    service_name = "substrate-torus-balance-transfers"
    metrics_registry = setup_metrics(service_name, port=9101, start_server=True)
    
    # Create indexer metrics
    indexer_metrics = IndexerMetrics(metrics_registry, "torus", "balance_transfers")
    
    # Simulate some indexer activity
    print("Simulating indexer activity...")
    
    # Process some blocks
    for block_height in range(1000, 1010):
        start_time = time.time()
        
        # Simulate block processing
        time.sleep(0.1)  # Simulate processing time
        
        processing_time = time.time() - start_time
        indexer_metrics.record_block_processed(block_height, processing_time)
        
        # Simulate database operations
        indexer_metrics.record_database_operation("insert", "balance_transfers", 0.05, True)
        
        # Simulate event processing
        indexer_metrics.record_event_processed("Balances.Transfer", 5)
        
        print(f"Processed block {block_height}")
    
    # Update additional metrics
    indexer_metrics.update_blocks_behind(50)
    indexer_metrics.update_processing_rate(10.5)
    
    print(f"Metrics server running on port {metrics_registry.port}")
    print(f"View metrics at: http://localhost:{metrics_registry.port}/metrics")
    
    return metrics_registry

def demo_api_metrics():
    """Demonstrate API metrics collection"""
    print("\n=== API Metrics Demo ===")
    
    # Setup metrics for API
    service_name = "chain-swarm-api"
    metrics_registry = setup_metrics(service_name, port=9200, start_server=True)
    
    # Create some sample metrics
    http_requests = metrics_registry.create_counter(
        'demo_http_requests_total',
        'Demo HTTP requests',
        ['method', 'endpoint', 'status']
    )
    
    request_duration = metrics_registry.create_histogram(
        'demo_http_request_duration_seconds',
        'Demo HTTP request duration',
        ['method', 'endpoint']
    )
    
    # Simulate some API activity
    print("Simulating API activity...")
    
    endpoints = ['/balance_series', '/money_flow', '/similarity_search']
    methods = ['GET', 'POST']
    statuses = [200, 200, 200, 400, 500]  # Mostly successful
    
    for i in range(20):
        method = methods[i % len(methods)]
        endpoint = endpoints[i % len(endpoints)]
        status = statuses[i % len(statuses)]
        
        # Simulate request processing
        duration = 0.1 + (i % 5) * 0.05  # Variable duration
        
        http_requests.labels(method=method, endpoint=endpoint, status=status).inc()
        request_duration.labels(method=method, endpoint=endpoint).observe(duration)
        
        print(f"Processed {method} {endpoint} -> {status}")
    
    print(f"API metrics server running on port {metrics_registry.port}")
    print(f"View metrics at: http://localhost:{metrics_registry.port}/metrics")
    
    return metrics_registry

def demo_mcp_metrics():
    """Demonstrate MCP metrics collection"""
    print("\n=== MCP Metrics Demo ===")
    
    # Setup metrics for MCP server
    service_name = "chain-insights-mcp-server"
    metrics_registry = setup_metrics(service_name, port=9300, start_server=True)
    
    # Create MCP-specific metrics
    tool_calls = metrics_registry.create_counter(
        'demo_mcp_tool_calls_total',
        'Demo MCP tool calls',
        ['tool', 'network']
    )
    
    tool_duration = metrics_registry.create_histogram(
        'demo_mcp_tool_duration_seconds',
        'Demo MCP tool duration',
        ['tool', 'network']
    )
    
    # Simulate MCP tool usage
    print("Simulating MCP tool usage...")
    
    tools = ['balance_series_query', 'money_flow_query', 'similarity_search_query']
    network = 'torus'
    
    for i in range(15):
        tool = tools[i % len(tools)]
        
        # Simulate tool execution
        duration = 0.5 + (i % 3) * 0.2  # Variable duration
        
        tool_calls.labels(tool=tool, network=network).inc()
        tool_duration.labels(tool=tool, network=network).observe(duration)
        
        print(f"Executed tool: {tool}")
    
    print(f"MCP metrics server running on port {metrics_registry.port}")
    print(f"View metrics at: http://localhost:{metrics_registry.port}/metrics")
    
    return metrics_registry

def fetch_and_display_metrics(port):
    """Fetch and display metrics from a metrics endpoint"""
    try:
        response = requests.get(f"http://localhost:{port}/metrics", timeout=5)
        if response.status_code == 200:
            print(f"\n=== Sample Metrics from Port {port} ===")
            lines = response.text.split('\n')
            # Show first 20 lines of metrics
            for line in lines[:20]:
                if line and not line.startswith('#'):
                    print(line)
            print("... (truncated)")
        else:
            print(f"Failed to fetch metrics from port {port}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching metrics from port {port}: {e}")

def main():
    """Main demonstration function"""
    print("ChainSwarm Prometheus Metrics Demonstration")
    print("=" * 50)
    
    registries = []
    
    try:
        # Demo different types of metrics
        indexer_registry = demo_indexer_metrics()
        registries.append(indexer_registry)
        
        api_registry = demo_api_metrics()
        registries.append(api_registry)
        
        mcp_registry = demo_mcp_metrics()
        registries.append(mcp_registry)
        
        # Wait a moment for servers to start
        time.sleep(2)
        
        # Fetch and display sample metrics
        for registry in registries:
            if registry and registry.port:
                fetch_and_display_metrics(registry.port)
        
        print("\n=== Summary ===")
        print("Metrics servers are running on the following ports:")
        for registry in registries:
            if registry and registry.port:
                print(f"  - {registry.service_name}: http://localhost:{registry.port}/metrics")
        
        print("\nTo view metrics in Prometheus format, visit the URLs above.")
        print("Press Ctrl+C to stop the demo.")
        
        # Keep the demo running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping demo...")
            
    except Exception as e:
        logger.error(f"Demo error: {e}")
    finally:
        # Cleanup would happen automatically when the script exits
        print("Demo completed.")

if __name__ == "__main__":
    main()