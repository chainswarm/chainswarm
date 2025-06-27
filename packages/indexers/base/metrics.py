import os
import threading
import time
from typing import Dict, Optional, Any
from prometheus_client import (
    CollectorRegistry, Counter, Histogram, Gauge, Info, 
    start_http_server, generate_latest, CONTENT_TYPE_LATEST
)
from prometheus_client.core import REGISTRY
from loguru import logger
import socket

# Global metrics registry per service
_service_registries: Dict[str, CollectorRegistry] = {}
_metrics_servers: Dict[str, Any] = {}
_metrics_lock = threading.Lock()

class MetricsRegistry:
    """Centralized metrics registry for a service following logging conventions"""
    
    def __init__(self, service_name: str, port: Optional[int] = None):
        self.service_name = service_name
        self.registry = CollectorRegistry()
        self.port = port
        self.server = None
        self._common_labels = self._extract_labels_from_service_name(service_name)
        
        # Initialize common metrics
        self._init_common_metrics()
    
    def _extract_labels_from_service_name(self, service_name: str) -> Dict[str, str]:
        """Extract common labels from service name following logging conventions"""
        labels = {"service": service_name}
        
        # Parse service name patterns like 'substrate-torus-balance-transfers'
        parts = service_name.split('-')
        if len(parts) >= 3 and parts[0] == 'substrate':
            labels["network"] = parts[1]
            labels["indexer"] = '-'.join(parts[2:])
        elif 'api' in service_name:
            labels["component"] = "api"
        elif 'mcp' in service_name:
            labels["component"] = "mcp"
            
        return labels
    
    def _init_common_metrics(self):
        """Initialize common metrics available to all services"""
        # Service info
        self.service_info = Info(
            'service_info',
            'Service information',
            registry=self.registry
        )
        self.service_info.info({
            'service_name': self.service_name,
            'version': '1.0.0',
            **self._common_labels
        })
        
        # Service uptime
        self.service_start_time = Gauge(
            'service_start_time_seconds',
            'Service start time in Unix timestamp',
            registry=self.registry
        )
        self.service_start_time.set_to_current_time()
        
        # Common error counter
        self.errors_total = Counter(
            'service_errors_total',
            'Total number of errors by type',
            ['error_type', 'component'],
            registry=self.registry
        )
        
        # Health status
        self.health_status = Gauge(
            'service_health_status',
            'Service health status (1=healthy, 0=unhealthy)',
            registry=self.registry
        )
        self.health_status.set(1)  # Start as healthy
    
    def create_counter(self, name: str, description: str, labelnames: list = None) -> Counter:
        """Create a counter metric with common labels"""
        return Counter(
            name, description, 
            labelnames or [], 
            registry=self.registry
        )
    
    def create_histogram(self, name: str, description: str, labelnames: list = None, 
                        buckets: tuple = None) -> Histogram:
        """Create a histogram metric with common labels"""
        kwargs = {
            'name': name,
            'documentation': description,
            'labelnames': labelnames or [],
            'registry': self.registry
        }
        if buckets:
            kwargs['buckets'] = buckets
        return Histogram(**kwargs)
    
    def create_gauge(self, name: str, description: str, labelnames: list = None) -> Gauge:
        """Create a gauge metric with common labels"""
        return Gauge(
            name, description,
            labelnames or [],
            registry=self.registry
        )
    
    def start_metrics_server(self, port: Optional[int] = None) -> bool:
        """Start HTTP server for metrics endpoint"""
        if self.server is not None:
            logger.warning(f"Metrics server already running for {self.service_name}")
            return True
            
        target_port = port or self.port or self._get_default_port()
        
        try:
            # Check if port is available
            if not self._is_port_available(target_port):
                logger.warning(f"Port {target_port} not available, trying next available port")
                target_port = self._find_available_port(target_port)
            
            self.server = start_http_server(target_port, registry=self.registry)
            self.port = target_port
            logger.info(f"Metrics server started for {self.service_name} on port {target_port}")
            logger.info(f"Metrics available at: http://localhost:{target_port}/metrics")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start metrics server for {self.service_name}: {e}")
            return False
    
    def _get_default_port(self) -> int:
        """Get default port based on service name"""
        # Check service-specific environment variables first
        service_env_mapping = {
            'balance-transfers': 'BALANCE_TRANSFERS_METRICS_PORT',
            'balance-series': 'BALANCE_SERIES_METRICS_PORT',
            'money-flow': 'MONEY_FLOW_METRICS_PORT',
            'block-stream': 'BLOCK_STREAM_METRICS_PORT',
            'chain-swarm-api': 'CHAIN_SWARM_API_METRICS_PORT',
            'blockchain-insights-block-stream-api': 'BLOCKCHAIN_INSIGHTS_API_METRICS_PORT'
        }
        
        # Try service-specific environment variable
        for key, env_var in service_env_mapping.items():
            if key in self.service_name:
                env_port = os.getenv(env_var)
                if env_port:
                    try:
                        return int(env_port)
                    except ValueError:
                        logger.warning(f"Invalid {env_var} value: {env_port}, using default")
                        break
        
        # Check generic METRICS_PORT environment variable
        env_port = os.getenv('METRICS_PORT')
        if env_port:
            try:
                return int(env_port)
            except ValueError:
                logger.warning(f"Invalid METRICS_PORT value: {env_port}, using default")
        
        # Base port mapping following the plan
        port_mapping = {
            'balance-transfers': 9101,
            'balance-series': 9102,
            'money-flow': 9103,
            'block-stream': 9104,
            'chain-swarm-api': 9200,
            'blockchain-insights-block-stream-api': 9201
        }
        
        # Try to match service name to port
        for key, port in port_mapping.items():
            if key in self.service_name:
                return port
        
        # Default fallback
        return 9090
    
    def _is_port_available(self, port: int) -> bool:
        """Check if port is available"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                return True
        except OSError:
            return False
    
    def _find_available_port(self, start_port: int) -> int:
        """Find next available port starting from start_port"""
        for port in range(start_port, start_port + 100):
            if self._is_port_available(port):
                return port
        raise RuntimeError(f"No available ports found starting from {start_port}")
    
    def get_metrics_text(self) -> str:
        """Get metrics in Prometheus text format"""
        return generate_latest(self.registry).decode('utf-8')
    
    def record_error(self, error_type: str, component: str = "unknown"):
        """Record an error occurrence"""
        self.errors_total.labels(error_type=error_type, component=component).inc()
    
    def set_health_status(self, healthy: bool):
        """Set service health status"""
        self.health_status.set(1 if healthy else 0)


def setup_metrics(service_name: str, port: Optional[int] = None, 
                 start_server: bool = True) -> MetricsRegistry:
    """
    Setup metrics for a service following the same pattern as setup_logger.
    
    Args:
        service_name: Name of the service (e.g., 'substrate-torus-balance-transfers')
        port: Optional port for metrics server
        start_server: Whether to start HTTP server immediately
        
    Returns:
        MetricsRegistry: Configured metrics registry for the service
    """
    with _metrics_lock:
        if service_name in _service_registries:
            logger.debug(f"Metrics already setup for {service_name}")
            return _service_registries[service_name]
        
        # Create metrics registry
        metrics_registry = MetricsRegistry(service_name, port)
        _service_registries[service_name] = metrics_registry
        
        # Start metrics server if requested
        if start_server:
            success = metrics_registry.start_metrics_server()
            if success:
                _metrics_servers[service_name] = metrics_registry.server
        
        logger.info(f"Metrics setup completed for service: {service_name}")
        return metrics_registry


def get_metrics_registry(service_name: str) -> Optional[MetricsRegistry]:
    """Get existing metrics registry for a service"""
    return _service_registries.get(service_name)


def shutdown_metrics_servers():
    """Shutdown all metrics servers"""
    with _metrics_lock:
        for service_name, server in _metrics_servers.items():
            try:
                if hasattr(server, 'shutdown'):
                    server.shutdown()
                logger.info(f"Shutdown metrics server for {service_name}")
            except Exception as e:
                logger.error(f"Error shutting down metrics server for {service_name}: {e}")
        
        _metrics_servers.clear()


# Common metric buckets for different use cases
DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, float('inf'))
SIZE_BUCKETS = (64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, float('inf'))
COUNT_BUCKETS = (1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, float('inf'))


class IndexerMetrics:
    """Standard metrics for blockchain indexers"""
    
    def __init__(self, registry: MetricsRegistry, network: str, indexer_type: str):
        self.registry = registry
        self.network = network
        self.indexer_type = indexer_type
        
        # Block processing metrics
        self.blocks_processed_total = registry.create_counter(
            'indexer_blocks_processed_total',
            'Total number of blocks processed',
            ['network', 'indexer']
        )
        
        self.current_block_height = registry.create_gauge(
            'indexer_current_block_height',
            'Current block height being processed',
            ['network', 'indexer']
        )
        
        self.blocks_behind_latest = registry.create_gauge(
            'indexer_blocks_behind_latest',
            'Number of blocks behind the latest block',
            ['network', 'indexer']
        )
        
        self.block_processing_duration = registry.create_histogram(
            'indexer_block_processing_duration_seconds',
            'Time spent processing blocks',
            ['network', 'indexer'],
            buckets=DURATION_BUCKETS
        )
        
        self.processing_rate = registry.create_gauge(
            'indexer_processing_rate_blocks_per_second',
            'Block processing rate',
            ['network', 'indexer']
        )
        
        # Database metrics
        self.database_operations_total = registry.create_counter(
            'indexer_database_operations_total',
            'Total database operations',
            ['network', 'indexer', 'operation', 'table']
        )
        
        self.database_operation_duration = registry.create_histogram(
            'indexer_database_operation_duration_seconds',
            'Database operation duration',
            ['network', 'indexer', 'operation'],
            buckets=DURATION_BUCKETS
        )
        
        self.database_errors_total = registry.create_counter(
            'indexer_database_errors_total',
            'Total database errors',
            ['network', 'indexer', 'error_type']
        )
        
        # Event processing metrics
        self.events_processed_total = registry.create_counter(
            'indexer_events_processed_total',
            'Total events processed',
            ['network', 'indexer', 'event_type']
        )
        
        self.failed_events_total = registry.create_counter(
            'indexer_failed_events_total',
            'Total failed events',
            ['network', 'indexer', 'error_type']
        )
    
    def record_block_processed(self, block_height: int, processing_time: float):
        """Record a processed block"""
        labels = {'network': self.network, 'indexer': self.indexer_type}
        self.blocks_processed_total.labels(**labels).inc()
        self.current_block_height.labels(**labels).set(block_height)
        self.block_processing_duration.labels(**labels).observe(processing_time)
    
    def record_database_operation(self, operation: str, table: str, duration: float, success: bool = True):
        """Record a database operation"""
        labels = {'network': self.network, 'indexer': self.indexer_type, 'operation': operation, 'table': table}
        self.database_operations_total.labels(**labels).inc()
        
        duration_labels = {'network': self.network, 'indexer': self.indexer_type, 'operation': operation}
        self.database_operation_duration.labels(**duration_labels).observe(duration)
        
        if not success:
            error_labels = {'network': self.network, 'indexer': self.indexer_type, 'error_type': 'operation_failed'}
            self.database_errors_total.labels(**error_labels).inc()
    
    def record_event_processed(self, event_type: str, count: int = 1):
        """Record processed events"""
        labels = {'network': self.network, 'indexer': self.indexer_type, 'event_type': event_type}
        self.events_processed_total.labels(**labels).inc(count)
    
    def record_failed_event(self, error_type: str, count: int = 1):
        """Record failed events"""
        labels = {'network': self.network, 'indexer': self.indexer_type, 'error_type': error_type}
        self.failed_events_total.labels(**labels).inc(count)
    
    def update_blocks_behind(self, blocks_behind: int):
        """Update blocks behind metric"""
        labels = {'network': self.network, 'indexer': self.indexer_type}
        self.blocks_behind_latest.labels(**labels).set(blocks_behind)
    
    def update_processing_rate(self, rate: float):
        """Update processing rate metric"""
        labels = {'network': self.network, 'indexer': self.indexer_type}
        self.processing_rate.labels(**labels).set(rate)