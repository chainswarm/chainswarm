import time
from typing import Dict, Optional
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp


class PrometheusMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for collecting Prometheus metrics"""
    
    def __init__(self, app: ASGIApp, metrics_registry=None, service_name: str = "api"):
        super().__init__(app)
        self.metrics_registry = metrics_registry
        self.service_name = service_name
        
        if metrics_registry:
            self._init_metrics()
        else:
            logger.warning(f"No metrics registry provided for {service_name}")
    
    def _init_metrics(self):
        """Initialize HTTP metrics"""
        # Request metrics
        self.http_requests_total = self.metrics_registry.create_counter(
            'http_requests_total',
            'Total HTTP requests',
            ['method', 'endpoint', 'status', 'network']
        )
        
        self.http_request_duration = self.metrics_registry.create_histogram(
            'http_request_duration_seconds',
            'HTTP request duration',
            ['method', 'endpoint', 'network'],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, float('inf'))
        )
        
        self.http_requests_in_progress = self.metrics_registry.create_gauge(
            'http_requests_in_progress',
            'HTTP requests currently being processed',
            ['method', 'endpoint', 'network']
        )
        
        self.http_request_size_bytes = self.metrics_registry.create_histogram(
            'http_request_size_bytes',
            'HTTP request size in bytes',
            ['method', 'endpoint', 'network'],
            buckets=(64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, float('inf'))
        )
        
        self.http_response_size_bytes = self.metrics_registry.create_histogram(
            'http_response_size_bytes',
            'HTTP response size in bytes',
            ['method', 'endpoint', 'network'],
            buckets=(64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, float('inf'))
        )
        
        # Error metrics
        self.http_errors_total = self.metrics_registry.create_counter(
            'http_errors_total',
            'Total HTTP errors',
            ['method', 'endpoint', 'status', 'error_type']
        )
        
        self.http_client_errors_total = self.metrics_registry.create_counter(
            'http_client_errors_total',
            'Total HTTP client errors (4xx)',
            ['method', 'endpoint', 'status', 'error_type']
        )
        
        # Database query metrics (for API endpoints that query databases)
        self.api_database_queries_total = self.metrics_registry.create_counter(
            'api_database_queries_total',
            'Total API database queries',
            ['network', 'query_type', 'table']
        )
        
        self.api_database_query_duration = self.metrics_registry.create_histogram(
            'api_database_query_duration_seconds',
            'API database query duration',
            ['network', 'query_type'],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float('inf'))
        )
        
        self.api_database_query_errors_total = self.metrics_registry.create_counter(
            'api_database_query_errors_total',
            'Total API database query errors',
            ['network', 'query_type', 'error_type']
        )
        
        self.api_database_rows_returned_total = self.metrics_registry.create_counter(
            'api_database_rows_returned_total',
            'Total rows returned by API database queries',
            ['network', 'query_type']
        )
    
    async def dispatch(self, request: Request, call_next):
        """Process HTTP request and collect metrics"""
        if not self.metrics_registry:
            return await call_next(request)
        
        # Extract request information
        method = request.method
        path = request.url.path
        endpoint = self._normalize_endpoint(path)
        network = self._extract_network_from_request(request)
        
        # Skip metrics endpoint itself
        if path == "/metrics":
            return await call_next(request)
        
        # Record request size
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                request_size = int(content_length)
                self.http_request_size_bytes.labels(
                    method=method, endpoint=endpoint, network=network
                ).observe(request_size)
            except ValueError:
                pass
        
        # Track requests in progress
        self.http_requests_in_progress.labels(
            method=method, endpoint=endpoint, network=network
        ).inc()
        
        start_time = time.time()
        try:
            # Process request
            response = await call_next(request)
            
            # Record metrics
            duration = time.time() - start_time
            status_code = response.status_code
            
            # Record request metrics
            self.http_requests_total.labels(
                method=method, endpoint=endpoint, status=status_code, network=network
            ).inc()
            
            self.http_request_duration.labels(
                method=method, endpoint=endpoint, network=network
            ).observe(duration)
            
            # Record response size if available
            if hasattr(response, 'headers') and 'content-length' in response.headers:
                try:
                    response_size = int(response.headers['content-length'])
                    self.http_response_size_bytes.labels(
                        method=method, endpoint=endpoint, network=network
                    ).observe(response_size)
                except ValueError:
                    pass
            
            # Record error metrics
            if status_code >= 400:
                error_type = self._categorize_error(status_code)
                self.http_errors_total.labels(
                    method=method, endpoint=endpoint, status=status_code, error_type=error_type
                ).inc()
                
                if 400 <= status_code < 500:
                    self.http_client_errors_total.labels(
                        method=method, endpoint=endpoint, status=status_code, error_type=error_type
                    ).inc()
            
            return response
            
        except Exception as e:
            # Record exception metrics
            duration = time.time() - start_time
            self.http_errors_total.labels(
                method=method, endpoint=endpoint, status=500, error_type="internal_error"
            ).inc()
            
            logger.error(f"Error processing request {method} {path}: {e}")
            raise
            
        finally:
            # Decrement in-progress counter
            self.http_requests_in_progress.labels(
                method=method, endpoint=endpoint, network=network
            ).dec()
    
    def _normalize_endpoint(self, path: str) -> str:
        """Normalize endpoint path to reduce cardinality"""
        # Remove query parameters
        if '?' in path:
            path = path.split('?')[0]
        
        # Normalize common patterns
        path_parts = path.strip('/').split('/')
        normalized_parts = []
        
        for part in path_parts:
            # Replace UUIDs, addresses, and numeric IDs with placeholders
            if self._looks_like_uuid(part):
                normalized_parts.append('{uuid}')
            elif self._looks_like_address(part):
                normalized_parts.append('{address}')
            elif part.isdigit():
                normalized_parts.append('{id}')
            else:
                normalized_parts.append(part)
        
        normalized = '/' + '/'.join(normalized_parts) if normalized_parts else '/'
        return normalized
    
    def _looks_like_uuid(self, value: str) -> bool:
        """Check if value looks like a UUID"""
        return len(value) == 36 and value.count('-') == 4
    
    def _looks_like_address(self, value: str) -> bool:
        """Check if value looks like a blockchain address"""
        # Common patterns for blockchain addresses
        return (
            (len(value) >= 40 and value.startswith('0x')) or  # Ethereum-style
            (len(value) >= 40 and all(c in '0123456789abcdefABCDEF' for c in value)) or  # Hex
            (len(value) >= 25 and value.startswith('5'))  # Substrate-style
        )
    
    def _extract_network_from_request(self, request: Request) -> str:
        """Extract network information from request"""
        # Try to get network from query parameters
        network = request.query_params.get('network', 'unknown')
        
        # Try to get from headers
        if network == 'unknown':
            network = request.headers.get('x-network', 'unknown')
        
        # Try to infer from path
        if network == 'unknown':
            path_parts = request.url.path.strip('/').split('/')
            for part in path_parts:
                if part.lower() in ['torus', 'bittensor', 'polkadot']:
                    network = part.lower()
                    break
        
        return network
    
    def _categorize_error(self, status_code: int) -> str:
        """Categorize HTTP error by status code"""
        if status_code == 400:
            return "bad_request"
        elif status_code == 401:
            return "unauthorized"
        elif status_code == 403:
            return "forbidden"
        elif status_code == 404:
            return "not_found"
        elif status_code == 429:
            return "rate_limited"
        elif 400 <= status_code < 500:
            return "client_error"
        elif status_code == 500:
            return "internal_error"
        elif status_code == 502:
            return "bad_gateway"
        elif status_code == 503:
            return "service_unavailable"
        elif status_code == 504:
            return "gateway_timeout"
        elif 500 <= status_code < 600:
            return "server_error"
        else:
            return "unknown"
    
    def record_database_query(self, network: str, query_type: str, table: str, 
                            duration: float, rows_returned: int = 0, success: bool = True):
        """Record database query metrics (to be called by API endpoints)"""
        if not self.metrics_registry:
            return
        
        self.api_database_queries_total.labels(
            network=network, query_type=query_type, table=table
        ).inc()
        
        self.api_database_query_duration.labels(
            network=network, query_type=query_type
        ).observe(duration)
        
        if rows_returned > 0:
            self.api_database_rows_returned_total.labels(
                network=network, query_type=query_type
            ).inc(rows_returned)
        
        if not success:
            self.api_database_query_errors_total.labels(
                network=network, query_type=query_type, error_type="query_failed"
            ).inc()


def create_metrics_endpoint(metrics_registry):
    """Create a /metrics endpoint for Prometheus scraping"""
    async def metrics_endpoint():
        """Prometheus metrics endpoint"""
        if metrics_registry:
            metrics_text = metrics_registry.get_metrics_text()
            return Response(content=metrics_text, media_type=CONTENT_TYPE_LATEST)
        else:
            return JSONResponse(
                status_code=503,
                content={"error": "Metrics not available"}
            )
    
    return metrics_endpoint