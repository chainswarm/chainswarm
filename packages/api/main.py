from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST
from packages.api.routers import money_flow, balance_series, known_addresses, similarity_search, balance_transfers
from packages.indexers.base import setup_enhanced_logger, setup_metrics, ErrorContextManager, classify_error, log_service_start
from loguru import logger
from packages.api.middleware.rate_limiting import rate_limit_middleware
from packages.api.middleware.prometheus_middleware import PrometheusMiddleware, create_metrics_endpoint
from packages.api.middleware.correlation_middleware import CorrelationMiddleware

version = "0.2.0"
app = FastAPI(
    title="Chain Swarm API",
    description="API for accessing chain swarm",
    version=version,
    docs_url="/docs",
    openapi_url="/openapi.json"
)


# Setup enhanced logging and metrics
import os
network = os.getenv("NETWORK", "torus").lower()
service_name = f"{network}-api"
setup_enhanced_logger(service_name)
metrics_registry = setup_metrics(service_name, start_server=False)  # Use environment variable for port
error_ctx = ErrorContextManager(service_name)

# Log service startup
log_service_start(
    service_name,
    version=version,
    network=network,
    cors_origins=["http://localhost:3000", "http://localhost:3001", "https://chain-insights-ui.vercel.app"],
    metrics_enabled=metrics_registry is not None
)

# Add correlation middleware FIRST (before other middleware)
app.add_middleware(CorrelationMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "https://chain-insights-ui.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Prometheus metrics middleware
app.add_middleware(PrometheusMiddleware, metrics_registry=metrics_registry, service_name=service_name)

# Add rate limiting middleware
app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limit_middleware)

app.include_router(money_flow.router)
app.include_router(similarity_search.router)
app.include_router(balance_series.router)
app.include_router(balance_transfers.router)
app.include_router(known_addresses.router)

# Add metrics endpoint that aggregates all service metrics
def create_aggregated_metrics_endpoint():
    """Create metrics endpoint that aggregates metrics from all services"""
    from packages.indexers.base.metrics import _service_registries
    
    async def aggregated_metrics_endpoint():
        """Prometheus metrics endpoint with aggregated metrics from all services"""
        try:
            all_metrics = []
            
            # Add main API metrics
            if metrics_registry:
                api_metrics = metrics_registry.get_metrics_text()
                all_metrics.append(api_metrics)
            
            # Add metrics from all indexer services
            api_service_name = f"{network}-api"
            for svc_name, service_registry in _service_registries.items():
                if svc_name != api_service_name:  # Avoid duplicating API metrics
                    service_metrics = service_registry.get_metrics_text()
                    all_metrics.append(service_metrics)
            
            # Combine all metrics
            combined_metrics = "\n".join(all_metrics)
            return Response(content=combined_metrics, media_type=CONTENT_TYPE_LATEST)
            
        except Exception as e:
            # ENHANCED: Error logging with context
            error_ctx.log_error(
                "Metrics aggregation failed",
                error=e,
                operation="metrics_aggregation",
                service_registries_count=len(_service_registries) if '_service_registries' in globals() else 0,
                error_category=classify_error(e)
            )
            return JSONResponse(
                status_code=503,
                content={"error": "Metrics aggregation failed"}
            )
    
    return aggregated_metrics_endpoint

aggregated_metrics_endpoint = create_aggregated_metrics_endpoint()
app.get("/metrics")(aggregated_metrics_endpoint)

@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "version": version
    }