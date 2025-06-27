from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST
from packages.api.routers import money_flow, balance_series, known_addresses, similarity_search, balance_transfers
from packages.indexers.base import setup_logger, setup_metrics
from loguru import logger
from packages.api.routers import money_flow, balance_series, known_addresses, similarity_search, balance_transfers
from packages.indexers.base import setup_logger
from packages.api.middleware.rate_limiting import rate_limit_middleware
from packages.api.middleware.prometheus_middleware import PrometheusMiddleware, create_metrics_endpoint

version = "0.2.0"
app = FastAPI(
    title="Chain Swarm API",
    description="API for accessing chain swarm",
    version=version,
    docs_url="/docs",
    openapi_url="/openapi.json"
)


# Setup logging and metrics
setup_logger("chain-swarm-api")
metrics_registry = setup_metrics("chain-swarm-api", port=8990, start_server=False)  # Metrics on port 8990


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "https://chain-insights-ui.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Prometheus metrics middleware
app.add_middleware(PrometheusMiddleware, metrics_registry=metrics_registry, service_name="chain-swarm-api")

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
            for service_name, service_registry in _service_registries.items():
                if service_name != "chain-swarm-api":  # Avoid duplicating API metrics
                    service_metrics = service_registry.get_metrics_text()
                    all_metrics.append(service_metrics)
            
            # Combine all metrics
            combined_metrics = "\n".join(all_metrics)
            return Response(content=combined_metrics, media_type=CONTENT_TYPE_LATEST)
            
        except Exception as e:
            logger.error(f"Error generating aggregated metrics: {e}")
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