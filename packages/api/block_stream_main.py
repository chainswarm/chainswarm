from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from packages.api.routers import block_stream
from packages.indexers.base import setup_logger, setup_metrics
from packages.api.middleware.prometheus_middleware import PrometheusMiddleware, create_metrics_endpoint

app = FastAPI(
    title="Block Stream API",
    description="API for accessing blockchain block stream data",
    version="1.0.0",
    docs_url="/docs",
    openapi_url="/openapi.json"
)

# Setup logging and metrics
setup_logger("blockchain-insights-block-stream-api")
metrics_registry = setup_metrics("blockchain-insights-block-stream-api", start_server=False)

# Add Prometheus metrics middleware
app.add_middleware(PrometheusMiddleware, metrics_registry=metrics_registry, service_name="blockchain-insights-block-stream-api")

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Only include the block stream router
app.include_router(block_stream.router)

# Add metrics endpoint
metrics_endpoint = create_metrics_endpoint(metrics_registry)
app.get("/metrics")(metrics_endpoint)

@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "version": "1.0.0",
        "services": ["block-stream"],
        "metrics_enabled": metrics_registry is not None
    }