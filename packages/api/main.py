from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from packages.api.routers import money_flow, balance_tracking, known_addresses, similarity_search
from packages.indexers.base import setup_logger
from packages.api.middleware.rate_limiting import rate_limit_middleware

version = "0.1.0"
app = FastAPI(
    title="Chain Insights API",
    description="API for accessing chain insights",
    version=version,
    docs_url="/docs",
    openapi_url="/openapi.json"
)
setup_logger("chain-insights-api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "https://chain-insights-ui.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add rate limiting middleware
app.add_middleware(BaseHTTPMiddleware, dispatch=rate_limit_middleware)

app.include_router(money_flow.router)
app.include_router(balance_tracking.router)
app.include_router(known_addresses.router)
app.include_router(similarity_search.router)


@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "version": version
    }

@app.get(
    "/networks",
    summary="Get supported networks",
    description="Retrieves list of supported blockchain networks and their features",
    response_description="List of supported networks with status and features",
    tags=["networks"]
)
async def get_supported_networks():
    return {
        "networks": [
            {
                "name": "torus",
                "status": "active",
                "features": ["money-flow", "balance-tracking", "known-addresses",  "similarity-search"]
            }
        ]
    }