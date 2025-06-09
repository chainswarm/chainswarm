from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from packages.api.routers import block_stream
from packages.indexers.base import setup_logger

app = FastAPI(
    title="Block Stream API",
    description="API for accessing blockchain block stream data",
    version="1.0.0",
    docs_url="/docs",
    openapi_url="/openapi.json"
)
setup_logger("blockchain-insights-block-stream-api")

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Only include the block stream router
app.include_router(block_stream.router)

@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "version": "1.0.0",
        "services": ["block-stream"]
    }